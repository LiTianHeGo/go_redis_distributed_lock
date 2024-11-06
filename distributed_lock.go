package go_redis_distributed_lock

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"go_redis_distributed_lock/utils"

	"github.com/go-redis/redis"
)

/*
 * Redis分布式锁
 * 1、支持阻塞自旋取锁和非阻塞取锁
 * 2、支持看门狗模式实现锁过期时间的重置
 * 3、支持对称性解锁（解锁者需要与当前持锁者身份一致）
 * 4、结合lua脚本实现加锁、解锁、看门狗续期的原子操作
 * 5、支持红锁 redLock 解决 redis 集群环境的数据弱一致性问题
 * 6、支持goroutine级别的锁可重入（基于hset实现）
 */
const LockKeyPrefix = "REDIS_DISTRIBUTED_LOCK_"

var ErrLockAcquiredByOthers = errors.New("lock is acquired by others")

type RedisDistributedLock struct {
	client *redis.Client
	key    string //锁的key

	//当前持锁者（考虑使用：机器ip+进程id+协程id）
	//todo：考虑新建锁时应该显式指定锁的holder身份信息，用于更好地支持一个持锁的父goroutine在开启一个子goroutine时，子goroutine需要对同一个分布式锁发起重入的需求（例如调用了）
	//考虑使用context存储holder身份信息（参考在context中传递用户权限信息），用于在开启更多go协程时向其中传递。
	//如果直接向子goroutine传递的是已有的锁对象，则可以利用锁对象的isWatching的计数值实现可重入；
	//如果向子goroutine传递的是context（含holder身份信息），并希望在子goroutine中新建一个锁对象来进行分布式锁的重入，
	//则应该在新建锁对象的时候显式赋值holder字段为context中包含的那份身份信息

	//目前【仅支持在同一个goroutine中】进行锁的多次重入（如调用一个需要获取同一个分布式锁的方法、新建一个holder相同的锁对象对同一个分布式锁发起重入）；
	//对于【新建一个子goroutine】并在其代码逻辑中也尝试对同一个分布式锁发起重入（即跨协程重入），作两种考虑：
	//1、向子goroutine传入父goroutine正在使用的锁对象，供子goroutine直接重入；或向子goroutine传入父goroutine正在使用的锁对象的holder身份信息，
	//	 供子goroutine新建锁对象（如果holder采用），并使用新的锁对象进行重入。
	//2、不向子goroutine传递锁相关的内容，子goroutine直接阻塞等待当前父goroutine执行完成并释放锁，然后参与分布式锁竞争。
	holder     string
	expireTime uint64 //锁过期时间,单位毫秒

	blockMode            bool   //取锁操作是否为阻塞等待模式
	maxBlockMilliseconds uint64 //最大阻塞等待时长，单位毫秒
	watchDogMode         bool   //是否开启看门狗模式（每个锁对象最多只会开启一个看门狗守护协程）

	//isWatching有两层含义:(1)看门狗是否工作中;(2)当前锁对象对redis分布式锁的重入计数值;
	//0-未启动看门狗；1-已启动(也意味着当前锁对象对分布式锁只重入了1次)；大于1-说明当前锁对象对分布式锁重入了多次。
	//isWatching主要用于启动和关闭看门狗时的辅助判断
	//只有在isWatching的值从0到1的上升沿（也即首次加锁成功）才会开启看门狗，只有在isWatching的值从1到0的下降沿才会关闭当前锁对象的看门狗
	isWatching   atomic.Int32
	stopWatching context.CancelFunc //利用cancelCtx取消看门狗守护协程
}

func NewDistributedLock(key string, client *redis.Client, opts ...LockOption) *RedisDistributedLock {
	lock := RedisDistributedLock{
		key:    key,
		holder: utils.GetPidAndGidStr(),
		client: client,
	}

	for _, opt := range opts {
		opt(&lock)
	}

	DefaultSetting(&lock)
	return &lock
}

// 加锁
func (lock *RedisDistributedLock) Lock(ctx context.Context) (err error) {
	defer func() {
		if err == nil && lock.watchDogMode { //加锁成功，且启用了看门狗模式
			lock.startWatchDog(ctx)
		}
	}()

	if err = lock.acquireLock(); err == nil {
		return
	}

	if !lock.blockMode { // 非阻塞等待模式下加锁失败，直接返回
		return
	}

	if !errors.Is(err, ErrLockAcquiredByOthers) { // 阻塞等待模式下，若发生的是【ErrLockAcquiredByOthers锁已被他人持有】之外的其他错误，直接返回
		return
	}

	// 开始阻塞取锁
	err = lock.blocking(ctx)
	return
}

// 执行取锁动作
func (lock *RedisDistributedLock) acquireLock() error {
	if lock.key == "" || lock.holder == "" {
		return errors.New("key or holder can't be empty")
	}

	/*
		//使用Setnx实现分布式锁，不支持可重入性
		ok, err := Rclient.SetNX(lock.getLockKey(), lock.holder, time.Duration(lock.expireTime)*time.Millisecond).Result()
		if ok { //加锁成功
			return nil
		}
		if !ok && err == nil { //加锁失败且未报错，即锁已被他人持有
			return fmt.Errorf("err: %w", ErrLockAcquiredByOthers)
		}
	*/

	//使用Hset来实现分布式锁，支持可重入
	keys := []string{lock.getLockKey()}
	args := []interface{}{lock.holder, lock.expireTime / 1000}
	result, err := Rclient.Eval(CheckAndReentry, keys, args...).Result()
	if err != nil {
		return err
	}

	if ret, _ := result.(int64); ret != 1 {
		return ErrLockAcquiredByOthers
	}

	return err
}

// 拼接锁的key
func (lock *RedisDistributedLock) getLockKey() string {
	return LockKeyPrefix + lock.key
}

// 执行阻塞取锁
func (lock *RedisDistributedLock) blocking(ctx context.Context) error {
	//最大阻塞等待时间,单位ms
	timeout := time.After(time.Duration(lock.maxBlockMilliseconds) * time.Millisecond)
	//自旋取锁，间隔50ms
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		select {

		case <-timeout:
			return fmt.Errorf("lock failed, bolck timeout, err: %w", ErrLockAcquiredByOthers)

		case <-ctx.Done():
			return fmt.Errorf("lock failed, ctx cancel, err: %w", ctx.Err())

		default:
			err := lock.acquireLock()
			if err == nil { //取锁成功
				return nil
			}
			if !errors.Is(err, ErrLockAcquiredByOthers) { //发生的是【ErrLockAcquiredByOthers锁已被他人持有】之外的其他错误
				return err
			}
		}
	}

	return nil
}

// 尝试启动看门狗
func (lock *RedisDistributedLock) startWatchDog(ctx context.Context) {
	//只有在isWatching从0到1的上升沿才会开启看门狗守护协程（即只在首次加锁时执行）,否则仅对【isWatching表示的重入计数值】进行加1操作
	// if atomic.AddInt32(&lock.isWatching, 1) == 1 {
	if lock.isWatching.Add(1) == 1 {
		ctx, lock.stopWatching = context.WithCancel(ctx)
		go func() {
			lock.startWatching(ctx)
		}()
	}
}

// 真正启动看门狗监视
func (lock *RedisDistributedLock) startWatching(ctx context.Context) {
	work_time_step := (lock.expireTime / 1000) * 3 / 4 //看门狗工作时间步长，取<锁过期时间的四分之三>
	ticker := time.NewTicker(time.Duration(work_time_step) * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		select {
		case <-ctx.Done():
			return //cancelCtx手动解锁
		default:
			lock.resetLockExpireTime() //重置锁的过期时间
		}
	}
}

// 重置锁的过期时间
func (lock *RedisDistributedLock) resetLockExpireTime() error {
	keys := []string{lock.getLockKey()}
	args := []interface{}{lock.holder, lock.expireTime / 1000}
	/*
		//基于setnx
		result, err := Rclient.Eval(LuaCheckAndResetExpire, keys, args...).Result()
		if ret, _ := result.(int64); ret != 1 {
			return errors.New("cannot operate other's lock")
		}
	*/

	//基于hset
	result, err := Rclient.Eval(CheckAndResetExpire, keys, args...).Result()
	if err != nil {
		return err
	}

	fmt.Println("重置锁过期时间：", result, err)
	if ret, _ := result.(int64); ret != 1 {
		return errors.New("lock dose not exist")
	}

	return nil
}

// 解锁
func (lock *RedisDistributedLock) Unlock() error {
	/*
		//删除分布式锁（基于setnx）
		keys := []string{lock.getLockKey()}
		args := []interface{}{lock.holder}
		result, err := Rclient.Eval(LuaCheckAndDeleteLock, keys, args...).Result()
		if ret, _ := result.(int64); ret != 1 {
			return errors.New("cannot delete other's lock")
		}
	*/

	//删除分布式锁（基于hset）
	keys := []string{lock.getLockKey()}
	args := []interface{}{lock.holder}
	result, err := Rclient.Eval(CheckAndDeleteReentryLock, keys, args...).Result()
	if err != nil {
		return err
	}

	if result == nil {
		return errors.New("unlock failed , lock dose not exist")
	}

	/*
		结合锁对象的看门狗启动规则，作相应的看门狗协程回收动作 ：
		1、如果使用同一个锁对象对分布式锁进行多次重入，只会在首次加锁时启动一个看门狗，后续对于该分布式锁的重入不再开启额外的看门狗，但会对lock.isWatching的重入计数值进行+1；
		2、如果使用不同的锁对象（但它们的holder值相同）对分布式锁进行重入，每个锁对象都会启动一个看门狗
	*/

	/*
		//ret, _ := result.(int64)
		//ret==1,表示redis中的分布式锁已经完全释放，也即当前锁对象的unlock释放的是整个分布式锁的最后一次引用；
		//ret==0,表示当前锁对象释放了一次对分布式锁的重入，此时要么是当前锁对象对分布式锁还存在重入引用（即同一个锁对象操作了多次Lock加锁），要么是还有其他holder值相同的锁对象仍在引用分布式锁
	*/

	//锁对象lock的解锁操作无需关注ret的值是0还是1，因为lock只需管理好自身看门狗守护协程的资源释放
	if lock.isWatching.Add(-1) == 0 && lock.stopWatching != nil { //只在锁对象的isWatching从1到0的下降沿才会释放的看门狗（即当前锁对象已经结束了对分布式锁的所有重入操作）
		lock.stopWatching()
	}

	return nil
}

/*******************************以下，options相关****************************/

type LockOption func(*RedisDistributedLock)

// 取锁操作是否阻塞等待
func WithBlock() LockOption {
	return func(lock *RedisDistributedLock) {
		lock.blockMode = true
	}
}

// 取锁操作的最大阻塞等待时间，单位毫秒
func WithMaxBlockMilliseconds(maxBlockSeconds uint64) LockOption {
	return func(lock *RedisDistributedLock) {
		lock.maxBlockMilliseconds = maxBlockSeconds
	}
}

// 锁的过期时间,单位毫秒
func WithExpireTime(expireTime uint64) LockOption {
	return func(lock *RedisDistributedLock) {
		lock.expireTime = expireTime
	}
}

// 是否开启看门狗模式（默认未开启）
func WithWatchDogMode(watchDogMode bool) LockOption {
	return func(lock *RedisDistributedLock) {
		lock.watchDogMode = watchDogMode
	}
}

// 配置锁的默认设置
func DefaultSetting(lock *RedisDistributedLock) {
	if lock.blockMode && lock.maxBlockMilliseconds == 0 {
		lock.maxBlockMilliseconds = 5 //取锁操作的最大阻塞等待时间，默认5秒
	}
	if lock.expireTime == 0 {
		lock.expireTime = 5 //锁的过期时间，默认5秒
	}
}
