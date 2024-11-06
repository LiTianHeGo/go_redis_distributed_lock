package go_redis_distributed_lock

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis"
)

// 红锁中每个节点默认的加锁超时时间 50 ms
const DefaultSingleLockTimeout = 50

type RedLock struct {
	locks []*RedisDistributedLock //在所有节点上申请的锁集合

	singleNodeTimeout uint64 //单个节点的请求加锁超时时间，单位毫秒
	expireTime        uint64 //锁的过期时间，单位毫秒
}

// 【红锁】新建
func NewRedLock(key string, clients []*redis.Client, opts ...RedLockOption) (*RedLock, error) {
	if len(clients) < 3 {
		return nil, errors.New("the number of nodes must be greater than 3")
	}

	rl := RedLock{}
	for _, opt := range opts {
		opt(&rl)
	}

	RedlockDefaultSetting(&rl)

	//所有节点累计花费的用于请求加锁时间不能大于分布式锁过期时间的十分之一,确保在成功取得锁之后有足够的时间用于执行业务
	if rl.expireTime > 0 && rl.singleNodeTimeout*10 > rl.expireTime {
		return nil, errors.New("expire thresholds of single node is too long")
	}

	locks := make([]*RedisDistributedLock, len(clients))
	for index, client := range clients {
		go func(i int, c *redis.Client) {
			locks[i] = NewDistributedLock(key, c, WithExpireTime(rl.expireTime))
		}(index, client)
	}

	return &rl, nil
}

// 【红锁】加锁,在单个节点的限制请求时间内对每个节点发起加锁请求
func (r *RedLock) Lock(ctx context.Context) error {
	var successCount atomic.Int32
	var wg sync.WaitGroup

	for _, lock := range r.locks {
		wg.Add(1)
		go func(l *RedisDistributedLock) { //todo:可以尝试使用cancelCtx和time.AfterFunc，对超过50ms的加锁请求直接进行取消
			defer wg.Done()
			startTime := time.Now()
			err := l.Lock(ctx)
			consume := time.Since(startTime)
			if err == nil && consume <= time.Duration(r.singleNodeTimeout) {
				successCount.Add(1)
			}
		}(lock)
	}

	wg.Wait()

	if int(successCount.Load()) < len(r.locks)>>1+1 {
		return errors.New("成功数量未达标，加锁失败")
	}
	return nil
}

// 【红锁】解锁
func (r *RedLock) Unlock() error {
	var err error
	for _, lock := range r.locks {
		if _err := lock.Unlock(); _err != nil {
			if err == nil {
				err = _err
			}
		}
	}
	return err
}

/*****************************以下，RedLockOptions相关***********************/
type RedLockOption func(*RedLock)

// 设置单个节点的请求加锁超时时间
func WithSingleNodeTimeout(singleNodeTimeout uint64) RedLockOption {
	return func(rl *RedLock) {
		rl.singleNodeTimeout = singleNodeTimeout
	}
}

// 设置锁的过期时间
func WithRedLockExpireTime(expireTime uint64) RedLockOption {
	return func(rl *RedLock) {
		rl.expireTime = expireTime
	}
}

// 配置红锁的默认设置
func RedlockDefaultSetting(rl *RedLock) {
	if rl.singleNodeTimeout <= 0 {
		rl.singleNodeTimeout = DefaultSingleLockTimeout
	}
}
