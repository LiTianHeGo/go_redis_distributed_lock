package go_redis_distributed_lock

//基于setnx的解锁
const LuaCheckAndDeleteLock = `
  local lockKey = KEYS[1]
  local newHolder = ARGV[1]
  local oldHolder = redis.call('get',lockKey)
  if (not oldHolder or oldHolder ~= newHolder) then
    return 0
	else
		return redis.call('del',lockKey)
  end
`

//基于setnx的看门狗
const LuaCheckAndResetExpire = `
  local lockKey = KEYS[1]
  local newHolder = ARGV[1]
  local duration = ARGV[2]
  local oldHolder = redis.call('get',lockKey)
  if (not oldHolder or oldHolder ~= newHolder) then
    return 0
	else
		return redis.call('expire',lockKey,duration)
  end
`

// 基于hset的可重入加锁（1、若锁不存在则可以直接加锁；2、若锁已存在且锁内的键对应的持有者身份与传入的身份相符则可以重入加锁；否则均不能加锁）
const CheckAndReentry = `
if redis.call('exists',KEYS[1]) == 0 or redis.call('hexists',KEYS[1],ARGV[1]) == 1 then
   redis.call('hincrby',KEYS[1],ARGV[1],1)
   redis.call('expire',KEYS[1],ARGV[2])
   return 1
else
   return 0
end
`

// 基于hset的解锁（若不存在符合条件的锁（锁不存在，或者锁存在但锁中的键对应的持有者身份与传入的身份不符），也即未获取到锁的场景，则不能进行操作）
const CheckAndDeleteReentryLock = `
if redis.call('hexists',KEYS[1],ARGV[1]) == 0 then
    return nil
elseif redis.call('hincrby',KEYS[1],ARGV[1],-1) == 0 then
    return redis.call('del',KEYS[1])
else
    return 0
end
`

//基于hset的看门狗（若不存在符合条件的锁（锁不存在，或者锁存在但锁中的键对应的持有者身份与传入的身份不符），也即未获取到锁的场景，不能进行操作）
const CheckAndResetExpire = `
  if redis.call('hexists',KEYS[1],ARGV[1]) == 0 then
    return 0
	else
		return redis.call('expire',KEYS[1],ARGV[2])
  end
`
