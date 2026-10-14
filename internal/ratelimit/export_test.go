package ratelimit

// OpenOptions 让测试把自己的拨号器换进 redis.Options，再按限流的要求建连接池。
var OpenOptions = newLimiter
