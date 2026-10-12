-- 令牌桶：一次调用原子地处理这次请求涉及的全部桶。时间只取 Redis 的 TIME，多个实例的时钟不一致也没关系。
-- KEYS[i] 是第 i 个桶；ARGV[1] 是 1（准入：全部够才全部扣）或 0（修正：不检查，直接扣）；
-- ARGV[2i]、ARGV[2i+1] 是第 i 个桶的每分钟限额 L 和这次的花费（修正里负数是加回）。
-- 桶是一个 hash：tokens 是上次写入时的令牌数（可以是负数），ms 是那时的毫秒时间。容量 L，每毫秒补 L/60000。
-- 不存在的桶当作满的；每次写都设过期时间：补满所需的时间再加 1 秒，到期删掉等于满桶。
-- 只返回整数（Lua 的小数返回给客户端会被截断）：放行是 {0, 0}；拒绝是 {不够的桶里等得最久的那个的序号, Retry-After 秒数}。
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local check = ARGV[1] == '1'
local tokens = {}
local worst, wait = 0, 0
for i, key in ipairs(KEYS) do
  local limit, cost = tonumber(ARGV[2 * i]), tonumber(ARGV[2 * i + 1])
  local b = redis.call('HMGET', key, 'tokens', 'ms')
  local v = limit
  if b[1] then
    v = math.min(limit, tonumber(b[1]) + math.max(0, now - tonumber(b[2])) * limit / 60000)
  end
  tokens[i] = v
  -- 花费超过整桶的请求只要求桶满：否则它永远进不来。
  local need = math.min(cost, limit) - v
  if check and need > 0 and need * 60000 / limit > wait then
    worst, wait = i, need * 60000 / limit
  end
end
if worst > 0 then
  return {worst, math.max(1, math.ceil(wait / 1000))}
end
for i, key in ipairs(KEYS) do
  local limit, cost = tonumber(ARGV[2 * i]), tonumber(ARGV[2 * i + 1])
  local v = math.min(limit, tokens[i] - cost)
  redis.call('HSET', key, 'tokens', v, 'ms', now)
  redis.call('PEXPIRE', key, math.ceil((limit - v) * 60000 / limit) + 1000)
end
return {0, 0}
