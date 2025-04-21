import redis

lua_add_dtype = """
-- Get key and value
local key = KEYS[1]
local value = ARGV[1]
local hash = "DTYPES"

-- Check if the key exists
if redis.call("HEXISTS", hash, key) == 0 then
    -- If not, set the value
    redis.call("HSET", hash, key, value)
    return "Key created"
else
    -- If it exists, return current value
    return redis.call("HGET", hash, key)
end


"""

r = redis.StrictRedis(host='localhost', port=6379)
print(r.ping())

check_and_set = r.register_script(lua_add_dtype)
print(check_and_set(keys=["test_key"], args=["test_value"]))

