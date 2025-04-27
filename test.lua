local lmdb = require("lmdb")

-- 创建环境
local env = assert(lmdb.open("./var"))
print(env)

-- 开始事务
local txn = assert(env:txn_begin())
print(txn)

-- 打开数据库
local dbi = assert(txn:dbi_open())
print(dbi)

-- 写入数据
local key = "name"
local value = "Lua User"
assert(dbi:put(key, value .. value))
assert(dbi:put(key, value))
assert(value == dbi:get(key))
assert(dbi:del(key))
assert(nil == dbi:get(key))

for i=1, 10 do
  assert(dbi:put("key" .. i, "value" .. i))
end

dbi:close()
print('txn id', txn:id())
-- 提交事务
txn:commit()

txn = assert(env:txn_begin())
dbi = assert(txn:dbi_open())
print(dbi)
-- 读取数据
local cursor = assert(dbi:cursor_open())
repeat
  local k, v = cursor:get()
  print(k, v)
until k == nil

print(cursor:count())
print(cursor:count(), 10)

cursor = assert(dbi:cursor_open())
for k, v in cursor:pairs() do
    print(k, v)
end

txn:abort()

-- 关闭环境
-- env:close()
print('Done')
