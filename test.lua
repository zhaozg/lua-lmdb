local lmdb = require("lmdb")

-- 创建环境
local env = assert(lmdb.open("var/db"))

-- 开始事务
local txn = env:txn_begin(false)

-- 打开数据库
local dbi = txn:dbi_open(nil, 0)

-- 写入数据
txn:put(dbi, "name", "Lua User")

-- 提交事务
txn:commit()

-- 读取数据
local txn_ro = env:txn_begin(true)
local value = txn_ro:get(dbi, "name")
print("Name:", value)  -- 输出: Name: Lua User
txn_ro:abort()

-- 关闭环境
env:close()
