/***
lmdb binding for Lua, provide lmdb function in lua.

LMDB is a fast memory-mapped key-value database.
It provides a simple and efficient way to store and retrieve data in Lua.

## handle error

In lmdb, errors are represented by non-zero numbered constants.
If an internal error is encountered, the failing luv function will
return to the caller an assertable `nil, errstr, code` tuple:

- `nil` idiomatically indicates failure
- `estr` is the error string
- `code` is int error code

This tuple is referred to below as the `fail` pseudo-type.

When a function is called successfully, it will return either a value that is
relevant to the operation of the function, or `object` to indicate
success, or sometimes nothing at all.

@usage
  local lmdb = require('lmdb')

*/
#include <lauxlib.h>
#include <lua.h>
#include <lualib.h>

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/signal.h>

#include "liblmdb/lmdb.h"

// 定义元表名称
#define LUA_LMDB_ENV    "LMDB.Env"
#define LUA_LMDB_TXN    "LMDB.Txn"
#define LUA_LMDB_DBI    "LMDB.Dbi"
#define LUA_LMDB_CURSOR "LMDB.Cursor"

// 环境对象
typedef struct
{
  MDB_env *env;
} lmdb_env;

// 事务对象
typedef struct
{
  MDB_txn *txn;
  int      env_ref;
} lmdb_txn;

// 数据库句柄
typedef struct
{
  MDB_dbi  dbi;
  MDB_txn *txn;
  int      txn_ref;
} lmdb_dbi;

// 游标对象
typedef struct
{
  MDB_cursor *cursor;
  int         dbi_ref;
} lmdb_cursor;

static int
lmdb_pushstat(lua_State *L, MDB_stat *stat)
{
  lua_newtable(L);
  lua_pushinteger(L, stat->ms_psize);
  lua_setfield(L, -2, "psize");
  lua_pushinteger(L, stat->ms_depth);
  lua_setfield(L, -2, "depth");
  lua_pushinteger(L, stat->ms_branch_pages);
  lua_setfield(L, -2, "branch_pages");
  lua_pushinteger(L, stat->ms_leaf_pages);
  lua_setfield(L, -2, "leaf_pages");
  lua_pushinteger(L, stat->ms_overflow_pages);
  lua_setfield(L, -2, "overflow_pages");
  lua_pushinteger(L, stat->ms_entries);
  lua_setfield(L, -2, "entries");
  return 1;
}

static int
lmdb_pusherror(lua_State *L, int err)
{
  const char *msg = mdb_strerror(err);
  lua_pushnil(L);
  lua_pushstring(L, msg);
  lua_pushinteger(L, err);
  return 3;
}

static MDB_val
lmdb_checkvalue(lua_State *L, int idx)
{
  MDB_val val;
  val.mv_data = (void*)luaL_checklstring(L, idx, &val.mv_size);
  return val;
}

/***
@section lmdb
*/
/***
Get version of lmdb
@function version
@treturn string version string
@treturn integer major
@treturn integer minor
@treturn integer patch
*/
static int
lmdb_version(lua_State *L)
{
  int         major, minor, patch;
  const char *version = mdb_version(&major, &minor, &patch);

  lua_pushstring(L, version);
  lua_pushinteger(L, major);
  lua_pushinteger(L, minor);
  lua_pushinteger(L, patch);
  return 4;
};

/***
Convert errno integer to error message
@function strerror
@tparam integer code errno number
@treturn string error message
*/
static int
lmdb_strerror(lua_State *L)
{
  int         err = luaL_checkinteger(L, 1);
  const char *msg = mdb_strerror(err);
  lua_pushstring(L, msg);
  return 1;
}

/***
options for open api

@field maxreaders[opt=1] maximum number of readers allowed
@field flags flags for `mdb_env_open`
@field mode mode The UNIX permissions to set on created files and semaphores,
default 0664
@field mapsize[opt=4M] mapsize for `mdb_env_set_mapsize`, default 4MB
@table options
*/
/***
Open a lmdb file
@function open
@tparam string path the path to the LMDB file
@tparam[opt] table options the options to use when creating or opening the LMDB
file
@treturn[1] env object
@return[2] fail
@see options
*/
static int
lmdb_open(lua_State *L)
{
  const char  *path = luaL_checkstring(L, 1);
  unsigned int flags;
  mdb_mode_t   mode;
  size_t       size;
  int          ret, maxreaders;
  lmdb_env    *env;

  if (lua_gettop(L) == 1) lua_newtable(L);
  luaL_checktype(L, 2, LUA_TTABLE);

  lua_getfield(L, 2, "flags");
  flags = luaL_optinteger(L, -1, MDB_FIXEDMAP | MDB_CREATE);
  lua_pop(L, 1);

  lua_getfield(L, 2, "mode");
  mode = luaL_optinteger(L, -1, 0664);
  lua_pop(L, 1);

  lua_getfield(L, 2, "mapsize");
  size = luaL_optinteger(L, -1, 4 * 1024 * 1024);  // 默认 4MB
  lua_pop(L, 1);

  lua_getfield(L, 2, "maxreaders");
  maxreaders = luaL_optinteger(L, -1, 1);  // 默认 1
  lua_pop(L, 1);

  env = (lmdb_env *)lua_newuserdata(L, sizeof(lmdb_env));
  if (env == NULL) {
    return lmdb_pusherror(L, ENOMEM);
  }

  ret = mdb_env_create(&env->env);
  if (ret != MDB_SUCCESS) {
    return lmdb_pusherror(L, ret);
  }

  ret = mdb_env_set_maxreaders(env->env, maxreaders);
  if (ret != MDB_SUCCESS) {
    return lmdb_pusherror(L, ret);
  }

  ret = mdb_env_set_mapsize(env->env, size);
  if (ret != MDB_SUCCESS) {
    return lmdb_pusherror(L, ret);
  }

  ret = mdb_env_open(env->env, path, flags, mode);
  if (ret != MDB_SUCCESS) {
    return lmdb_pusherror(L, ret);
  }

  luaL_getmetatable(L, LUA_LMDB_ENV);
  lua_setmetatable(L, -2);

  return 1;
}

/***
A env class

@type env
*/

/***
Close an opened LMDB file
@function close
@tparam env handle of env to close
*/
static int
lmdb_close(lua_State *L)
{
  lmdb_env *env = (lmdb_env *)luaL_checkudata(L, 1, LUA_LMDB_ENV);
  if (env->env) {
    void *ctx = mdb_env_get_userctx(env->env);
    if (ctx) {
      int ref = (int)(intptr_t)ctx;
      luaL_unref(L, LUA_REGISTRYINDEX, ref);
    }
    mdb_env_close(env->env);
    env->env = NULL;
  }
  return 0;
}

struct msg_ctx
{
  lua_State *L;
  int        ref;
};

static int
lmdb_msg(const char *msg, void *ctx)
{
  struct msg_ctx *mctx = (struct msg_ctx *)ctx;
  lua_State      *L = mctx->L;
  int             ret;

  lua_rawgeti(L, LUA_REGISTRYINDEX, mctx->ref);
  lua_pushstring(L, msg);
  ret = lua_pcall(L, 1, 1, 0);
  if (ret == 0) {
    return 0;
  } else {
    fprintf(stderr, "error in reader_list: %s\n", lua_tostring(L, -1));
    lua_pop(L, 1);
  }

  return -1;
};

/***
Dump the entries in the reader lock table.
@function reader_list

@tparam function callback(message)
@treturn[1] env self
@return[2] fail
*/
static int
lmdb_reader_list(lua_State *L)
{
  lmdb_env      *env = (lmdb_env *)luaL_checkudata(L, 1, LUA_LMDB_ENV);
  struct msg_ctx ctx;
  int            ret;

  luaL_checktype(L, 2, LUA_TFUNCTION);

  lua_pushvalue(L, 2);
  ctx.ref = luaL_ref(L, LUA_REGISTRYINDEX);
  ctx.L = L;

  ret = mdb_reader_list(env->env, lmdb_msg, &ctx);
  luaL_unref(L, LUA_REGISTRYINDEX, ctx.ref);

  if (ret == MDB_SUCCESS) {
    lua_pushvalue(L, 1);
    return 1;
  }
  return lmdb_pusherror(L, ret);
}

/***
Check for stale entries in the reader lock table.
@function reader_check

@treturn[1] integer dead integer of stale slots that were cleared
@return[2] fail
*/
static int
lmdb_reader_check(lua_State *L)
{
  lmdb_env *env = (lmdb_env *)luaL_checkudata(L, 1, LUA_LMDB_ENV);
  int       dead = 0;

  int ret = mdb_reader_check(env->env, &dead);

  if (ret == MDB_SUCCESS) {
    lua_pushinteger(L, dead);
    return 1;
  }
  return lmdb_pusherror(L, ret);
}

/***
Copy an LMDB environment to the specified path.
@function copy
@tparam string path path copy to
@treturn[1] env self
@return[2] fail
*/

static int
lmdb_copy(lua_State *L)
{
  lmdb_env   *env = (lmdb_env *)luaL_checkudata(L, 1, LUA_LMDB_ENV);
  const char *path = luaL_checkstring(L, 2);
  int         ret = EINVAL;
  if (env->env) {
    ret = mdb_env_copy(env->env, path);
    lua_pushvalue(L, 1);
    return 1;
  }
  return lmdb_pusherror(L, ret);
}

/***
Flush the data buffers to disk.

@function sync
@tparam boolean force if true, force a synchronous flush.
@treturn[1] env self
@return[2] fail
*/
static int
lmdb_sync(lua_State *L)
{
  lmdb_env *env = (lmdb_env *)luaL_checkudata(L, 1, LUA_LMDB_ENV);
  int       force = lua_toboolean(L, 2);

  int ret = EINVAL;
  if (env->env) {
    ret = mdb_env_sync(env->env, force);
    lua_pushvalue(L, 1);
    return 1;
  }
  return lmdb_pusherror(L, ret);
}

/***
Get environment associated property, include `flags`, `path`, `fd`,
`mapsize`, `maxreaders`, `maxkeysize`.

@function get
@tparam string item The property to get
@return[1] value
  - integer for `flags`
  - string for `path`
  - integer for `fd`
  - integer for `mapsize`
  - integer for `maxreaders`
  - integer for `maxkeysize`
@return[2] fail
*/
static int
lmdb_get_property(lua_State *L)
{
  lmdb_env   *env = (lmdb_env *)luaL_checkudata(L, 1, LUA_LMDB_ENV);
  const char *item = luaL_checkstring(L, 2);
  int         ret = EINVAL;

  if (!env->env) {
    return lmdb_pusherror(L, EINVAL);
  }

  if (strcmp(item, "flags") == 0) {
    unsigned int flags;
    ret = mdb_env_get_flags(env->env, &flags);
    if (ret == MDB_SUCCESS) {
      lua_pushinteger(L, flags);
      return 1;
    }
  } else if (strcmp(item, "path") == 0) {
    const char *path = NULL;

    ret = mdb_env_get_path(env->env, &path);
    if (ret == MDB_SUCCESS) {
      lua_pushstring(L, path);
      return 1;
    }
  } else if (strcmp(item, "fd") == 0) {
    mdb_filehandle_t fd = 0;

    ret = mdb_env_get_fd(env->env, &fd);
    if (ret == MDB_SUCCESS) {
      lua_pushinteger(L, fd);
      return 1;
    }
  } else if (strcmp(item, "maxreaders") == 0) {
    unsigned int maxreaders;

    ret = mdb_env_get_maxreaders(env->env, &maxreaders);
    if (ret == MDB_SUCCESS) {
      lua_pushinteger(L, maxreaders);
      return 1;
    }
  } else if (strcmp(item, "maxkeysize") == 0) {
    lua_pushinteger(L, mdb_env_get_maxkeysize(env->env));
    return 1;
  } else if (strcmp(item, "userctx") == 0) {
    int   ref;
    void *ctx = mdb_env_get_userctx(env->env);
    if (ctx == NULL) {
      lua_pushnil(L);
      return 1;
    }
    ref = (int)(intptr_t)ctx;
    lua_rawgeti(L, LUA_REGISTRYINDEX, ref);
    return 1;
  } else {
    luaL_error(L, "unknown property: %s", item);
  }

  return lmdb_pusherror(L, ret);
}

/***
Set environment associated property, include `flags`, `mapsize`, `maxreaders`,
`maxdbs`, `userctx`.

@function set
@tparam string item The property to set
@param ...
  - integer for `flags`
  - string for `path`
  - integer for `fd`
  - integer for `mapsize`
  - integer for `maxreaders`
  - integer for `maxkeysize`
@treturn[1] env self
@return[2] fail
*/

static int
lmdb_set_property(lua_State *L)
{
  lmdb_env   *env = (lmdb_env *)luaL_checkudata(L, 1, LUA_LMDB_ENV);
  const char *item = luaL_checkstring(L, 2);
  int         ret = EINVAL;
  luaL_checkany(L, 3);

  if (strcmp(item, "flags") == 0) {
    unsigned int flags = luaL_checkinteger(L, 3);
    luaL_checkany(L, 4);
    int onoff = lua_toboolean(L, 4);

    ret = mdb_env_set_flags(env->env, flags, onoff);

    if (ret == MDB_SUCCESS) {
      lua_pushvalue(L, 1);
      return 1;
    }
  } else if (strcmp(item, "mapsize") == 0) {
    mdb_size_t size = luaL_checkinteger(L, 3);
    ret = mdb_env_set_mapsize(env->env, size);
    if (ret == MDB_SUCCESS) {
      lua_pushvalue(L, 1);
      return 1;
    }
  } else if (strcmp(item, "maxreaders") == 0) {
    unsigned int maxreaders = luaL_checkinteger(L, 3);
    ret = mdb_env_set_maxreaders(env->env, maxreaders);
    if (ret == MDB_SUCCESS) {
      lua_pushvalue(L, 1);
      return 1;
    }
  } else if (strcmp(item, "maxdbs") == 0) {
    unsigned int maxdbs = luaL_checkinteger(L, 3);
    ret = mdb_env_set_maxdbs(env->env, maxdbs);
    if (ret == MDB_SUCCESS) {
      lua_pushvalue(L, 1);
      return 1;
    }
  } else if (strcmp(item, "userctx") == 0) {
    int ref;
    lua_pushvalue(L, 3);
    ref = luaL_ref(L, LUA_REGISTRYINDEX);
    ret = mdb_env_set_userctx(env->env, (void *)(intptr_t)ref);
    if (ret == MDB_SUCCESS) {
      lua_pushvalue(L, 1);
      return 1;
    }
    luaL_unref(L, LUA_REGISTRYINDEX, ref);
  } else {
    luaL_error(L, "unknown property: %s", item);
  }

  return lmdb_pusherror(L, ret);
}

/***
Return statistics about the LMDB environment.

@function stat
@treturn[1] table the statistics
@return[2] fail
*/
static int
lmdb_stat(lua_State *L)
{
  lmdb_env *env = (lmdb_env *)luaL_checkudata(L, 1, LUA_LMDB_ENV);
  int       ret = EINVAL;
  if (env->env) {
    MDB_stat stat;
    ret = mdb_env_stat(env->env, &stat);
    if (ret == MDB_SUCCESS) {
      return lmdb_pushstat(L, &stat);
    }
  }
  return lmdb_pusherror(L, ret);
}

/***
Return information about the LMDB environment.

@function info
@treturn[1] table the information
@return[2] fail
*/
static int
lmdb_info(lua_State *L)
{
  lmdb_env *env = (lmdb_env *)luaL_checkudata(L, 1, LUA_LMDB_ENV);
  int       ret = EINVAL;
  if (env->env) {
    MDB_envinfo info;
    ret = mdb_env_info(env->env, &info);
    if (ret == MDB_SUCCESS) {
      lua_newtable(L);
      lua_pushinteger(L, info.me_mapsize);
      lua_setfield(L, -2, "mapsize");
      lua_pushinteger(L, info.me_last_pgno);
      lua_setfield(L, -2, "last_pgno");
      lua_pushinteger(L, info.me_last_txnid);
      lua_setfield(L, -2, "last_txnid");
      lua_pushinteger(L, info.me_maxreaders);
      lua_setfield(L, -2, "maxreaders");
      lua_pushinteger(L, info.me_numreaders);
      lua_setfield(L, -2, "numreaders");
      return 1;
    }
  }
  return lmdb_pusherror(L, ret);
}

/***
Create a transaction for use with the environment.

@function begin
@tparam[opt] txn parent
@tparam[opt=0] integer flags the flags for the transaction
@treturn txn the transaction object
@return[2] fail
*/
static int
lmdb_txn_begin(lua_State *L)
{
  lmdb_env *env = (lmdb_env *)luaL_checkudata(L, 1, LUA_LMDB_ENV);
  MDB_txn  *parent = (lua_isuserdata(L, 2) ? (MDB_txn *)luaL_checkudata(L, 2, LUA_LMDB_TXN) : NULL);
  unsigned int flags = parent ? luaL_optinteger(L, 3, 0) : luaL_optinteger(L, 2, 0);
  lmdb_txn    *txn = (lmdb_txn *)lua_newuserdata(L, sizeof(lmdb_txn));
  int          ret = mdb_txn_begin(env->env, parent, flags, &txn->txn);
  if (ret != MDB_SUCCESS) {
    return lmdb_pusherror(L, ret);
  }
  luaL_getmetatable(L, LUA_LMDB_TXN);
  lua_setmetatable(L, -2);

  lua_pushvalue(L, 1);
  txn->env_ref = luaL_ref(L, LUA_REGISTRYINDEX);
  return 1;
}

/***
A txn class

@type txn
*/

/***
Return the transaction's ID.

This returns the identifier associated with this transaction. For a
read-only transaction, this corresponds to the snapshot being read;
concurrent readers will frequently have the same transaction ID.

@function id
@treturn integer the transaction ID
*/
static int
lmdb_txn_id(lua_State *L)
{
  lmdb_txn *txn = (lmdb_txn *)luaL_checkudata(L, 1, LUA_LMDB_TXN);
  lua_pushinteger(L, mdb_txn_id(txn->txn));
  return 1;
}

static void
lmdb_txn_close(lua_State *L, lmdb_txn *txn)
{
  if (txn->txn) {
    txn->txn = NULL;
    luaL_unref(L, LUA_REGISTRYINDEX, txn->env_ref);
    txn->env_ref = LUA_NOREF;
  }
}

/***
Commit all the operations of a transaction into the database.

The transaction handle is freed. It and its cursors must not be used
again after this call, except with #cursor:renew().

@function commit
@treturn[1] boolean
@return[2] fail
*/
static int
lmdb_txn_commit(lua_State *L)
{
  lmdb_txn *txn = (lmdb_txn *)luaL_checkudata(L, 1, LUA_LMDB_TXN);
  int       ret = mdb_txn_commit(txn->txn);
  if (ret == MDB_SUCCESS) {
    lua_pushboolean(L, 1);
    lmdb_txn_close(L, txn);
    return 1;
  }
  return lmdb_pusherror(L, ret);
}

/***
Abandon all the operations of the transaction instead of saving them.
@function abort
*/
static int
lmdb_txn_abort(lua_State *L)
{
  lmdb_txn *txn = (lmdb_txn *)luaL_checkudata(L, 1, LUA_LMDB_TXN);
  mdb_txn_abort(txn->txn);
  lmdb_txn_close(L, txn);
  return 0;
}

/***
Reset a read-only transaction.
@function reset
@treturn lmdb.txn self
*/
static int
lmdb_txn_reset(lua_State *L)
{
  lmdb_txn *txn = (lmdb_txn *)luaL_checkudata(L, 1, LUA_LMDB_TXN);
  mdb_txn_reset(txn->txn);
  lua_pushvalue(L, 1);
  return 1;
}

/***
Renew a read-only transaction.
@function renew

@treturn[1] lmdb.txn self
@return[2] fail
*/
static int
lmdb_txn_renew(lua_State *L)
{
  lmdb_txn *txn = (lmdb_txn *)luaL_checkudata(L, 1, LUA_LMDB_TXN);
  int       ret = mdb_txn_renew(txn->txn);
  if (ret == MDB_SUCCESS) {
    lua_pushvalue(L, 1);
    return 1;
  }
  return lmdb_pusherror(L, ret);
}

/***
Open a database in the environment.
@function dbi_open
@tparam[opt] string name The name of the database to open.
Default only a single database in the environment
@tparam[opt=0] integer flags The flags for the database
@treturn[1] dbi
@return[2] fail
*/
static int
lmdb_dbi_open(lua_State *L)
{
  lmdb_txn    *txn = (lmdb_txn *)luaL_checkudata(L, 1, LUA_LMDB_TXN);
  const char  *name = luaL_optstring(L, 2, NULL);
  unsigned int flags = luaL_optinteger(L, 3, 0);

  lmdb_dbi *dbi = (lmdb_dbi *)lua_newuserdata(L, sizeof(lmdb_dbi));

  int ret = mdb_dbi_open(txn->txn, name, flags, &dbi->dbi);
  if (ret != MDB_SUCCESS) {
    return lmdb_pusherror(L, ret);
  }

  luaL_getmetatable(L, LUA_LMDB_DBI);
  lua_setmetatable(L, -2);

  lua_pushvalue(L, 1);
  dbi->txn_ref = luaL_ref(L, LUA_REGISTRYINDEX);
  dbi->txn = txn->txn;

  return 1;
}

/***
A dbi class.
@type dbi
*/

/***
Retrieve statistics for a database.
@function stat
@treturn[1] table the statistics
@return[2] fail
*/
static int
lmdb_dbi_stat(lua_State *L)
{
  lmdb_dbi *dbi = (lmdb_dbi *)luaL_checkudata(L, 1, LUA_LMDB_DBI);
  MDB_stat  stat;
  int       ret = mdb_stat(dbi->txn, dbi->dbi, &stat);
  if (ret == MDB_SUCCESS) {
    return lmdb_pushstat(L, &stat);
  }
  return lmdb_pusherror(L, ret);
}

/***
Retrieve the DB flags for a database handle.
@function flags
@treturn[1] integer the flags
@return[2] fail
*/
static int
lmdb_dbi_flags(lua_State *L)
{
  lmdb_dbi    *dbi = (lmdb_dbi *)luaL_checkudata(L, 1, LUA_LMDB_DBI);
  unsigned int flags;
  int          ret = mdb_dbi_flags(dbi->txn, dbi->dbi, &flags);
  if (ret == MDB_SUCCESS) {
    lua_pushinteger(L, flags);
    return 1;
  }
  return lmdb_pusherror(L, ret);
}

/***
Drop the DB flags
@function drop
@tparam[opt] bool delete true to delete the database, false to empty it
@treturn[1] dbi self
@return[2] fail
*/
static int
lmdb_dbi_drop(lua_State *L)
{
  lmdb_dbi *dbi = (lmdb_dbi *)luaL_checkudata(L, 1, LUA_LMDB_DBI);
  int      del = luaL_optinteger(L, 2, 0);
  int      ret = mdb_drop(dbi->txn, dbi->dbi, del);
  if (ret == MDB_SUCCESS) {
    lua_pushvalue(L, 1);
    return 1;
  }
  return lmdb_pusherror(L, ret);
}

static int
lmdb_dbi_close(lua_State *L)
{
  lmdb_dbi *dbi = (lmdb_dbi *)luaL_checkudata(L, 1, LUA_LMDB_DBI);
  if (dbi->dbi) {
    lmdb_txn *txn;
    lmdb_env *env;

    lua_rawgeti(L, LUA_REGISTRYINDEX, dbi->txn_ref);
    txn = (lmdb_txn *)luaL_checkudata(L, -1, LUA_LMDB_TXN);
    lua_pop(L, 1);

    if (txn->txn) {
      lua_rawgeti(L, LUA_REGISTRYINDEX, txn->env_ref);
      env = (lmdb_env *)luaL_checkudata(L, -1, LUA_LMDB_ENV);
      lua_pop(L, 1);
      mdb_dbi_close(env->env, dbi->dbi);
    }

    dbi->dbi = 0;
    luaL_unref(L, LUA_REGISTRYINDEX, dbi->txn_ref);
    dbi->txn_ref = LUA_NOREF;
  }
  return 0;
}

/***
Get items from a database.
@function get

@tparam string key the key to get
@treturn[1] string value
@return[2] fail
*/
static int
lmdb_get(lua_State *L)
{
  lmdb_dbi *dbi = (lmdb_dbi *)luaL_checkudata(L, 1, LUA_LMDB_DBI);
  MDB_val key = lmdb_checkvalue(L, 2);
  MDB_val val;

  int rc = mdb_get(dbi->txn, dbi->dbi, &key, &val);
  if (rc == MDB_SUCCESS) {
    lua_pushlstring(L, (const char *)val.mv_data, val.mv_size);
    return 1;
  }
  return lmdb_pusherror(L, rc);
}

/***
Store items into a database.
@function put
@tparam string key the key to set
@tparam string value the value to set
@tparam[opt=0] integer flags
@treturn[1] dbi self
@return[2] fail
*/
static int
lmdb_put(lua_State *L)
{
  lmdb_dbi *dbi = (lmdb_dbi *)luaL_checkudata(L, 1, LUA_LMDB_DBI);
  MDB_val key = lmdb_checkvalue(L, 2);
  MDB_val val = lmdb_checkvalue(L, 3);
  unsigned int flags = luaL_optinteger(L, 4, 0);

  int rc = mdb_put(dbi->txn, dbi->dbi, &key, &val, flags);
  if (rc == MDB_SUCCESS) {
    lua_pushvalue(L, 1);
    return 1;
  }

  return lmdb_pusherror(L, rc);
}

/***
Delete items from a database.
@function del
@tparam string key the key to del
@tparam string value the value to set
@treturn[1] dbi self
@return[2] fail
*/
static int
lmdb_del(lua_State *L)
{
  lmdb_dbi *dbi = (lmdb_dbi *)luaL_checkudata(L, 1, LUA_LMDB_DBI);
  MDB_val key = lmdb_checkvalue(L, 2);

  int rc = mdb_del(dbi->txn, dbi->dbi, &key, NULL);
  if (rc == MDB_SUCCESS) {
    lua_pushvalue(L, 1);
    return 1;
  }

  return lmdb_pusherror(L, rc);
}

/***
Compare two data items according to a particular database.
@function cmp
@tparam string first item to compare,
@tparam string second item to compare
@treturn integer  < 0 if a < b, 0 if a == b, > 0 if a > b
*/
static int
lmdb_cmp(lua_State *L)
{
  lmdb_dbi *dbi = (lmdb_dbi *)luaL_checkudata(L, 1, LUA_LMDB_DBI);

  MDB_val a = lmdb_checkvalue(L, 2);
  MDB_val b = lmdb_checkvalue(L, 3);

  int rc = mdb_cmp(dbi->txn, dbi->dbi, &a, &b);
  lua_pushinteger(L, rc);
  return 1;
}

/***
Compare two data items according to a particular database.

@function dcmp
@tparam string first item to compare,
@tparam string second item to compare
@treturn integer  < 0 if a < b, 0 if a == b, > 0 if a > b
*/
static int
lmdb_dcmp(lua_State *L)
{
  lmdb_dbi *dbi = (lmdb_dbi *)luaL_checkudata(L, 1, LUA_LMDB_DBI);

  MDB_val a = lmdb_checkvalue(L, 2);
  MDB_val b = lmdb_checkvalue(L, 3);

  int rc = mdb_dcmp(dbi->txn, dbi->dbi, &a, &b);
  lua_pushinteger(L, rc);
  return 1;
}

/***
Create a cursor handle.
@function cursor_open

@tparam dbi an dbi handle
@treturn[1] cursor
@return[2] fail
*/

static int
lmdb_cursor_open(lua_State *L)
{
  lmdb_dbi    *dbi = (lmdb_dbi *)luaL_checkudata(L, 1, LUA_LMDB_DBI);
  lmdb_cursor *cursor = (lmdb_cursor *)lua_newuserdata(L, sizeof(lmdb_cursor));

  int ret = mdb_cursor_open(dbi->txn, dbi->dbi, &cursor->cursor);
  if (ret == MDB_SUCCESS) {
    lua_pushvalue(L, 1);
    cursor->dbi_ref = luaL_ref(L, LUA_REGISTRYINDEX);
    luaL_getmetatable(L, LUA_LMDB_CURSOR);
    lua_setmetatable(L, -2);
    return 1;
  }
  return lmdb_pusherror(L, ret);
}

/***
A cursor class.
@type cursor
*/

/***
Close a cursor handle.
@function close

@treturn cursor object or error
*/
static int
lmdb_cursor_close(lua_State *L)
{
  lmdb_cursor *cursor = (lmdb_cursor *)luaL_checkudata(L, 1, LUA_LMDB_CURSOR);
  if (cursor->cursor) {
    mdb_cursor_close(cursor->cursor);
    luaL_unref(L, LUA_REGISTRYINDEX, cursor->dbi_ref);
    cursor->cursor = NULL;
    cursor->dbi_ref = LUA_NOREF;
  }

  return 0;
}

/***
Renew a cursor handle.
@function renew

@treturn cursor object or error
*/
static int
lmdb_cursor_renew(lua_State *L)
{
  lmdb_cursor *cursor = (lmdb_cursor *)luaL_checkudata(L, 1, LUA_LMDB_CURSOR);
  lmdb_dbi    *dbi = NULL;
  int          ret;

  if (cursor->cursor == NULL) {
    return 0;
  }

  lua_rawgeti(L, LUA_REGISTRYINDEX, cursor->dbi_ref);
  dbi = (lmdb_dbi *)lua_touserdata(L, -1);
  lua_pop(L, 1);
  ret = mdb_cursor_renew(dbi->txn, cursor->cursor);
  if (ret == MDB_SUCCESS) {
    lua_pushvalue(L, 1);
    return 1;
  }
  return lmdb_pusherror(L, ret);
}

/***
Return the cursor's transaction handle.
@function txn

@treturn lmdb.txn
*/
static int
lmdb_cursor_txn(lua_State *L)
{
  lmdb_cursor *cursor = (lmdb_cursor *)luaL_checkudata(L, 1, LUA_LMDB_CURSOR);
  lmdb_dbi    *dbi;

  lua_rawgeti(L, LUA_REGISTRYINDEX, cursor->dbi_ref);
  dbi = (lmdb_dbi *)lua_touserdata(L, -1);
  lua_pop(L, 1);

  lua_rawgeti(L, LUA_REGISTRYINDEX, dbi->txn_ref);
  return 1;
}

/***
Return the cursor's database handle.
@function dbi

@treturn dbi
*/
static int
lmdb_cursor_dbi(lua_State *L)
{
  lmdb_cursor *cursor = (lmdb_cursor *)luaL_checkudata(L, 1, LUA_LMDB_CURSOR);
  if (cursor->cursor == NULL) {
    return 0;
  }

  lua_rawgeti(L, LUA_REGISTRYINDEX, cursor->dbi_ref);
  return 1;
}

/***
Retrieve key/val pair by cursor.
@function get

@tparam string key the key to get
@treturn[1] string key
@treturn[1] string value
@return[2] fail
*/
static int
lmdb_cursor_get(lua_State *L)
{
  lmdb_cursor  *cursor = (lmdb_cursor *)luaL_checkudata(L, 1, LUA_LMDB_CURSOR);
  MDB_cursor_op op = luaL_optinteger(L, 2, MDB_NEXT);

  MDB_val key, val;

  int rc = mdb_cursor_get(cursor->cursor, &key, &val, op);
  if (rc == MDB_SUCCESS) {
    lua_pushlstring(L, (const char *)key.mv_data, key.mv_size);
    lua_pushlstring(L, (const char *)val.mv_data, val.mv_size);
    return 2;
  }
  return lmdb_pusherror(L, rc);
}

/***
Store key/value pair by cursor.

@function put
@tparam string key the key to set
@tparam string value the value to set
@tparam[opt=0] integer flags
@treturn[1] cursor self
@return[2] fail
*/
static int
lmdb_cursor_put(lua_State *L)
{
  lmdb_cursor *cursor = (lmdb_cursor *)luaL_checkudata(L, 1, LUA_LMDB_CURSOR);

  MDB_val va = lmdb_checkvalue(L, 2);
  MDB_val vb = lmdb_checkvalue(L, 3);
  unsigned int flags = luaL_optinteger(L, 4, 0);

  int rc = mdb_cursor_put(cursor->cursor, &va, &vb, flags);
  if (rc == MDB_SUCCESS) {
    lua_pushvalue(L, 1);
    return 1;
  }

  return lmdb_pusherror(L, rc);
}

/***
Delete current key/data pair
@function del
@tparam[opt=0] integer flags
@treturn[1] cursor self
@return[2] fail
*/
static int
lmdb_cursor_del(lua_State *L)
{
  lmdb_cursor *cursor = (lmdb_cursor *)luaL_checkudata(L, 1, LUA_LMDB_CURSOR);
  unsigned int flags = luaL_optinteger(L, 2, 0);

  int rc = mdb_cursor_del(cursor->cursor, flags);
  if (rc == MDB_SUCCESS) {
    lua_pushvalue(L, 1);
    return 1;
  }

  return lmdb_pusherror(L, rc);
}

/***
Count key/data pairs.
@function count
@treturn[1] int
@return[2] fail
*/
static int
lmdb_cursor_count(lua_State *L)
{
  lmdb_cursor *cursor = (lmdb_cursor *)luaL_checkudata(L, 1, LUA_LMDB_CURSOR);
  mdb_size_t   count = 0;

  int rc = mdb_cursor_count(cursor->cursor, &count);
  if (rc == MDB_SUCCESS) {
    lua_pushinteger(L, count);
    return 1;
  }

  return lmdb_pusherror(L, rc);
}

static void
auxiliar_newclass(lua_State *L, const char *classname, const luaL_Reg *func)
{
  luaL_newmetatable(L, classname); /* mt */
  /* create __index table to place methods */
  lua_pushstring(L, "__index"); /* mt,"__index" */
  lua_newtable(L); /* mt,"__index",it */
  /* put class name into class metatable */
  lua_pushstring(L, "class"); /* mt,"__index",it,"class" */
  lua_pushstring(L, classname); /* mt,"__index",it,"class",classname */
  lua_rawset(L, -3); /* mt,"__index",it */
  /* pass all methods that start with _ to the metatable, and all others
   * to the index table */
  for (; func->name; func++) { /* mt,"__index",it */
    lua_pushstring(L, func->name);
    lua_pushcfunction(L, func->func);
    lua_rawset(L, func->name[0] == '_' ? -5 : -3);
  }
  lua_rawset(L, -3); /* mt */
  lua_pop(L, 1);
}

static int
auxiliar_tostring(lua_State *L)
{
  char buf[32];
  if (!lua_getmetatable(L, 1)) goto error;
  lua_pushstring(L, "__index");
  lua_gettable(L, -2);
  if (!lua_istable(L, -1)) goto error;
  lua_pushstring(L, "class");
  lua_gettable(L, -2);
  if (!lua_isstring(L, -1)) goto error;
  snprintf(buf, sizeof(buf), "%p", lua_touserdata(L, 1));
  lua_pushfstring(L, "%s: %s", lua_tostring(L, -1), buf);
  return 1;
error:
  lua_pushstring(L, "invalid object passed to 'auxiliar.c:__tostring'");
  lua_error(L);
  return 1;
}

// 模块方法列表
static const luaL_Reg env_methods[] = {
  { "txn_begin",    lmdb_txn_begin    },
  { "close",        lmdb_close        },
  { "copy",         lmdb_copy         },
  { "sync",         lmdb_sync         },
  { "get",          lmdb_get_property },
  { "set",          lmdb_set_property },
  { "stat",         lmdb_stat         },
  { "info",         lmdb_info         },
  { "reader_list",  lmdb_reader_list  },
  { "reader_check", lmdb_reader_check },

  { "__tostring",   auxiliar_tostring },
  { NULL,           NULL              }
};

static const luaL_Reg txn_methods[] = {
  { "commit",     lmdb_txn_commit   },
  { "abort",      lmdb_txn_abort    },
  { "reset",      lmdb_txn_reset    },
  { "renew",      lmdb_txn_renew    },
  { "id",         lmdb_txn_id       },
  { "dbi_open",   lmdb_dbi_open     },

  { "__tostring", auxiliar_tostring },
  { NULL,         NULL              }
};

static const luaL_Reg dbi_methods[] = {
  { "cmp",        lmdb_cmp          },
  { "dcmp",       lmdb_dcmp         },
  { "put",        lmdb_put          },
  { "del",        lmdb_del          },
  { "get",        lmdb_get          },
  { "stat",       lmdb_dbi_stat     },
  { "flags",      lmdb_dbi_flags    },
  { "flags",      lmdb_dbi_drop    },
  { "close",      lmdb_dbi_close },
  { "cursor_open",       lmdb_cursor_open  },

  { "__gc",lmdb_dbi_close },
  { "__tostring", auxiliar_tostring },
  { NULL,         NULL              }
};

static const luaL_Reg cursor_methods[] = {
  { "renew",      lmdb_cursor_renew },
  { "close",      lmdb_cursor_close },
  { "dbi",        lmdb_cursor_dbi   },
  { "txn",        lmdb_cursor_txn   },
  { "get",        lmdb_cursor_get   },
  { "put",        lmdb_cursor_put   },
  { "del",        lmdb_cursor_del   },
  { "count",      lmdb_cursor_count },
  { "__gc",      lmdb_cursor_close },

  { "__tostring", auxiliar_tostring },
  { NULL,         NULL              }
};

// 注册全局函数
static const luaL_Reg funcs[] = {
  { "version",  lmdb_version  },
  { "strerror", lmdb_strerror },
  { "open",     lmdb_open     },

  { NULL,       NULL          }
};

#define LDBM_ENUM(name)                                                                            \
  {                                                                                                \
    lua_pushliteral(L, #name);                                                                     \
    lua_pushinteger(L, MDB_##name);                                                                \
    lua_rawset(L, -3);                                                                             \
  }

// 模块入口函数
LUA_API int
luaopen_lmdb(lua_State *L)
{
  auxiliar_newclass(L, LUA_LMDB_ENV, env_methods);
  auxiliar_newclass(L, LUA_LMDB_TXN, txn_methods);
  auxiliar_newclass(L, LUA_LMDB_DBI, dbi_methods);
  auxiliar_newclass(L, LUA_LMDB_CURSOR, cursor_methods);

  luaL_newlib(L, funcs);

  lua_pushliteral(L, "ENV_FLAG");
  lua_newtable(L);
  LDBM_ENUM(FIXEDMAP);
  LDBM_ENUM(NOSUBDIR);
  LDBM_ENUM(NOSYNC);
  LDBM_ENUM(RDONLY);
  LDBM_ENUM(NOMETASYNC);
  LDBM_ENUM(WRITEMAP);
  LDBM_ENUM(MAPASYNC);
  LDBM_ENUM(NOTLS);
  LDBM_ENUM(NOLOCK);
  LDBM_ENUM(NORDAHEAD);
  LDBM_ENUM(NOMEMINIT);
  LDBM_ENUM(PREVSNAPSHOT);
  lua_rawset(L, -3);

  lua_pushliteral(L, "CODE");
  lua_newtable(L);
  LDBM_ENUM(SUCCESS);
  LDBM_ENUM(KEYEXIST);
  LDBM_ENUM(NOTFOUND);
  LDBM_ENUM(PAGE_NOTFOUND);
  LDBM_ENUM(CORRUPTED);
  LDBM_ENUM(PANIC);
  LDBM_ENUM(VERSION_MISMATCH);
  LDBM_ENUM(INVALID)
  LDBM_ENUM(MAP_FULL)
  LDBM_ENUM(DBS_FULL);
  LDBM_ENUM(READERS_FULL);
  LDBM_ENUM(TLS_FULL);
  LDBM_ENUM(TXN_FULL);
  LDBM_ENUM(CURSOR_FULL);
  LDBM_ENUM(PAGE_FULL);
  LDBM_ENUM(MAP_RESIZED);
  LDBM_ENUM(INCOMPATIBLE);
  LDBM_ENUM(BAD_RSLOT);
  LDBM_ENUM(BAD_TXN);
  LDBM_ENUM(BAD_VALSIZE);
  LDBM_ENUM(BAD_DBI);
  LDBM_ENUM(PROBLEM);
  LDBM_ENUM(LAST_ERRCODE);
  lua_rawset(L, -3);

  lua_pushliteral(L, "DBI_FLAG");
  lua_newtable(L);
  LDBM_ENUM(REVERSEKEY);
  LDBM_ENUM(DUPSORT);
  LDBM_ENUM(INTEGERKEY);
  LDBM_ENUM(DUPFIXED);
  LDBM_ENUM(INTEGERDUP);
  LDBM_ENUM(REVERSEDUP);
  LDBM_ENUM(CREATE);
  lua_rawset(L, -3);

  lua_pushliteral(L, "CUR_OP");
  lua_newtable(L);
  LDBM_ENUM(FIRST);
  LDBM_ENUM(FIRST_DUP);
  LDBM_ENUM(GET_BOTH);
  LDBM_ENUM(GET_BOTH_RANGE);
  LDBM_ENUM(GET_CURRENT);
  LDBM_ENUM(GET_MULTIPLE);
  LDBM_ENUM(LAST);
  LDBM_ENUM(LAST_DUP);
  LDBM_ENUM(NEXT);
  LDBM_ENUM(NEXT_DUP);
  LDBM_ENUM(NEXT_MULTIPLE);
  LDBM_ENUM(NEXT_NODUP);
  LDBM_ENUM(PREV);
  LDBM_ENUM(PREV_DUP);
  LDBM_ENUM(PREV_NODUP);
  LDBM_ENUM(SET);
  LDBM_ENUM(SET_KEY);
  LDBM_ENUM(SET_RANGE);
  LDBM_ENUM(PREV_MULTIPLE);
  lua_rawset(L, -3);

  lua_pushliteral(L, "WRITE_FLAG");
  lua_newtable(L);
  LDBM_ENUM(NOOVERWRITE);
  LDBM_ENUM(NODUPDATA);
  LDBM_ENUM(CURRENT);
  LDBM_ENUM(RESERVE);
  LDBM_ENUM(APPEND);
  LDBM_ENUM(APPENDDUP);
  LDBM_ENUM(MULTIPLE);
  lua_rawset(L, -3);

  return 1;
}
