--------------------------------------------------------------------------------
-- A library for interfacing with SQLite3 databases.
--
-- Copyright (C) 2011-2016 Stefano Peluchetti. All rights reserved.
--
-- Features, documentation and more: http://www.scilua.org .
--
-- This file is part of the LJSQLite3 library, which is released under the MIT
-- license: full text in file LICENSE.TXT in the library's root folder.
--------------------------------------------------------------------------------
--[[lit-meta
  name = "SinisterRectus/sqlite3"
  version = "1.0.0-1"
  homepage = "http://scilua.org/ljsqlite3.html"
  description = "SciLua's sqlite3 bindings repackaged for lit."
  tags = {"database", "sqlite3"}
  license = "MIT"
  author = "Stefano Peluchetti"
  contributors = {
    "Sinister Rectus"
  }
]]
--------------------------------------------------------------------------------

-- TODO: Refactor according to latest style / coding guidelines.
-- TODO: introduce functionality to get of a defined type to avoid if check?
-- TODO: Add extended error codes from Sqlite?
-- TODO: Consider type checks?
-- TODO: Exposed cdef constants are ok?
-- TODO: Resultset (and so exec) could be optimized by avoiding loads/stores
-- TODO: of row table via _step?

local ffi  = require "ffi"
local bit  = require "bit"
local jit  = require "jit"

---- xsys replacement ----------------------------------------------------------

local insert = table.insert
local match, gmatch = string.match, string.gmatch

local function split(str, delim)
  local words = {}
    for word in gmatch(str .. delim, '(.-)' .. delim) do
      insert(words, word)
    end
  return words
end

local function trim(str)
  return match(str, '^%s*(.-)%s*$')
end

--------------------------------------------------------------------------------

local function err(code, msg)
  error("ljsqlite3["..code.."] "..msg)
end

-- Codes -----------------------------------------------------------------------
local sqlconstants = {} -- SQLITE_* and OPEN_* declarations.
local codes = {
  [0] = "OK", "ERROR", "INTERNAL", "PERM", "ABORT", "BUSY", "LOCKED", "NOMEM",
  "READONLY", "INTERRUPT", "IOERR", "CORRUPT", "NOTFOUND", "FULL", "CANTOPEN",
  "PROTOCOL", "EMPTY", "SCHEMA", "TOOBIG", "CONSTRAINT", "MISMATCH", "MISUSE",
  "NOLFS", "AUTH", "FORMAT", "RANGE", "NOTADB", [100] = "ROW", [101] = "DONE"
} -- From 0 to 26.

do
  local types = { "INTEGER", "FLOAT", "TEXT", "BLOB", "NULL" } -- From 1 to 5.

  local opens = {
    READONLY =        0x00000001;
    READWRITE =       0x00000002;
    CREATE =          0x00000004;
    DELETEONCLOSE =   0x00000008;
    EXCLUSIVE =       0x00000010;
    AUTOPROXY =       0x00000020;
    URI =             0x00000040;
    MAIN_DB =         0x00000100;
    TEMP_DB =         0x00000200;
    TRANSIENT_DB =    0x00000400;
    MAIN_JOURNAL =    0x00000800;
    TEMP_JOURNAL =    0x00001000;
    SUBJOURNAL =      0x00002000;
    MASTER_JOURNAL =  0x00004000;
    NOMUTEX =         0x00008000;
    FULLMUTEX =       0x00010000;
    SHAREDCACHE =     0x00020000;
    PRIVATECACHE =    0x00040000;
    WAL =             0x00080000;
  }

  local t = sqlconstants
  local pre = "static const int32_t SQLITE_"
  for i=0,26    do t[#t+1] = pre..codes[i].."="..i..";\n" end
  for i=100,101 do t[#t+1] = pre..codes[i].."="..i..";\n" end
  for i=1,5     do t[#t+1] = pre..types[i].."="..i..";\n" end
  pre = pre.."OPEN_"
  for k,v in pairs(opens) do t[#t+1] = pre..k.."="..bit.tobit(v)..";\n" end
end

-- Cdef ------------------------------------------------------------------------
-- SQLITE_*, OPEN_*
ffi.cdef(table.concat(sqlconstants))

-- sqlite3*, ljsqlite3_*
ffi.cdef[[
// Typedefs.
typedef struct sqlite3 sqlite3;
typedef struct sqlite3_stmt sqlite3_stmt;
typedef void (*sqlite3_destructor_type)(void*);
typedef struct sqlite3_context sqlite3_context;
typedef struct Mem sqlite3_value;

// Get informative error message.
const char *sqlite3_errmsg(sqlite3*);

// Connection.
int sqlite3_open_v2(const char *filename, sqlite3 **ppDb, int flags,
  const char *zVfs);
int sqlite3_close(sqlite3*);
int sqlite3_busy_timeout(sqlite3*, int ms);

// Statement.
int sqlite3_prepare_v2(sqlite3 *conn, const char *zSql, int nByte,
  sqlite3_stmt **ppStmt, const char **pzTail);
int sqlite3_step(sqlite3_stmt*);
int sqlite3_reset(sqlite3_stmt *pStmt);
int sqlite3_finalize(sqlite3_stmt *pStmt);

// Extra functions for SELECT.
int sqlite3_column_count(sqlite3_stmt *pStmt);
const char *sqlite3_column_name(sqlite3_stmt*, int N);
int sqlite3_column_type(sqlite3_stmt*, int iCol);

// Get value from SELECT.
int64_t sqlite3_column_int64(sqlite3_stmt*, int iCol);
double sqlite3_column_double(sqlite3_stmt*, int iCol);
int sqlite3_column_bytes(sqlite3_stmt*, int iCol);
const unsigned char *sqlite3_column_text(sqlite3_stmt*, int iCol);
const void *sqlite3_column_blob(sqlite3_stmt*, int iCol);

// Set value in bind.
int sqlite3_bind_int64(sqlite3_stmt*, int, int64_t);
int sqlite3_bind_double(sqlite3_stmt*, int, double);
int sqlite3_bind_null(sqlite3_stmt*, int);
int sqlite3_bind_text(sqlite3_stmt*, int, const char*, int n, void(*)(void*));
int sqlite3_bind_blob(sqlite3_stmt*, int, const void*, int n, void(*)(void*));

// Clear bindings.
int sqlite3_clear_bindings(sqlite3_stmt*);

// Get value in callbacks.
int sqlite3_value_type(sqlite3_value*);
int64_t sqlite3_value_int64(sqlite3_value*);
double sqlite3_value_double(sqlite3_value*);
int sqlite3_value_bytes(sqlite3_value*);
const unsigned char *sqlite3_value_text(sqlite3_value*); //Not used.
const void *sqlite3_value_blob(sqlite3_value*);

// Set value in callbacks.
void sqlite3_result_error(sqlite3_context*, const char*, int);
void sqlite3_result_int64(sqlite3_context*, int64_t);
void sqlite3_result_double(sqlite3_context*, double);
void sqlite3_result_null(sqlite3_context*);
void sqlite3_result_text(sqlite3_context*, const char*, int, void(*)(void*));
void sqlite3_result_blob(sqlite3_context*, const void*, int, void(*)(void*));

// Persistency of data in callbacks (here just a pointer for tagging).
void *sqlite3_aggregate_context(sqlite3_context*, int nBytes);

// Typedefs for callbacks.
typedef void (*ljsqlite3_cbstep)(sqlite3_context*,int,sqlite3_value**);
typedef void (*ljsqlite3_cbfinal)(sqlite3_context*);

// Register callbacks.
int sqlite3_create_function(
  sqlite3 *conn,
  const char *zFunctionName,
  int nArg,
  int eTextRep,
  void *pApp,
  void (*xFunc)(sqlite3_context*,int,sqlite3_value**),
  void (*xStep)(sqlite3_context*,int,sqlite3_value**),
  void (*xFinal)(sqlite3_context*)
);
]]

--------------------------------------------------------------------------------
local sql = ffi.load("sqlite3")

local transient = ffi.cast("sqlite3_destructor_type", -1)
local int64_ct = ffi.typeof("int64_t")

local blob_mt = {} -- For tagging only.

local function blob(str)
  return setmetatable({ str }, blob_mt)
end

local connstmt = {} -- Statements for a conn.
local conncb = {} -- Callbacks for a conn.
local aggregatestate = {} -- Aggregate states.

local stmt_step

local stmt_mt, stmt_ct = {}
stmt_mt.__index = stmt_mt

local conn_mt, conn_ct = {}
conn_mt.__index = conn_mt

-- Checks ----------------------------------------------------------------------

-- Helper function to get error msg and code from sqlite.
local function codemsg(pconn, code)
  return codes[code]:lower(), ffi.string(sql.sqlite3_errmsg(pconn))
end

-- Throw error for a given connection.
local function E_conn(pconn, code)
  local code, msg = codemsg(pconn, code)
  err(code, msg)
end

-- Test code is OK or throw error for a given connection.
local function T_okcode(pconn, code)
  if code ~= sql.SQLITE_OK then
    E_conn(pconn, code)
  end
end

local function T_open(x)
  if x._closed then
    err("misuse", "object is closed")
  end
end

-- Getters / Setters to minimize code duplication ------------------------------
local sql_get_code = [=[
return function(stmt_or_value <opt_i>)
  local t = sql.sqlite3_<variant>_type(stmt_or_value <opt_i>)
  if t == sql.SQLITE_INTEGER then
    return sql.sqlite3_<variant>_int64(stmt_or_value <opt_i>)
  elseif t == sql.SQLITE_FLOAT then
    return sql.sqlite3_<variant>_double(stmt_or_value <opt_i>)
  elseif t == sql.SQLITE_TEXT then
    local nb = sql.sqlite3_<variant>_bytes(stmt_or_value <opt_i>)
    return ffi.string(sql.sqlite3_<variant>_text(stmt_or_value <opt_i>), nb)
  elseif t == sql.SQLITE_BLOB then
    local nb = sql.sqlite3_<variant>_bytes(stmt_or_value <opt_i>)
    return ffi.string(sql.sqlite3_<variant>_blob(stmt_or_value <opt_i>), nb)
  elseif t == sql.SQLITE_NULL then
    return nil
  else
    err("constraint", "unexpected SQLite3 type")
  end
end
]=]

local sql_set_code = [=[
return function(stmt_or_value, v <opt_i>)
  local t = type(v)
  if ffi.istype(int64_ct, v) then
    return sql.sqlite3_<variant>_int64(stmt_or_value <opt_i>, v)
  elseif t == "number" then
    return sql.sqlite3_<variant>_double(stmt_or_value <opt_i>, v)
  elseif t == "string" then
    return sql.sqlite3_<variant>_text(stmt_or_value <opt_i>, v, #v,
      transient)
  elseif t == "table" and getmetatable(v) == blob_mt then
    v = v[1]
    return sql.sqlite3_<variant>_blob(stmt_or_value <opt_i>, v, #v,
      transient)
  elseif t == "nil" then
    return sql.sqlite3_<variant>_null(stmt_or_value <opt_i>)
  else
    err("constraint", "unexpected Lua type")
  end
end
]=]

-- Environment for setters/getters.
local sql_env = {
  sql          = sql,
  transient    = transient,
  ffi          = ffi,
  int64_ct     = int64_ct,
  blob_mt      = blob_mt,
  getmetatable = getmetatable,
  err          = err,
  type         = type
}

local function sql_format(s, variant, index)
  return s:gsub("<variant>", variant):gsub("<opt_i>", index)
end

local function loadcode(s, env)
  local ret = assert(loadstring(s))
  if env then setfenv(ret, env) end
  return ret()
end

-- Must always be called from *:_* function due to error level 4.
local get_column = loadcode(sql_format(sql_get_code, "column", ",i"),	sql_env)
local get_value  = loadcode(sql_format(sql_get_code, "value" , "  "),	sql_env)
local set_column = loadcode(sql_format(sql_set_code, "bind"  , ",i"),	sql_env)
local set_value  = loadcode(sql_format(sql_set_code, "result", "  "),	sql_env)

-- Connection ------------------------------------------------------------------
local open_modes = {
  ro = sql.SQLITE_OPEN_READONLY,
  rw = sql.SQLITE_OPEN_READWRITE,
  rwc = bit.bor(sql.SQLITE_OPEN_READWRITE, sql.SQLITE_OPEN_CREATE)
}

local function open(str, mode)
  mode = mode or "rwc"
  mode = open_modes[mode]
  if not mode then
    err("constraint", "argument #2 to open must be ro, rw, or rwc")
  end
  local aptr = ffi.new("sqlite3*[1]")
  -- Usually aptr is set even if error code, so conn always needs to be closed.
  local code = sql.sqlite3_open_v2(str, aptr, mode, nil)
  local conn = conn_ct(aptr[0], false)
  -- Must create this anyway due to conn:close() function.
  connstmt[conn] = setmetatable({}, { __mode = "k" })
  conncb[conn] = { scalar = {}, step = {}, final = {} }
  if code ~= sql.SQLITE_OK then
    local code, msg = codemsg(conn._ptr, code) -- Before closing!
    conn:close() -- Free resources, should not fail here in this case!
    err(code, msg)
  end
  return conn
end

function conn_mt:close() T_open(self)
   -- Close all stmt linked to conn.
  for k,_ in pairs(connstmt[self]) do if not k._closed then k:close() end end
   -- Close all callbacks linked to conn.
  for _,v in pairs(conncb[self].scalar) do v:free() end
  for _,v in pairs(conncb[self].step)   do v:free() end
  for _,v in pairs(conncb[self].final)  do v:free() end
  local code = sql.sqlite3_close(self._ptr)
  T_okcode(self._ptr, code)
  connstmt[self] = nil -- Table connstmt is not weak, need to clear manually.
  conncb[self] = nil
  self._closed = true -- Set only if close succeded.
end

function conn_mt:__gc()
  if not self._closed then self:close() end
end

function conn_mt:prepare(stmtstr) T_open(self)
  local aptr = ffi.new("sqlite3_stmt*[1]")
  -- If error code aptr NULL, so no need to close anything.
  local code = sql.sqlite3_prepare_v2(self._ptr, stmtstr, #stmtstr, aptr, nil)
  T_okcode(self._ptr, code)
  local stmt = stmt_ct(aptr[0], false, self._ptr, code)
  connstmt[self][stmt] = true
  return stmt
end

-- Connection exec, __call, rowexec --------------------------------------------
function conn_mt:exec(commands, get) T_open(self)
  local cmd1 = split(commands, ";")
  local res, n
  for i=1,#cmd1 do
    local cmd = trim(cmd1[i])
    if #cmd > 0 then
      local stmt = self:prepare(cmd)
      res, n = stmt:resultset(get)
      stmt:close()
    end
  end
  return res, n -- Only last record is returned.
end

function conn_mt:rowexec(command) T_open(self)
  local stmt = self:prepare(command)
  local res = stmt:_step()
  if stmt:_step() then
    err("misuse", "multiple records returned, 1 expected")
  end
  stmt:close()
  return unpack(res)
end

function conn_mt:__call(commands, out) T_open(self)
  out = out or print
  local cmd1 = split(commands, ";")
  for c=1,#cmd1 do
    local cmd = trim(cmd1[c])
    if #cmd > 0 then
      local stmt = self:prepare(cmd)
      local ret, n = stmt:resultset()
      if ret then -- All the results get handled, not only last one.
        out(unpack(ret[0])) -- Headers are printed.
        for i=1,n do
          local o = {}
          for j=1,#ret[0] do
            local v = ret[j][i]
            if type(v) == "nil" then v = "" end -- Empty strings for NULLs.
            o[#o+1] = tostring(v)
          end
          out(unpack(o))
        end
      end
      stmt:close()
    end
  end
end

-- Callbacks -------------------------------------------------------------------
-- Update (one of) callbacks registry for sqlite functions.
local function updatecb(self, where, name, f)
  local cbs = conncb[self][where]
  if cbs[name] then -- Callback already present, free old one.
    cbs[name]:free()
  end
  cbs[name] = f -- Could be nil and that's fine.
end

-- Return manually casted callback that sqlite expects, scalar.
local function scalarcb(name, f)
  local values = {} -- Conversion buffer.
  local function sqlf(context, nvalues, pvalues)
    -- Indexing 0,N-1.
    for i=1,nvalues do values[i] = get_value(pvalues[i - 1]) end
    -- Throw error via sqlite function if necessary.
    local ok, result = pcall(f, unpack(values, 1, nvalues))
    if not ok then
      local msg = "Lua registered scalar function "..name.." error: "..result
      sql.sqlite3_result_error(context, msg, #msg)
    else
      set_value(context, result)
    end
  end
  return ffi.cast("ljsqlite3_cbstep", sqlf)
end

-- Return the state for aggregate case (created via initstate()). We use the ptr
-- returned from aggregate_context() for tagging only, all the state data is
-- handled from Lua side.
local function getstate(context, initstate, size)
  -- Only pointer address relevant for indexing, size irrelevant.
  local ptr = sql.sqlite3_aggregate_context(context, size)
  local pid = tonumber(ffi.cast("intptr_t",ptr))
  local state = aggregatestate[pid]
  if type(state) == "nil" then
    state = initstate()
    aggregatestate[pid] = state
  end
  return state, pid
end

-- Return manually casted callback that sqlite expects, stepper for aggregate.
local function stepcb(name, f, initstate)
  local values = {} -- Conversion buffer.
  local function sqlf(context, nvalues, pvalues)
    -- Indexing 0,N-1.
    for i=1,nvalues do values[i] = get_value(pvalues[i - 1]) end
    local state = getstate(context, initstate, 1)
    -- Throw error via sqlite function if necessary.
    local ok, result = pcall(f, state, unpack(values, 1, nvalues))
    if not ok then
      local msg = "Lua registered step function "..name.." error: "..result
      sql.sqlite3_result_error(context, msg, #msg)
    end
  end
  return ffi.cast("ljsqlite3_cbstep", sqlf)
end

-- Return manually casted callback that sqlite expects, finalizer for aggregate.
local function finalcb(name, f, initstate)
  local function sqlf(context)
    local state, pid = getstate(context, initstate, 0)
    aggregatestate[pid] = nil -- Clear the state.
    local ok, result = pcall(f, state)
    -- Throw error via sqlite function if necessary.
    if not ok then
      local msg = "Lua registered final function "..name.." error: "..result
      sql.sqlite3_result_error(context, msg, #msg)
    else
      set_value(context, result)
    end
  end
  return ffi.cast("ljsqlite3_cbfinal", sqlf)
end

function conn_mt:setscalar(name, f) T_open(self)
  jit.off(stmt_step) -- Necessary to avoid bad calloc in some use cases.
  local cbf = f and scalarcb(name, f) or nil
  local code = sql.sqlite3_create_function(self._ptr, name, -1, 5, nil,
    cbf, nil, nil) -- If cbf nil this clears the function is sqlite.
  T_okcode(self._ptr, code)
  updatecb(self, "scalar", name, cbf) -- Update and clear old.
end

function conn_mt:setaggregate(name, initstate, step, final) T_open(self)
  jit.off(stmt_step) -- Necessary to avoid bad calloc in some use cases.
  local cbs = step  and stepcb (name, step,  initstate) or nil
  local cbf = final and finalcb(name, final, initstate) or nil
  local code = sql.sqlite3_create_function(self._ptr, name, -1, 5, nil,
    nil, cbs, cbf) -- If cbs, cbf nil this clears the function is sqlite.
  T_okcode(self._ptr, code)
  updatecb(self, "step", name, cbs) -- Update and clear old.
  updatecb(self, "final", name, cbf) -- Update and clear old.
end

conn_ct = ffi.metatype("struct { sqlite3* _ptr; bool _closed; }", conn_mt)

-- Statement -------------------------------------------------------------------
function stmt_mt:reset() T_open(self)
  -- Ignore possible error code, it would be repetition of error raised during
  -- most recent evaluation of statement which would have been raised already.
  sql.sqlite3_reset(self._ptr)
  self._code = sql.SQLITE_OK -- Always succeds.
  return self
end

function stmt_mt:close() T_open(self)
  -- Ignore possible error code, it would be repetition of error raised during
  -- most recent evaluation of statement which would have been raised already.
  sql.sqlite3_finalize(self._ptr)
  self._code = sql.SQLITE_OK -- Always succeds.
  self._closed = true -- Must be called exaclty once.
end

function stmt_mt:__gc()
  if not self._closed then self:close() end
end

-- Statement step, resultset ---------------------------------------------------
function stmt_mt:_ncol()
  return sql.sqlite3_column_count(self._ptr)
end

function stmt_mt:_header(h)
  for i=1,self:_ncol() do -- Here indexing 0,N-1.
    h[i] = ffi.string(sql.sqlite3_column_name(self._ptr, i - 1))
  end
end

stmt_step = function(self, row, header)
  -- Must check code ~= SQL_DONE or sqlite3_step --> undefined result.
  if self._code == sql.SQLITE_DONE then return nil end -- Already finished.
  self._code = sql.sqlite3_step(self._ptr)
  if self._code == sql.SQLITE_ROW then
    -- All the sql.* functions called never errors here.
    row = row or {}
    for i=1,self:_ncol() do
      row[i] = get_column(self._ptr, i - 1)
    end
    if header then self:_header(header) end
    return row, header
  elseif self._code == sql.SQLITE_DONE then -- Have finished now.
    return nil
  else -- If code not DONE or ROW then it's error.
    E_conn(self._conn, self._code)
  end
end
stmt_mt._step = stmt_step

function stmt_mt:step(row, header) T_open(self)
  return self:_step(row, header)
end

function stmt_mt:resultset(get, maxrecords) T_open(self)
  get = get or "hik"
  maxrecords = maxrecords or math.huge
  if maxrecords < 1 then
    err("constraint", "agument #1 to resultset must be >= 1")
  end
  local hash, hasi, hask = get:find("h"), get:find("i"), get:find("k")
  local r, h = self:_step({}, {})
  if not r then return nil, 0 end -- No records case.
  -- First record, o is a temporary table used to get records.
  local o = hash and { [0] = h } or {}
  for i=1,#h do o[i] = { r[i] } end
  -- Other records.
  local n = 1
  while n < maxrecords and self:_step(r) do
    n = n + 1
    for i=1,#h do o[i][n] = r[i] end
  end

  local out = { [0] = o[0] } -- Eventually copy colnames.
  if hasi then -- Use numeric indexes.
    for i=1,#h do out[i] = o[i] end
  end
  if hask then -- Use colnames indexes.
    for i=1,#h do out[h[i]] = o[i] end
  end
  return out, n
end

-- Statement bind --------------------------------------------------------------
function stmt_mt:_bind1(i, v)
  local code = set_column(self._ptr, v, i) -- Here indexing 1,N.
  T_okcode(self._conn, code)
  return self
end

function stmt_mt:bind1(i, v) T_open(self)
  return self:_bind1(i, v)
end

function stmt_mt:bind(...) T_open(self)
  for i=1,select("#", ...) do self:_bind1(i, select(i, ...)) end
  return self
end

function stmt_mt:clearbind() T_open(self)
  local code = sql.sqlite3_clear_bindings(self._ptr)
  T_okcode(self._conn, code)
  return self
end

stmt_ct = ffi.metatype([[struct {
  sqlite3_stmt* _ptr;
  bool          _closed;
  sqlite3*      _conn;
  int32_t       _code;
}]], stmt_mt)

return {
  open = open,
  blob = blob,
}
