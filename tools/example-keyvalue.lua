--[[
This is a simple example of using SQLite for key-value storage.
Make sure your require paths are correct and that you have an sqlite library.

Usage:

local Database = require('./example-keyvalue') -- path to this file

local db = Database('test') -- open a database file by name (or create a new one)
local tbl = db:getTable('table') -- get a table by name (or create a new one)

tbl:set('foo', 'bar') -- set values
local foo = tbl:get('foo') -- get values
tbl:set('foo', nil) -- use nil to delete values
tbl:set(42, '1234') -- numbers work, too
]]

local sql = require('./sqlite3')

local f = string.format

local Database = {}
Database.__index = Database

local Table = {}
Table.__index = Table

setmetatable(Database, {__call = function(self, name)
	return setmetatable({
		conn = sql.open(name .. '.db'),
		name = name,
	}, self)
end})

function Database:getTable(name)
	self.conn:exec(f("CREATE TABLE IF NOT EXISTS %q (key PRIMARY KEY, value);", name))
	return Table(name, self)
end

setmetatable(Table, {__call = function(self, name, database)
	local conn = database.conn
	return setmetatable({
		stmts = {
			get = conn:prepare(f("SELECT value FROM %q WHERE key == ?;", name)),
			set = conn:prepare(f("INSERT OR REPLACE INTO %q VALUES (?, ?);", name)),
			delete = conn:prepare(f("DELETE FROM %q WHERE key == ?;", name)),
		},
		name = name,
		database = database,
	}, self)
end})

function Table:get(k)
	local stmt = self.stmts.get
	local res = stmt:reset():bind(k):step()
	return res and res[1]
end

function Table:set(k, v)
	if v == nil then
		local stmt = self.stmts.delete
		return stmt:reset():bind(k):step()
	else
		local stmt = self.stmts.set
		return stmt:reset():bind(k, v):step()
	end
end

return Database
