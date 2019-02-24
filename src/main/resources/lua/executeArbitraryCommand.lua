-- Lua 5.1 http://www.lua.org/manual/5.1/manual.html
-- The scipt executes arbitrary Redis command.

-- ARGV should be a full command terms list.
return redis.call(unpack(ARGV))