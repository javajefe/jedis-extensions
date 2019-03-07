Extensions for Jedis Java library
=====

[Jedis](https://github.com/xetorthio/jedis) is a small and simple client library for Redis DB.
However, some latest Redis APIs are not implemented (for example, Redis Streams commands).
  
This small library tries to implement some missing functions. The approach is very simple: every command is implemented as Lua script. BWT it would be obviously better to get native support (at least of stream commands) in Jedis.

### Release notes
##### v 0.0.2
- Now `SCRIPT FLUSH`, etc. does not makes consequent executions fail.

##### v 0.0.1
- Implemented batch `XADD` command.
- Implemented `XDEL` command.
- Implemented `XLEN` command.
- Implemented `XRANGE` command.
- Implemented `XREVRANGE` command.

### TODO
- Implement the rest of commands.
- Extend input parameters validation.
- API refactoring.
- Implement support of Sentinel and Cluster modes.