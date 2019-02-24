 Extensions for Jedis Java library
=====

[Jedis](https://github.com/xetorthio/jedis) is a small and simple client library for Redis DB.
However, some latest Redis APIs are not implemented (for example, Redis Streams commands).
  
This small library tries to implement some missing functions.

### Release notes
##### v 0.0.1
- Implemented batch `XADD` command.
- Implemented `XDEL` command.
- Implemented `XLEN` command.
- Implemented `XRANGE` command.
- Implemented `XREVRANGE` command.
