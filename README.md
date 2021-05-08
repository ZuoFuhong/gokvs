## gokvs

`gokvs` 是一个基于redis协议且支持持久化的key/value存储系统

## Running

该项目的`bin`目录下提供了服务端 和 客户端执行文件

终端模式下进入`bin/server`目录，启动服务端：

```
go run kvs-server.go
```

终端模式下进入`bin/client`目录，使用客户端：

```
go run kvs-cli.go set name mars
go run kvs-cli.go get name
```

## Supported commands

目前, `gokvs`支持下面这些命令：

- [GET](https://redis.io/commands/get)
  ```
  set name mars
  ```
- [SET](https://redis.io/commands/set)
  ```
  get name
  ```
- [DEL](https://redis.io/commands/del)
  ```
  del name
  ```

## Reference

- [TP 201: Practical Networked Applications](https://github.com/pingcap/talent-plan/blob/master/courses/rust/docs/lesson-plan.md)
- [Redis Protocol specification](https://redis.io/topics/protocol)
- [tokio-rs/mini-redis](https://github.com/tokio-rs/mini-redis)

## License

This project is licensed under the MIT license.