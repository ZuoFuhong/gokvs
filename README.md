## gokvs

**gokvs** 是一个基于 **Raft** 共识算法且支持 Redis 协议的分布式可靠的 key/value 存储系统

## Running

启动 kvs-server

```
go run main.go
```

终端命令操作进入 `kvsctl` 目录编译工具，支持基本数据操作和集群管理：

```shell
go build -o kvsctl

# 读写命令
./kvsctl get name
./kvsctl get name -a 127.0.0.1:2317

# 集群命令
./kvsctl member add 127.0.0.1:2317 127.0.0.1:2318
./kvsctl member remove 127.0.0.1:2317
./kvsctl member list
```

集群服务器列表：

```shell
id=127.0.0.1:2317 address=127.0.0.1:2318 suffrage=0 isLeader=true
id=127.0.0.1:2327 address=127.0.0.1:2328 suffrage=0 isLeader=false
id=127.0.0.1:2337 address=127.0.0.1:2338 suffrage=0 isLeader=false
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

This project is licensed under the [MIT license](https://github.com/ZuoFuhong/gokvs/blob/master/LICENSE).