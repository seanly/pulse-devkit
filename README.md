# pulse-devkit

一个 **agent 机群控制面** 框架：服务端招募 agent、持有它们的长连接、按容量把工作租约派发下去；agent 端注册、保活、执行、回报。

框架不知道「工作」是什么。拨测一个证书、跑一次构建、在一批主机上执行一个运维动作，对 pulse-devkit 都是同一种东西：一个带超时和重试预算的 job，派给一个在线 agent，产出一个带状态的 result。

```go
// agent 侧：注册执行器，跑起来
agent, _ := pulseagent.New(pulseagent.Options{
    Config: pulseagent.RuntimeConfig{
        Name:           "runner-1",
        BootstrapToken: os.Getenv("PULSE_BOOTSTRAP_TOKEN"),
        CredentialFile: "/var/lib/myapp/credential.json",
        Limits:         pulseagent.Limits{Concurrency: 8, QPS: 20, Timeout: 30 * time.Second},
    },
    API:       api,
    Executors: pulseagent.Executors{"build": myBuildExecutor},
})
agent.Run(ctx)
```

```go
// 服务端：注册表 + 队列
reg, _ := pulsereg.New(pulsereg.Options{
    Agents: store, Tokens: store, Configs: store, MasterKey: masterKey,
})
queue, _ := pulsequeue.New(pulsequeue.Options{
    Jobs: store, Agents: store, Registry: reg,
})
reg.SetSink(mySink)          // 结果归应用
queue.Start(ctx)
pulsev1.RegisterAgentServiceServer(grpcServer, pulsereg.NewGRPCServer(reg))
```

## 它解决什么

写过一次 server/agent 的人都知道这些坑：

- 凭据怎么签发、怎么吊销，怎么让一次性 token 真的只用一次
- 断线重连怎么不让整个机群同时回扑服务端
- 重连时新流怎么顶替旧流，而旧流的清理不会误删新流
- job 怎么**绝不**被租两次（SELECT 后 UPDATE 在任何方言下都是竞态）
- 「agent 满了」和「job 失败了」怎么区分——混为一谈会凭空制造失败记录
- 配置下推怎么验证真的生效了，而不只是「发出去了」

这些都在里面，带测试。

## 边界

**框架负责**：agent 身份与生命周期、长连接、心跳与在线判定、job 租约与重试、配置下推、标签存储。

**框架不负责**：job 里装什么、result 怎么解释、该派给谁（给原语，不给策略）、业务表、HTTP/Web、告警、通知。

派发目标由应用在入队时决定并写进 job，框架从不改派。

## 包

| 包 | 用途 |
|---|---|
| [`pulse`](pulse/) | 核心类型与存储端口，不依赖任何同级包 |
| [`pb/pulse/v1`](pb/pulse/v1/) | 生成的 gRPC 代码，两端共用 |
| [`pulseagent`](pulseagent/) | agent 运行时：注册、重连、心跳、worker pool |
| [`pulseagent/pulseguard`](pulseagent/pulseguard/) | 可选：目标准入策略（deny CIDR / 域名） |
| [`pulsereg`](pulsereg/) | 控制面：注册、凭据、流注册表、gRPC 服务端 |
| [`pulsequeue`](pulsequeue/) | 租约派发、ack 处理、租约回收 |
| [`pulsestore/sqlstore`](pulsestore/sqlstore/) | 参考存储：postgres + sqlite |
| [`pulsestore/porttest`](pulsestore/porttest/) | 存储端口的契约测试套件 |
| [`pulsetest`](pulsetest/) | 内存 store、假 stream、可推进时钟 |
| [`pulsecodec`](pulsecodec/) | 类型化载荷（JSON / protobuf） |

## 接入存储

两条路：

**新项目**用 `sqlstore`。它不引入数据库驱动（应用自己注册），迁移以语句列表交出去（应用用自己的迁移工具跑），表名带 `pulse_` 前缀可配。

**老项目**写适配器实现 `pulse` 的四个端口，映射到现有表——不用迁数据，不用动外键。用 `porttest.Run` 验证适配器符合契约：

```go
func TestMyAdapter(t *testing.T) {
    porttest.Run(t, func(t *testing.T) porttest.Stores {
        db := setupTestDB(t)
        return porttest.Stores{Agents: db.AgentPort(), Jobs: db.JobPort(), /* ... */}
    })
}
```

## 类型化载荷

协议层的 job/result 载荷是不透明字节（见 [design.md §2](docs/design.md)）。类型安全在 Go 层补回来：

```go
codec := pulsecodec.JSON[BuildJob, BuildResult]()
exec := pulsecodec.Typed(codec, func(ctx context.Context, job pulse.Job, p BuildJob) (BuildResult, error) {
    return runBuild(ctx, p)      // 纯业务，没有编解码样板
})
```

解码失败会变成 `status=error`、`error_code=pulse_payload_decode`，执行器根本不会被调用——它对一个解不开的载荷无能为力。

## 状态

v0.x，API 还会变。测试覆盖 170+ 个用例，含 sqlite 与 postgres 的存储契约测试、race 检测、以及走真实 gRPC 的端到端测试。

实施计划与每个 issue 的设计说明见 [docs/issues/](docs/issues/)。

## 来历

从 [certpulse](https://github.com/seanly/certpulse)（证书到期监控）里抽出来的。那套 server/agent 已经在生产里解决了上面列的所有问题，pulse-devkit 是把与证书无关的部分拿出来、去掉包级全局、把载荷换成不透明字节的结果。

## 开发

```bash
make test       # 跑测试
make race       # 带 race 检测
make generate   # 重新生成 proto（需要 protoc）
```

跨模块开发（比如和 certpulse 一起改）用 `go.work`，不要在 `go.mod` 里写 `replace` —— replace 会被提交，下游 `go get` 到一个指向本地路径的模块就炸了。

```
work-dir/
├── certpulse/
│   └── go.work      # use . 和 ../pulse-devkit（不提交）
└── pulse-devkit/
```

go.work 由消费方持有，所以一个只用这个框架的项目不需要知道别人的目录怎么摆。`GOWORK=off` 走正常的模块解析，CI 和容器用那条路。

## License

MIT
