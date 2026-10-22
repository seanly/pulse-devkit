# pulse-devkit 实施计划

总体设计见 [../design.md](../design.md)。本目录每个文件是一个可独立交付的 issue，编号即建议实施顺序。

## 里程碑

| 里程碑 | 目标 | issues |
|---|---|---|
| **M0** 协议与骨架 | 模块能编译、协议与端口定下来 | 0001, 0002, 0003 |
| **M1** Agent 运行时 | agent 侧完整可用（**最小可用**） | 0004, 0005, 0006, 0007 |
| **M2** 控制面 | 注册、凭据、长连接跑通 | 0008, 0009, 0010 |
| **M3** 队列与存储 | 租约派发与参考存储 | 0011, 0012 |
| **M4** 配套与接入 | 测试工具、类型化载荷、certpulse 迁移 | 0013–0017 |

## Issue 列表

| # | 标题 | 依赖 | 状态 |
|---|---|---|---|
| [0001](0001-module-skeleton.md) | 模块骨架与 go.work 工作区 | — | ✅ 已完成 |
| [0002](0002-protocol-envelope.md) | 协议信封：载荷不透明的 Job/Result | 0001 | ✅ 已完成 |
| [0003](0003-core-types-ports.md) | 核心类型与存储端口 | 0001 | ✅ 已完成 |
| [0004](0004-agent-lifecycle.md) | Agent 生命周期：注册、重连、心跳 | 0002, 0003 | ✅ 已完成 |
| [0005](0005-agent-runner.md) | Worker pool：并发、QPS、超时、drain | 0004 | ✅ 已完成 |
| [0006](0006-agent-config-push.md) | 配置下推与生效回显 | 0004 | ✅ 已完成 |
| [0007](0007-pulseguard.md) | 可选的目标准入策略 | 0005 | ✅ 已完成 |
| [0008](0008-credentials.md) | 凭据签名与 bootstrap token | 0003 | ✅ 已完成 |
| [0009](0009-registration.md) | 注册服务与标签合并 | 0008 | ✅ 已完成 |
| [0010](0010-stream-registry.md) | 流注册表、心跳与 gRPC 服务端 | 0009 | ✅ 已完成 |
| [0011](0011-lease-dispatch.md) | 租约派发、ack 与租约回收 | 0003, 0010 | ✅ 已完成 |
| [0012](0012-sqlstore.md) | 参考存储实现（postgres + sqlite） | 0003 | ✅ 已完成 |
| [0013](0013-pulsetest.md) | 测试工具：内存 store 与假 stream | 0003 | ✅ 已完成 |
| [0014](0014-pulsecodec.md) | 类型化载荷 codec | 0002 | ✅ 已完成 |
| [0015](0015-certpulse-migration.md) | certpulse 迁移到 pulse-devkit | 0001–0014 | 🚧 进行中 |
| [0016](0016-payload-resolver.md) | 派发时解析载荷 | 0011 | ✅ 已完成 |
| [0017](0017-clock-free-contract.md) | 契约套件不再需要可控时钟 | 0012, 0013 | ✅ 已完成 |

## 约定

- 每个 issue 自带测试，不依赖外部服务跑 CI（postgres 用例靠 `PULSE_TEST_POSTGRES` 开关，缺省 skip）。
- 框架代码不得 import 任何业务语义的包；`pulse` 包不得依赖同级包。
- 生成的 proto 代码提交进仓库，构建不依赖贡献者装了 protoc。
- 不写包级可变全局；日志、版本、metrics、时钟一律注入。
- proto 字段号只增不改不复用。
- 跨模块开发用根目录 `go.work`，`go.mod` 里不写 `replace`。
