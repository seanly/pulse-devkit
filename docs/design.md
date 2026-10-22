# pulse-devkit 总体设计

一个 **agent 机群控制面** 框架：服务端招募 agent、持有它们的长连接、按容量把工作租约派发下去；agent 端注册、保活、执行、回报。

框架不知道「工作」是什么。拨测一个证书、跑一次构建、在一批主机上执行一个运维动作，对 pulse-devkit 都是同一种东西：一个带超时和重试预算的 job，派给一个在线 agent，产出一个带状态的 result。

## 1. 来历与边界

这套代码从 [certpulse](https://github.com/seanly/certpulse) 里长出来。certpulse 的 server/agent 已经解决了一堆与证书无关的问题：

- 一次性 bootstrap token 换长期凭据，凭据可吊销
- 断线重连的指数退避与抖动，避免整个机群同时回扑服务端
- 单 agent 单流的注册表，重连时新流顶替旧流
- 按 agent 剩余容量租约派发，租约过期自动回收
- 配置下推带版本号，agent 心跳回显「已生效版本」

这些是 pulse-devkit 的全部内容。证书、DNS、ACME、告警规则留在 certpulse。

**框架负责**：agent 身份与生命周期、长连接、心跳与在线判定、job 租约与重试、配置下推、标签选择。

**框架不负责**：job 里装什么、result 怎么解释、怎么决定该派给谁（给选择器，不给策略）、业务表、HTTP/Web、告警、通知。

## 2. 核心取舍：载荷不透明

这是整个框架唯一的关键设计决定。

certpulse 的 proto 把 `fqdn/port/sni` 和 `days_remaining/not_after/chain_trusted` 写在了 Job/Result 里。这对证书拨测很顺手，但 CI 构建没有 `sni`，运维任务没有 `days_remaining`。

pulse-devkit 的 proto 只定义**信封**，业务载荷是 `bytes`：

```protobuf
message Job {
  string job_id = 1;
  string group_id = 2;     // 一批 job 的归属（certpulse 的 task，CI 的 pipeline）
  string subject_id = 3;   // job 作用的对象（certpulse 的 target，CI 的 repo）
  string kind = 4;         // 工作类型，agent 据此选 executor
  int32  timeout_ms = 5;
  int32  attempt = 6;
  bytes  payload = 7;      // 应用自己编解码
}
```

框架搬运 `payload` 但从不解读它。类型安全在 Go 层用 [`pulsecodec`](../pulsecodec) 补回来，不污染 proto。

**代价**：框架的日志和指标说不出「正在拨测 example.com」，只能说 `job_id=... kind=tlsprobe subject_id=...`。应用想要可读的日志，自己在 `payload` 解码后打。这是刻意的——让框架认识业务字段，就是把 certpulse 的形状永久焊进框架。

**为什么不用 `google.protobuf.Any`**：Any 要求载荷类型注册进 proto 描述符表，等于把 descriptor registry 拖进每个 agent 二进制，而且强制应用用 protobuf 编码。`bytes` 让应用自选 protobuf / JSON / CBOR。

## 3. 模块划分

```
pulse/          核心类型与端口：Job、Result、Agent、AgentConfig、Store 接口
pb/pulse/v1/    生成的 proto 代码（公开，两端都 import）
pulseagent/     agent 运行时：注册、重连、心跳、worker pool、drain
  pulseguard/   可选的目标准入策略（deny CIDR / 域名）
pulsereg/       控制面：注册服务、凭据签名、流注册表、离线 reaper
pulsequeue/     租约派发：按容量 lease、ack 处理、租约回收、重试预算
pulsestore/     端口的参考实现
  sqlstore/     database/sql 实现，postgres + sqlite，自带 embed 迁移
pulsetest/      内存 store 与假 stream，给下游写测试用
pulsecodec/     类型化载荷：JSON 与 protobuf codec
```

依赖方向严格单向，`pulse` 不依赖任何同级包：

```
pulseagent ─┐
pulsereg   ─┼─→ pulse ←─ pulsestore/sqlstore
pulsequeue ─┘      ↑
                 pb (pulse 与各包共用)
```

## 4. 存储：端口 + 参考实现

`pulsereg` 和 `pulsequeue` 不碰 `database/sql`，只认 `pulse` 里的接口：

```go
type AgentStore interface { ... }   // agent 行、心跳、离线扫描
type TokenStore interface { ... }   // bootstrap token 签发与claim
type ConfigStore interface { ... }  // 下推配置的存取
type JobStore interface { ... }     // 入队、租约、完成、失败、回收
```

端口拆成四个而不是一个大 `Store`：`pulsereg` 不需要 job 表，`pulsequeue` 不需要 token 表。按需传入，下游也可以只接一半。

**为什么同时给参考实现**：租约那段 SQL 是整个框架里最难写对的部分——postgres 要 `FOR UPDATE SKIP LOCKED`，sqlite 靠单写者语义但必须压成单条语句（否则 SELECT 后 UPDATE 在任何方言下都是竞态）。只给接口等于把最硬的部分推给每个下游重写一遍。

`sqlstore` 的表名带前缀（默认 `pulse_`），迁移以 `embed.FS` 暴露，应用用自己的迁移工具（goose/atlas/手写都行）执行。框架不选迁移工具。

**老项目怎么接**：certpulse 已有 `agents` / `probe_jobs` 表和一堆外键指向它们。它不迁表，而是在自己的 `store` 包里写一个适配器实现这四个端口，映射到现有列。参考实现是给新项目的。这条路径让「接入框架」不需要数据迁移。

## 5. agent 侧

主循环与 certpulse 的一致（那个形状已经被生产验证过）：

```
读 credential_file
  ├─ 有 → Connect
  └─ 无 → Register(bootstrap_token) → 落盘 0600 → Connect
Connect
  ├─ 心跳 15s 一次，回显已生效的 config version
  ├─ 接收循环：job → 投进 worker pool（绝不阻塞）；command；config
  └─ 断线 → 指数退避 1s→60s，±20% 抖动
凭据被拒 → 若有 bootstrap_token 则重新注册，否则退出并说明原因
```

与 certpulse 的差别：

- `Executor` 取代 `Prober`，按 `job.Kind` 分发，一个 agent 可以同时跑多种工作
- 日志级别、版本号、metrics 全部注入，不再是包级全局（见 §7）
- `config.LoadAgent` 的依赖换成 `ConfigProvider` 回调，TOML 解析留在应用侧
- deny list 移到可选的 `pulseguard`，通过 AgentConfig 的 `payload` 下推

### Executor

```go
type Executor interface {
    // Execute 跑一个 job。ctx 的 deadline 已经是本地截断后的有效超时。
    Execute(ctx context.Context, job pulse.Job) (pulse.Result, error)
}
```

返回 `error` 而不是只返回 Result：框架把 error 翻成 `status=error` 加一个错误码，省掉每个应用都写一遍的样板。应用想完全控制状态（比如 `skipped`），返回 Result 且 error 为 nil。

## 6. 服务端侧

`pulsereg` 负责身份与连接，`pulsequeue` 负责工作流转，两者通过 `pulse.Registry` 接口解耦——`pulsequeue` 只需要「把这条消息发给这个 agent」。

inbound 方向给应用一个 `Sink`：

```go
type Sink interface {
    // Deliver 收下一个 result。accepted=false 表示服务端忽略了它
    // （未知 job 或重复上报），agent 不该重试。
    Deliver(ctx context.Context, agentID string, r pulse.Result) (accepted bool, err error)
}
```

certpulse 的 `agentreg.Dispatcher` 已经是这个形状，所以这部分是改名不是重写。

**派发不含策略**：`pulsequeue` 只回答「这个 agent 还能吃几个 job，给它」。「这个 subject 该给哪些 agent」是应用的规则引擎（certpulse 的 `ruleengine`），它在入队时就把 `agent_id` 写进 job 行。框架提供标签选择的原语（`pulse.LabelSelector`），不提供规则语言。

## 7. 去全局化

certpulse 里有三处包级全局，库化必须改掉：

| certpulse | pulse-devkit |
|---|---|
| `logging.SetLevel(lvl)` | `Options.LevelSetter` 接口，nil 则级别下推是 no-op |
| `version.Version` | `Options.Version string` |
| `agent.Metrics` 固定结构 + 自带 HTTP | `pulse.Metrics` 接口，暴露方式归应用 |

心跳间隔这类两端必须一致的常量放进 `pulse`，两边 import 同一个包——这正好消掉 certpulse 里 `runner.go` 和 `agentreg.go` 各写一份心跳常量的重复。

## 8. 兼容性承诺

- proto 字段号永不复用，只增不改。server 与 agent 允许差一个小版本。
- `pulse` 的接口在 v1 内只增方法不改签名（加方法也是破坏性的，所以宁可新开接口）。
- `sqlstore` 的迁移只向前，不写 down 到丢数据的方向。

v0.x 阶段不做这些承诺，certpulse 接入过程中该改就改。

## 9. 不做的事

- **不做服务发现**：agent 配置里写服务端地址，够了。
- **不做 job 的 DAG / 依赖编排**：那是上层的事。框架只管「一个 job 派出去、跑完、回来」。
- **不做结果存储的 schema**：result 的 `payload` 怎么存是应用的决定。
- **不做 Web / HTTP API**：`pulsereg` 和 `pulsequeue` 暴露 Go 方法，HTTP 壳子归应用。
- **不做多租户**：没有 tenant 概念。需要的话在应用层按 label 切。
