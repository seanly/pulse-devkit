# 0015 certpulse 迁移到 pulse-devkit

**里程碑** M4 · **依赖** 0001–0014

## 目标

certpulse 改为消费 pulse-devkit，删掉重复代码。这是验证框架「真的够用」的唯一方式——没有第二个消费者的框架只是一堆猜测。

## 前置条件

**pulse-devkit 必须先上传到 github.com/seanly/pulse-devkit**。在那之前：

- 本机开发靠父目录的 `go.work`，能编译能测试
- `docker build` 会失败（构建上下文里没有 pulse-devkit 源码）
- CI 会失败（拉不到模块）

所以这个 issue 分两步：先在工作区里改通（本地可验证），等模块发布后再打开容器与 CI。

## 不迁数据库表

certpulse 已有 `agents`、`agent_bootstrap_tokens`、`agent_configs`、`probe_jobs`，并且有外键指向它们（`probe_results.job_id`、`probe_jobs.target_id` 等）。

**做法：在 `internal/store` 里写适配器实现 pulse-devkit 的四个端口，映射到现有列。** 不建新表，不迁数据，外键全部保持。

```go
// internal/store/pulseport.go
// AgentPort 让 pulse-devkit 的 pulsereg 读写 certpulse 既有的 agents 表。
func (db *DB) AgentPort() pulse.AgentStore { ... }
func (db *DB) JobPort() pulse.JobStore { ... }
```

需要补的列很少：`pulse_jobs` 的 `kind`（certpulse 全部填 `tlsprobe`）和 `requeue_count`（已有）。一次加列迁移，不动数据。

## 一次性的凭据失效

pulse-devkit 自己签凭据（HMAC，不是 JWT，见 issue 0008），格式与 certpulse 现有凭据不兼容。

影响：升级后所有 agent 的凭据校验失败 → 返回 `Unauthenticated` → **有 bootstrap_token 的 agent 自动重新注册**，无需人工干预。这正是 certpulse 现有的凭据吊销恢复路径，已经有测试覆盖。

没有 bootstrap_token 的 agent 需要人工重新入网。升级说明里要写明这点。

## proto 契约替换

certpulse 的 `certpulse.agent.v1` 整个被 `pulse.v1` 取代，业务字段进 payload。certpulse 新建一个只有载荷消息的 proto：

```protobuf
// proto/certpulse/probe/v1/payload.proto
message ProbeJobPayload {
  string fqdn = 1;
  int32  port = 2;
  string sni = 3;
}
message ProbeResultPayload {
  repeated string resolved_ips = 1;
  int32  days_remaining = 2;
  int64  not_after_unix = 3;
  bool   chain_trusted = 4;
  string cert_json = 5;
  // ... 其余原 ReportResultRequest 的业务字段
}
```

**这打破了「两端可以差一个版本」的承诺**——server 和 agent 必须同时发布一次。这是整个迁移里唯一的硬断点，发布说明要写明。

## 分步改造顺序

每一步都保持 `make test` 绿：

1. 加 pulse-devkit 依赖，改 Makefile 的 `GOWORK` 处理（issue 0001）
2. 写 store 适配器 + 端口契约测试（用 pulse-devkit 的共享 testsuite）
3. agent 侧：`internal/prober` 改为实现 `pulse.Executor`，`cmd/certpulse-agent` 换成 `pulseagent.New`
4. 服务端：`internal/agentreg` 换成 `pulsereg`，保留 certpulse 自己的 HTTP 层
5. 调度：`internal/scheduler` 切成「`pulsequeue` + certpulse 的 task/规则/告警」
6. 删掉被取代的代码，`go mod tidy`
7. 容器与 CI（等模块发布）

## 预期删掉的代码

| certpulse 文件 | 去向 |
|---|---|
| `internal/agent/{agent,api,runner,backoff,credential,guard,metrics}.go` | 删，用 `pulseagent` |
| `internal/agentreg/{agentreg,bootstrap,credentials,grpc}.go` | 删，用 `pulsereg` |
| `internal/scheduler/scheduler.go` 的派发/回收部分 | 删，用 `pulsequeue` |
| `proto/certpulse/agent/v1/agent.proto` | 替换为载荷 proto |

保留：`prober`、`pkg/probe`、`acmeissue`、`dnsprovider`、`domainsync`、`certlib`、`ruleengine`、`alerting`、`notify`、`server`（HTTP）、`tasks.go` 的目标展开。

## 验收标准

- [ ] `make test` 与 `make race` 全绿
- [ ] 端口适配器通过 pulse-devkit 的共享契约测试
- [ ] agent 注册、心跳、派发、回报端到端跑通（现有 integration 测试改造后仍绿）
- [ ] 配置下推与生效回显仍工作
- [ ] 删掉的代码确实删了，不是留着不用
- [ ] 升级说明写明凭据失效与两端同版本发布
- [ ] （模块发布后）容器构建与 CI 通过
