# 0002 协议信封：载荷不透明的 Job/Result

**里程碑** M0 · **依赖** 0001

## 目标

定义 `pulse/v1` 的 gRPC 契约。这是整个框架最该一次定对的东西——字段号发布后不能复用，而两端允许差一个版本运行。

## 范围

- `proto/pulse/v1/pulse.proto`
- 生成到 `pb/pulse/v1/`（公开路径，不是 internal）
- `Makefile generate`：用 protoc（buf 可选，不作为硬依赖）

## 与 certpulse 协议的差别

certpulse 的 `ProbeJob` 带 `fqdn/port/sni`，`ReportResultRequest` 带 `days_remaining/not_after_unix/chain_trusted/cert_json`。这些全部移出信封，进 `bytes payload`。

| certpulse | pulse-devkit |
|---|---|
| `task_id` | `group_id` |
| `target_id` | `subject_id` |
| `fqdn` / `port` / `sni` | → `payload` |
| `days_remaining` / `not_after_unix` / `chain_trusted` | → `payload` |
| `resolved_ips` / `resolver` / `observed_ip` | → `payload` |
| `per_ip` / `cert_json` / `tls_json` / `timing_json` | → `payload` |
| — | `kind`（新增，见下） |

保留在信封里的是框架真正要读的：`job_id`、`group_id`、`subject_id`、`timeout_ms`、`attempt`、`status`、`error_code`、`error_detail`、`executed_at_unix`。

## kind：一个 agent 跑多种工作

新增 `Job.kind`，agent 据此选 `Executor`。certpulse 只有一种（`tlsprobe`），但 CI 场景下同一个 agent 既要跑构建又要跑部署，没有 kind 就得为每种工作部署一套 agent。

`Register` 带 `repeated string capabilities`，声明这个 agent 能处理哪些 kind。服务端可以据此避免把 `kind=build` 派给只会 `kind=tlsprobe` 的 agent。框架存下来并提供查询，**不**自动过滤——派发目标是应用在入队时决定的。

## AgentConfig 的通用性

certpulse 的 `AgentConfig` 里有 `deny_cidrs` / `deny_domains`。这对「外拨探测」通用，对 CI 不通用。

信封保留真正通用的：`version`、`concurrency`、`qps`、`timeout_ms`、`log_level`、`payload`。deny list 进 `payload`，由 `pulseguard`（issue 0007）解码。

`LocalConfig` 同理：通用字段 + `map<string,string> extra`。certpulse 把 `allow_loopback`、`deny_cidr_count` 塞进 extra，控制台照样能展示。

## 保留 certpulse 的两条语义

这两条是 certpulse 用生产经验换来的，原样保留并写进 proto 注释：

1. **AgentConfig 的未设置字段 = 「服务端无意见，用本地值」**。所以推一个空 config 是安全的 no-op，清除一个 override 也不需要重连。
2. **`ReportResult` 按 job_id 幂等**。服务端保留第一个结果，忽略重放；`accepted=false` 时 agent 不重试。

## 验收标准

- [x] `make generate` 产出 `pb/pulse/v1/*.pb.go`，提交进仓库
- [x] proto 注释写清每个 oneof 的向后兼容行为（旧 agent 收到不认识的 payload 落到 default 分支、打日志、继续跑）
- [x] 信封里没有任何业务语义字段
- [x] `go build ./pb/...` 通过
