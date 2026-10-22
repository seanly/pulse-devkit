# 0018 kind 协议：原语、进度、续期、取消、blob

**里程碑** M5 · **依赖** 0002

## 目标

把信封从「一个 job 发出去、一个 result 回来」扩展到能承载长任务、大输出和自管理。定下原语族的命名与载荷 schema。

设计背景见 [../design.md §3](../design.md)。这个 issue 只产出 proto 与文档，**不含任何执行器实现** —— 后面六个 issue 各实现一族。

## 为什么先做这个

字段号发布后不能复用，而 0019–0024 全部依赖这里的形状。协议定错一次，六个 issue 一起返工。

## 四个缺口

现在的信封装不下的东西，每一个都有具体后果：

| 缺口 | 现状 | 后果 |
|---|---|---|
| 流式进度 | agent→server 只有 `Heartbeat` / `JobAck` | 五分钟的任务在跑完之前运维什么都看不到 |
| 租约续期 | 租约 = 2×超时 + 30s，超时上限 1h | 租约在任务还在跑时过期 → **同一个写操作派发两次** |
| 取消 | Command 只有 RELOAD / SHUTDOWN | 跑错了只能等它跑完 |
| 大内容 | gRPC 单消息默认 4MB | 一个 Ansible role 树塞不进 payload |

租约那条是唯一会**损坏数据**的，所以它优先。

## proto 增量

```protobuf
message AgentMessage {
  oneof payload {
    Heartbeat heartbeat = 1;
    JobAck job_ack = 2;
    JobProgress progress = 3;   // 新
    JobLease lease = 4;         // 新
  }
}

// JobProgress 是一个还在跑的 job 的增量输出。
message JobProgress {
  string job_id = 1;
  // seq 从 1 单调递增。服务端据此发现丢块与乱序 —— 一段缺了中间的日志
  // 比一段明显截断的日志更危险，因为它看起来是完整的。
  uint64 seq = 2;
  // stream 区分 stdout / stderr。两者分开传、分开存：合并之后再也分不开，
  // 而「这行是错误还是正常输出」常常是排查的第一个问题。
  string stream = 3;
  bytes chunk = 4;
}

// JobLease 说「我还在跑」。服务端顺延租约。
//
// 不复用 Heartbeat：心跳是 agent 级的，租约是 job 级的，而一个 agent 可能
// 同时跑着一个长任务和几个短任务。心跳停了说明 agent 死了；某个 job 不再
// 续租说明那个 job 死了，别的还活着。
message JobLease {
  string job_id = 1;
}

message Command {
  enum Kind {
    KIND_UNSPECIFIED = 0;
    KIND_RELOAD = 1;
    KIND_SHUTDOWN = 2;
    KIND_CANCEL = 3;            // 新
  }
  Kind kind = 1;
  // job_id 只对 KIND_CANCEL 有意义。
  string job_id = 2;
}

service AgentService {
  // ... 现有三个 ...

  // FetchBlob 按内容摘要拉取大内容。
  //
  // 内容寻址而不是把包塞进 job payload，有两个理由：payload 过不了 gRPC 的
  // 4MB 默认上限；而 1000 台 agent 拉的是同一个包，按 digest 缓存意味着第
  // 二台起本地命中。
  rpc FetchBlob(FetchBlobRequest) returns (stream BlobChunk);
}

message FetchBlobRequest {
  // digest 是 "sha256:<hex>"。agent 必须校验收到的内容确实哈希成这个值 ——
  // 内容寻址的全部安全性都在这一步。
  string digest = 1;
}

message BlobChunk {
  bytes chunk = 1;
}
```

## 原语的命名

`族.动作`，族是 agent 的能力边界，动作是幂等性的承诺：

| kind | 幂等 | issue |
|---|---|---|
| `exec.run` | 否（由被执行的程序决定） | [0019](0019-exec-run.md) |
| `fs.put` | **是** | [0020](0020-fs-primitives.md) |
| `fs.get` / `fs.stat` | 是（只读） | 0020 |
| `fs.remove` | 是 | 0020 |
| `net.tlsprobe` | 是（只读） | [0021](0021-net-primitives.md) |
| `agent.info` | 是（只读） | [0022](0022-agent-info.md) |
| `agent.upgrade` | 否 | [0024](0024-agent-upgrade.md) |

族名是本地策略的授权单位（issue 0023）：「这台 agent 只允许 `net.*`」要能一句话说清。

## 载荷用 JSON

和 certpulse 的选择一致：原语载荷是 Go 结构体 + JSON，不是 proto 消息。

理由是这些载荷会被人读。运维问「这次执行到底跑了什么命令、带了什么环境变量」时，载荷就是答案，它不该需要一个解码器。代价是体积，而原语载荷本来就小 —— 大内容走 blob。

## 验收标准

- [ ] `make generate` 产出新字段，提交进仓库
- [ ] 四个缺口的 proto 注释写清语义：`seq` 的丢块检测、`JobLease` 为什么不复用心跳、`digest` 必须被 agent 校验
- [ ] 原语载荷的 Go 结构体定义在 `pulse` 包（两端共用），带 JSON tag
- [ ] 旧 agent 收到不认识的 Command 落到 default 分支、打日志、继续跑（已有行为，加测试覆盖 KIND_CANCEL）
- [ ] 信封里仍然没有任何业务语义字段
