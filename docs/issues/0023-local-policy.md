# 0023 本地策略：服务端推不了的那条边界

**里程碑** M5 · **依赖** 0019, 0020, 0021, 0022

## 目标

限制「服务端被攻破」的爆炸半径。这是整个原语模型能不能上生产的前提，其它六个 issue 都依赖它。

## 先把代价说清

把 agent 变成通用原语机，意味着：

```
服务端被攻破 = 1000 台主机 root
```

这是设计的**内在属性**，不是实现缺陷。`exec.run` 的存在就意味着服务端能让 agent 执行程序；给 agent 发任意程序的能力和给它 root 后门的能力，差别只在于约束。

所以这个 issue 不是「加几个开关」，它是这套能力的安全模型本身。

## 唯一有效的机制

服务端不可信时，唯一还能依赖的是**agent 自己持有、服务端改不了的策略**。

这正是 `pulseguard` 已经建立的模式。它的文档里写着：

> 当服务端规则配错时，这是最后一道防线。一个内网 agent 尤其需要 —— 一条坏规则不该让它变成自己子网的扫描器。
>
> deny list 只增不减：下推的条目**加到**本地的上面，本地条目永不被移除。一次下推能放宽它的话，这层保护就成了装饰。

把同一条原则扩展到所有原语：

| 本地配置（服务端不可改） | 约束什么 |
|---|---|
| `allowed_kinds` | 这台 agent 允许执行哪些 kind |
| `exec.allowed_bins` | `exec.run` 的 `Argv[0]` 白名单，**绝对路径** |
| `exec.workspace_root` | 工作区落在哪（建议 tmpfs） |
| `fs.allowed_paths` | `fs.*` 允许读写的路径前缀 |
| `net.allowed_resolvers` | `net.*` 允许指定的 DNS 服务器 |
| `run_as` | 以哪个用户执行（默认不是 root） |
| 以及 `pulseguard` 现有的 deny list | 向外连接的目标 |

一致的故事：**本地是底线，下推只能收紧**。不是七个互不相关的开关，是同一条原则的七个作用面。

## allowed_kinds 是最重要的那一条

它让「agent 二进制通用，部署姿态不通用」成为可能：

```toml
# 1000 台拨测节点：只会探测，9MB 镜像，非 root
[policy]
allowed_kinds = ["net.*", "agent.info"]

# 少数运维节点：能执行，装了 Ansible，可能需要提权
[policy]
allowed_kinds = ["net.*", "agent.*", "fs.*", "exec.run"]
run_as = "root"
[policy.exec]
allowed_bins = ["/usr/bin/ansible-playbook"]
[policy.fs]
allowed_paths = ["/etc/ssl/", "/usr/local/share/ca-certificates/"]
```

拨测节点上的 `exec.run` **根本不存在** —— 不是被拒绝，是那个 executor 没注册。服务端派过来只会得到 `unsupported_kind`。

这意味着一个被攻破的服务端能对 950 台拨测节点做的，和现在一样多：让它们去探测不该探测的地址，而 deny list 还在挡着。它拿不到 root，因为那些 agent 没有执行能力。

## exec.allowed_bins 里绝不能有 shell

允许 `/bin/sh` 或 `/bin/bash` 等于允许任意命令，白名单当场失效。

这条约束和 `exec.run` 的「`Argv` 不经 shell」是同一个决定的两半：协议层不提供放 shell 的地方，策略层不允许把 shell 当作可执行文件。缺任何一半，另一半都没用。

实现上要显式拒绝并说明原因，而不是让它静默通过 —— 一个把 `/bin/sh` 写进白名单的运维，需要知道为什么这样不行。

同理 `agent.info` 探测二进制版本时跑的 `--version`，也要走同一个白名单。否则它就是一个绕过白名单执行任意程序的后门。

## 配置缺省必须是最严格的

一个没有 `[policy]` 段的 agent：

- `allowed_kinds` 默认 `["net.*", "agent.info"]` —— 只读能力
- `exec.allowed_bins` 默认**空**，也就是 `exec.run` 什么都不能跑
- `fs.allowed_paths` 默认**空**
- `run_as` 默认当前用户，不提权

理由：升级一个现有 agent 不该意外给它新能力。1000 台拨测节点升到新版本之后，它们的能力应该和升级前一样 —— 新能力必须是有人明确打开的。

## 拒绝的回报

被本地策略拒绝的 job 回报 `AckRejected`（框架已有），并在 `error_detail` 里说明是哪条策略拒绝的。

这里有个已知的信息损失（记在 certpulse 的 0043 里）：框架的 ack 把具体原因归一成 `rejected`。这个 issue 是补上它的好时机 —— 在 `JobAck` 上加一个 `detail` 字段，`reason` 保持为派发器分支用的稳定代码。运维需要知道「被拒了」和「因为哪条规则被拒」。

## 验收标准

- [ ] 七个约束面各有测试，含「策略拒绝 → AckRejected + 可读原因」
- [ ] `allowed_kinds` 不匹配时 executor 根本不注册，回报 `unsupported_kind`
- [ ] `/bin/sh`、`/bin/bash` 等写进 `allowed_bins` 被显式拒绝并说明原因
- [ ] `agent.info` 的二进制探测走同一个白名单
- [ ] 空配置的默认值是只读能力，有测试
- [ ] 没有任何一条策略可以被服务端下推放宽，有测试（推一个更宽的配置，确认无效）
- [ ] `JobAck` 加 `detail`，补上 0043 记的那处信息损失
