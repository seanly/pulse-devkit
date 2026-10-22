# 0022 agent.info：主机 facts

**里程碑** M5 · **依赖** 0018

## 目标

让服务端知道它在管什么机器，而且是**机器自己说的**，不是运维在配置文件里写的。

## 为什么标签不够

现在的标签来自 agent 的 toml，运维手工维护。1000 台手工维护的标签**必然**和现实脱节 —— 有人装了新版本忘了改标签，有人复制了配置文件连带复制了别人的 region。

而「在所有 RHEL 8 主机上更新 cacert」这种目标选择，错一台的后果是在一台不该动的机器上执行了写操作。

facts 是机器自报的，所以它不会和现实脱节。标签仍然有用 —— 它表达的是**运维的意图**（这台属于哪个业务、哪个环境），那是机器不知道的。两者互补：

| 标签 | facts |
|---|---|
| 人赋予的意图：`env=prod`、`team=payments` | 机器的事实：OS、版本、架构、已装软件 |
| 机器不知道 | 人维护不准 |
| 服务端可覆盖（有墓碑机制） | 只能由 agent 上报 |

## 载荷

```go
// AgentFacts 是 agent 对自己所在主机的观察。
//
// 全部是「能从本机读出来的客观事实」。任何需要判断的东西（这台算不算
// 生产、该不该升级）都不在这里 —— 那是标签和服务端策略的事。
type AgentFacts struct {
    Hostname string `json:"hostname"`
    // OS/Distro/Version: "linux" / "rhel" / "8.9"
    OS      string `json:"os"`
    Distro  string `json:"distro,omitempty"`
    Version string `json:"version,omitempty"`
    Arch    string `json:"arch"`
    Kernel  string `json:"kernel,omitempty"`

    // Addresses 是本机的地址，用于「这台在哪个网段」这类选择。
    Addresses []string `json:"addresses,omitempty"`

    // PackageManager: "apk" / "apt" / "yum" / "dnf"，空表示没认出来。
    PackageManager string `json:"package_manager,omitempty"`

    // Binaries 是服务端问起的可执行文件在不在、版本是什么。
    //
    // 服务端问而不是 agent 全盘上报：一个「列出所有已装软件」的 facts 既
    // 昂贵又是信息泄露，而服务端真正需要知道的只有几个 —— 要派 Ansible
    // 任务之前，它想知道哪些 agent 有 ansible-playbook。
    Binaries map[string]BinaryFact `json:"binaries,omitempty"`

    // Disk 是服务端问起的挂载点的可用空间。exec.run 的工作区放得下吗。
    Disk map[string]DiskFact `json:"disk,omitempty"`

    CollectedAt time.Time `json:"collected_at"`
}

type BinaryFact struct {
    Path    string `json:"path,omitempty"`     // 空表示没找到
    Version string `json:"version,omitempty"`
}

type DiskFact struct {
    TotalBytes uint64 `json:"total_bytes"`
    FreeBytes  uint64 `json:"free_bytes"`
}
```

## 服务端问，agent 不全盘上报

`Binaries` 和 `Disk` 是**查询参数**，不是固定清单。载荷里带「我想知道这些」：

```json
{ "binaries": ["ansible-playbook", "python3"], "mounts": ["/", "/var/lib/certpulse"] }
```

理由有两条。一是成本：枚举所有已装包在一台机器上要几秒，1000 台周期性做这件事没有意义。二是**信息泄露**：一份完整的软件清单是攻击者的侦察成果，服务端数据库里存 1000 台的完整清单，泄露的价值比存几个二进制的版本高得多。

探测二进制的方式：`exec.LookPath` + 跑 `--version`。后者要受本地策略的白名单约束（issue 0023）—— 否则 `agent.info` 就成了一个绕过白名单执行任意程序的后门。

## 什么时候收集

- 注册时一次（服务端立刻知道新 agent 是什么）
- 服务端按需派发 `agent.info`
- **不**周期性自动上报：facts 变化不频繁，而 1000 台周期上报是又一条每秒几十次的写入

## 存哪

框架提供端口，不选存储形状：

```go
// FactsStore 持久化 agent 自报的事实。
type FactsStore interface {
    SaveFacts(ctx context.Context, agentID string, raw []byte) error
    GetFacts(ctx context.Context, agentID string) ([]byte, error)
    // ListFacts 给动态 inventory 用：一次读全机群。
    ListFacts(ctx context.Context) (map[string][]byte, error)
}
```

`raw []byte` 而不是结构体：facts 的内容是应用定义的（certpulse 关心包管理器，CI 系统关心 Docker 版本），框架只负责搬运和存取。这和载荷不透明是同一条原则。

## 验收标准

- [x] 注册时带上一份 facts
- [x] 服务端可按需派发 `agent.info`，带 binaries/mounts 查询参数
- [x] 二进制探测受本地白名单约束 —— 不在白名单的程序不执行 `--version`，有测试
- [x] 不做周期性自动上报
- [x] `FactsStore` 端口 + 参考实现 + 契约测试
- [x] facts 里没有任何判定字段
