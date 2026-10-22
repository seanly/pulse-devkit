# 0020 fs.*：不需要子进程的文件操作

**里程碑** M5 · **依赖** 0018

## 目标

原子、幂等地管理目标主机上的文件。**不起子进程、不需要目标主机有任何工具** —— 一个 9MB 的静态二进制自己就能做完。

## 为什么它比 exec.run 重要

最常见的运维操作是改一个文件：更新 CA 信任库、改一行配置、放一个证书。

`exec.run` 能做到，但那意味着目标主机上要有 Python 或者至少一套 coreutils，而且要起一个子进程、拼一条命令、处理它的退出码。`fs.put` 把这件事变成 agent 内部的几十行代码，没有外部依赖，而且**可以真正幂等**。

certpulse 的 cacert 场景（[0045](https://github.com/seanly/certpulse/blob/main/docs/issues/0045-cacert-update.md)）就是这个原语，不需要 Ansible。

## 载荷

```go
// FSPut 原子、幂等地写一个持久文件。
type FSPut struct {
    Path string `json:"path"`
    // Content 和 Blob 二选一。大文件（CA bundle 几百 KB 起）走 Blob。
    Content string `json:"content,omitempty"`
    Blob    string `json:"blob,omitempty"`   // digest

    Mode  string `json:"mode,omitempty"`     // 八进制，空表示保留现有或 0644
    Owner string `json:"owner,omitempty"`
    Group string `json:"group,omitempty"`

    // Backup 为真时保留被替换的内容，供回滚使用。
    Backup bool `json:"backup,omitempty"`

    // ConfirmWithin 是死人开关：写入后若在这段时间内 agent 没有成功连上
    // 服务端一次，自动恢复备份。0 表示不启用。
    //
    // 已发布的 JSON 名是 confirm_within_ns（time.Duration），不再改名。
    // 这是唯一只有 agent 能实现的安全机制，见下。
    ConfirmWithin time.Duration `json:"confirm_within_ns,omitempty"`
}

// FSPutResult 回报的载荷。
type FSPutResult struct {
    // Changed 为假表示内容已经是目标状态，什么都没做。
    Changed bool   `json:"changed"`
    OldSHA256 string `json:"old_sha256,omitempty"`
    NewSHA256 string `json:"new_sha256"`
    // BackupPath 是旧内容的落点，用于服务端发起回滚。
    BackupPath string `json:"backup_path,omitempty"`
}
```

`fs.get` / `fs.stat` / `fs.remove` 的载荷是显然的，不在这里展开。`fs.get` 要有大小上限 —— 一个 `fs.get /var/log/messages` 不该把服务端打垮。

## 幂等的形式

先算目标内容的校验和，和现有文件比：相同就什么都不做，回报 `changed=false`。

这正好是 Ansible 模块的语义，而且顺手给出了「这次执行改了什么」的报告 —— 1000 台里有 3 台的 bundle 不一致，`changed=true` 的那三行就是答案。

原子性：写临时文件 → `fsync` → `chmod`/`chown` → `rename`。中途崩溃不会留下半个文件。

## 死人开关

这是整个原语集合里唯一**只有 agent 能实现**的机制，所以值得讲清为什么。

换 CA 信任库时，如果新 bundle 不包含签发服务端证书的那个 CA：

```
agent 写入新 bundle
  → 下次连接服务端：证书验证失败
  → agent 再也连不上来
  → 1000 台同时失联
  → 修复手段：逐台 SSH
```

服务端**无法补救**这种情况，因为它恰好失去了通道。它看到的只是「agent 掉线了」，和网络故障一模一样。

所以 `ConfirmWithin`（JSON `confirm_within_ns`）：

```
写入 → 启动计时器 → 正常连上服务端并心跳成功一次 → 清除计时器
                  └─ 超时未连上 → 恢复备份 → 重连
```

这个机制不依赖服务端的任何配合，这是关键 —— 它在服务端已经联系不上的那个场景里工作。

**它不是万能的。** 如果新内容让 agent 进程本身起不来（不适用于 fs.put，但适用于 `agent.upgrade`，见 [0024](0024-agent-upgrade.md)），计时器也不会被检查。fs.put 这边安全，因为写文件不影响 agent 进程自身的运行。

验证「新内容是否正确」不在这个原语里 —— 那是服务端编排的事：`fs.put` → `exec.run`（验证）→ 失败则 `fs.put`（恢复）。原语保持单一职责，而死人开关只兜「服务端联系不上了」这一种情况。

## 验收标准

- [x] 内容相同 → `changed=false`，文件的 mtime 不变，有测试
- [x] 原子写：中途失败不留半个文件，有测试
- [x] mode/owner/group 生效；只改 mode 也算 `changed`
- [x] `Backup` 保留旧内容并在结果里给出路径
- [x] 死人开关：模拟「写入后连不上服务端」，确认自动恢复，有测试
- [x] 死人开关在 agent 重启后仍然有效（计时器状态要落盘，不能只在内存）
- [x] `fs.get` 有大小上限
- [x] 全部操作受本地路径白名单约束（issue 0023）
