# 0019 exec.run：一次性工作区里跑一个程序

**里程碑** M5 · **依赖** 0018

## 目标

整套原语的拱心石。一个操作顶掉九成「在目标主机上做点什么」的场景：Ansible、脚本、包管理器、任意已装二进制。

## 载荷

```go
// ExecRun 在一个一次性工作区里跑一个程序。
type ExecRun struct {
    // Files 是工作区里要落的内容，键是工作区内的相对路径。
    // playbook 树、inventory、SSH 私钥、脚本都走这里。
    Files map[string]File `json:"files,omitempty"`
    // Blobs 是要先拉下来再展开的大内容（按内容摘要，见 0018）。
    Blobs map[string]Blob `json:"blobs,omitempty"`

    // Argv 是程序和参数。Argv[0] 是可执行文件，**不经 shell**。
    Argv []string `json:"argv"`
    // Env 是环境变量。secret 走这里，不走 Argv —— 见下。
    Env map[string]string `json:"env,omitempty"`
    // Stdin 喂给进程的标准输入：sudo 密码、vault 密码。
    Stdin string `json:"stdin,omitempty"`
    // Dir 是工作区内的相对工作目录，空表示工作区根。
    Dir string `json:"dir,omitempty"`

    // Capture 是跑完要读回的文件（工作区内相对路径），比如 Ansible 的
    // JSON 结果。单个文件有大小上限，超了截断并标记。
    Capture []string `json:"capture,omitempty"`
    // StreamOutput 打开时，stdout/stderr 边跑边回传（0018 的 JobProgress）。
    // 关掉时只在结果里带最后 N KB —— 短命令不值得一路流。
    StreamOutput bool `json:"stream_output,omitempty"`
}

type File struct {
    Content string `json:"content"`
    // Mode 是八进制字符串（"0600"）。空表示 0644。
    Mode string `json:"mode,omitempty"`
}

type Blob struct {
    Digest string `json:"digest"`
    // Extract 为真时按归档展开到这个路径，否则原样落成一个文件。
    Extract bool `json:"extract,omitempty"`
}

// ExecResult 是回报的载荷。
type ExecResult struct {
    ExitCode int               `json:"exit_code"`
    Stdout   string            `json:"stdout,omitempty"`   // 尾部若干 KB
    Stderr   string            `json:"stderr,omitempty"`
    Captured map[string]string `json:"captured,omitempty"`
    Truncated bool             `json:"truncated,omitempty"`
    DurationMS int64           `json:"duration_ms"`
}
```

## 三个协议级决定

### 不经 shell

`Argv` 是 `exec.Command(argv[0], argv[1:]...)`，不是 `sh -c`。

这消掉整个 shell 注入面，但更重要的是它让本地策略的可执行文件白名单（issue 0023）真的有意义 —— 如果允许 `/bin/sh -c`，白名单就成了装饰，因为 shell 能执行任何东西。

所以这不是实现细节，是协议承诺：**载荷里没有任何地方可以放一段 shell**。需要管道或重定向的，把脚本放进 `Files` 再执行它 —— 那样脚本内容是可审计的、可记录的，而不是一行拼出来的字符串。

### secret 走 Env，不走 Argv

Argv 在目标主机上 `ps` 可见，任何本机用户都能读。

这不是假想：go-ansible 的 [issue #140](https://github.com/apenella/go-ansible/issues/140) 里贴出的真实命令行是

```
ansible-playbook --extra-vars {"ansible_sudo_pass":"REDACTED"} --forks 5 ...
```

那个 sudo 密码对目标主机上的每个用户都是明文。环境变量在 `/proc/<pid>/environ`，要同用户或 root 才能读 —— 不完美，但差一个数量级。

实现上要补一条：**agent 的日志和回报里都不能出现 Env 的值**。只记键名。

### 一个 job = 一个工作区

工作区在 job 开始时建、结束时删，`defer` 保证。

考虑过「会话」模型：打开工作区 → 跑多个操作 → 关闭。更灵活，但有个要命的失败模式 —— 服务端在会话中间崩了，目标主机上就留着一个装有 SSH 私钥的目录，而清理它的那条指令永远不会到。

单 job 自包含意味着 agent 自己负责清理，服务端崩溃、网络断开、agent 重启都不漏。代价是跨操作共享状态要重新落一次盘，而那比在 1000 台上留下私钥便宜。

工作区位置：本地配置的 `exec.workspace_root` 下的随机目录，mode 0700。**优先 tmpfs** —— 私钥不落持久盘。

## 取消要按进程组杀

`ansible-playbook` 会 fork 一堆 `ssh`。只杀父进程的话，那些 ssh 还在跑，而且还握着目标主机的连接。

所以：`SysProcAttr{Setpgid: true}` 启动，取消时 `syscall.Kill(-pgid, SIGTERM)`，宽限期后 `SIGKILL`。

## 长任务

- 跑起来之后周期性发 `JobLease`（0018），直到进程退出
- `StreamOutput` 打开时，stdout/stderr 各自一路 `JobProgress`，带单调 `seq`
- 输出速率要有上限：一个疯掉的进程每秒刷几十 MB，不能让它把服务端打垮。超过阈值就降采样并在结果里标记

## 验收标准

- [ ] 工作区建立/落盘/mode/清理，含 agent 被取消和 panic 两条路径
- [ ] `Argv` 不经 shell，有测试证明 `"; rm -rf /"` 这类参数只是一个普通参数
- [ ] Env 的值不出现在日志和回报里，有测试
- [ ] 取消杀整个进程组，有测试（启一个 fork 子进程的脚本，确认子进程也死了）
- [ ] `Capture` 的大小上限与截断标记
- [ ] 长任务续租，有测试（跑一个超过租约期的任务，确认没有被重新派发）
- [ ] 流式输出带单调 `seq`，速率超限时降采样
- [ ] 工作区优先落 tmpfs，配置可改
