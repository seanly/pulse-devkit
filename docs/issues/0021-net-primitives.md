# 0021 net.*：向外探测

**里程碑** M5 · **依赖** 0018

## 目标

把拨测变成一个原语。agent 报告**看到了什么**，不报告那意味着什么。

## 这是 certpulse 的代码搬家

`net.tlsprobe` 的实现就是 certpulse 现在的 `internal/prober`：DNS 解析、对每个地址 TCP + TLS 握手、收集证书、分类错误。那套代码已经在生产跑了，这个 issue 是把它的**通用部分**搬进框架。

搬的边界在哪，是这个 issue 唯一要想清的事。

## 观测与判定的分界

| 归框架（观测） | 归应用（判定） |
|---|---|
| 解析到哪些地址 | 这些地址算内网还是公网 |
| 握手成功/失败，失败的分类 | 失败算 error 还是 skipped |
| 证书的 subject/issuer/有效期/指纹/SAN | 还剩几天算 warning |
| 链是否被系统根验证通过 | 不通过算不算问题（内网自签名可能是正常的） |
| 每个地址各自的结果 | 怎么聚合成一个结论 |

判据：**同一个观测，不同应用会得出不同结论**。「还剩 20 天」对一个 90 天轮换的站点是正常，对一个要求 30 天提前续期的策略是 warning。所以 `warn_days` 不该在 agent 上。

这也修正了一处积累的别扭：certpulse 现在的 agent 带着 `warn_days` 配置，而注释里写着「这只是个本地默认值，服务端按任务应用自己的阈值」—— 一个存在但不该被信任的字段。搬家时它不跟过来。

## 载荷

```go
// NetTLSProbe 对一个名字做一次 TLS 观测。
type NetTLSProbe struct {
    Host string `json:"host"`
    Port int    `json:"port"`
    // SNI 覆盖握手里发的名字，空表示用 Host。
    SNI string `json:"sni,omitempty"`

    // AllAddresses 为真时探测解析出的每个地址，而不是只连一次。
    //
    // 默认为真，因为这是这个原语存在的理由：CDN 或负载均衡后面，一次连接
    // 只落到一个后端，某个节点上的过期证书会躲在健康的兄弟后面。
    AllAddresses *bool `json:"all_addresses,omitempty"`

    // Resolvers 覆盖用哪些 DNS 服务器，空表示系统配置。
    Resolvers []string `json:"resolvers,omitempty"`
}
```

**`Resolvers` 这个字段要受本地策略约束**（issue 0023）。一个服务端能任意指定 DNS 服务器的 agent，可以被用来把查询打到任何地址 —— 这和 deny list 要防的是同一类事。现在的约束是默认拒绝：非空列表在没有 `AllowResolvers` 钩子时直接返回 `resolvers_denied`，不发起查询。哪份策略可以放行，留给 0023。

结果载荷是 `pulse.TLSObservation`：per-address 事实 + 摘要证书 + 时序。没有 status，没有 `warn_days`。certpulse 收到之后再判成自己的 `probe.Result`。

## 握手不验证

certpulse 现在的实现有一条值得原样保留的决定：**握手时不验证证书，验证在之后对收集到的证书做**。

因为一个过期或不被信任的链仍然要报告它的 issuer、有效期和指纹 —— 那恰恰是拨测存在的理由。握手时就失败的话，拿不到任何信息，只知道「失败了」。

## 解析后检查

地址类的 deny 规则只能在 DNS 解析之后执行，而框架的派发层看不到解析结果。所以这个原语必须在连接前自己调 `pulseguard` 的 `CheckIPs`。

这条在 `pulseguard` 的文档里已经写了（「Executor 必须自己调它」），搬家后它从一个约定变成框架内部的保证。

## 验收标准

- [x] certpulse 的 prober 测试搬过来并通过（它们是这套逻辑的真实覆盖）
- [x] 结果里没有任何判定字段：没有 warn_days、没有「是否告警」
- [x] 每个解析地址各自一条结果；摘要是最早到期的那张证书
- [x] 握手不验证，过期/不受信的链仍然报告完整证书信息
- [x] 解析后调 `pulseguard.CheckIPs`，有测试
- [x] `Resolvers` 受本地策略约束（issue 0023）：默认拒绝，放行钩子留给策略文件
- [x] certpulse 迁过去之后，它的 agent 侧不再有任何证书判定代码
