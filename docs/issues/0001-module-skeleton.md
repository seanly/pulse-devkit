# 0001 模块骨架与 go.work 工作区

**里程碑** M0 · **依赖** —

## 目标

建立 `github.com/seanly/pulse-devkit` 模块，和 certpulse 组成一个 go.work 工作区，使得在 pulse-devkit 发布到 GitHub 之前，certpulse 就能 import 它。

## 范围

- `go.mod`：模块路径 `github.com/seanly/pulse-devkit`，go 1.26
- 消费方的 `go.work`：certpulse 聚合 `.` 与 `../pulse-devkit`
- `Makefile`：test / race / cover / fmt / vet / generate
- `README.md`、`.gitignore`、`LICENSE`

## go.work 而非 replace

`replace` 会写进 go.mod 并被提交，下游 `go get` 到一个指向本地路径的模块就炸了。go.work 不参与模块解析的发布面——它只影响本机构建，而且 `go.work` 本身不进 git（进 `.gitignore`）。

工作区由**消费方**持有，指向同级的框架 checkout：

```
work-dir/
├── certpulse/
│   └── go.work      ← use . 和 ../pulse-devkit
└── pulse-devkit/
```

起初放在两个仓库的父目录，后来移进了 certpulse。父目录的工作区会把同级的每个 Go 项目都拉进解析范围（那个目录下有十来个不相关的仓库），而且让「certpulse 怎么构建」取决于它的上一层长什么样。放进消费方就只有它自己的事。

**certpulse 的 Makefile 要改**：现在它用 `GO := GOWORK=off go`，注释说「GOWORK=off 防止父目录的 go.work 把不相关的模块拉进构建」。接入 pulse-devkit 后这正好反过来了——没有工作区就解析不到未发布的依赖。改成默认启用工作区，并保留一个 `GOWORK=off` 的 CI 开关（CI 上 pulse-devkit 已发布，不需要工作区）。

**Dockerfile 也要改**：构建上下文只有 certpulse 一个目录，容器里没有 pulse-devkit 源码。两条路：

1. pulse-devkit 发布到 GitHub 后，容器内走正常的模块下载（推荐，CI 和容器都不碰工作区）
2. 构建上下文提到父目录，把两个模块都 COPY 进去（本地开发用）

issue 0015 处理这件事。在 pulse-devkit 上传 GitHub 之前，容器构建会失败——这是预期的。

## 依赖纪律

核心模块只依赖：

- `google.golang.org/grpc`、`google.golang.org/protobuf`
- `golang.org/x/time`（限速器）

**不依赖**数据库驱动、不依赖迁移工具、不依赖 TOML/YAML 解析器。`sqlstore` 只用 `database/sql`，驱动由应用注册；迁移以 `embed.FS` 交出去，由应用的迁移工具执行。这样一个只用 agent 侧的下游项目不会被迫编译 pgx。

## 验收标准

- [x] `go build ./...` 通过
- [x] `go vet ./...` 通过
- [x] certpulse 的 `go.work` 存在，`go list -m` 解析到本地 checkout
- [x] pulse-devkit 与 certpulse 的 `go.mod` 里都没有 `replace`
- [x] `go.work` / `go.work.sum` 在两边的 `.gitignore` 里（它是每个开发者自己的，不是仓库的）
