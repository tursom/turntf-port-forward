# turntf TCP/UDP 端口转发

`turntf-port-forward` 通过 turntf 网络把本地 TCP 连接或 UDP 数据报转发到远端固定目标。一个二进制同时提供服务端和客户端模式；两端都需要使用预先创建的、可登录的 turntf 普通用户。

## 边界

- 服务端目标必须在配置中预先声明，客户端不能动态指定地址。
- 每条服务端规则必须配置 turntf 用户白名单。
- TCP 在 turntf 段使用可靠有序 Relay，并保留 TCP 半关闭语义。
- UDP 数据使用 `best_effort` 瞬时包，保持数据报边界但允许丢包和乱序；打开与关闭控制包使用 `route_retry`。
- 服务端账号应专用，并只保持一个转发服务端 session。turntf 断线会终止现有会话，重连后只接受新会话。
- 不提供 TLS 终止、SOCKS、HTTP CONNECT、透明代理或动态目标。

## 构建

```bash
go build -o turntf-port-forward ./cmd/turntf-port-forward
```

也可以构建容器镜像：

```bash
docker build -t turntf-port-forward:local .
```

## 发布

GitHub Actions 会在推送符合 SemVer 的 `v*` tag 后自动创建 GitHub Release。首次发布前，必须先把发布工作流提交并推送到默认分支，再创建指向该提交或后续提交的 tag：

```bash
git tag v1.0.0
git push origin v1.0.0
```

稳定版本使用 `vMAJOR.MINOR.PATCH`，例如 `v1.0.0`；`v1.0.0-rc.1` 等预发布 tag 会创建 prerelease。每个 Release 提供 Linux amd64/arm64、macOS amd64/arm64 和 Windows amd64 压缩包。压缩包包含二进制、README 和两份示例配置，并附带统一的 `SHA256SUMS`。

同一 tag 还会发布 `linux/amd64`、`linux/arm64` 多架构镜像到 `ghcr.io/tursom/turntf-port-forward`。稳定版会生成完整版本、主次版本、主版本和 `latest` 标签；预发布版只生成完整版本标签。SemVer build metadata 的 `+` 在镜像标签中转换为 `_`，例如 `v1.0.0+build.4` 对应 `1.0.0_build.4`。推送 `master` 时会更新 `master` 和 `sha-<12 位提交前缀>` 标签。

首次发布镜像后，需要仓库 Owner 在 GitHub Package settings 中将 Package visibility 手动设置为 Public。

## 配置

生成示例配置：

```bash
./turntf-port-forward example-config server > server.yaml
./turntf-port-forward example-config client > client.yaml
```

校验配置：

```bash
./turntf-port-forward check-config server -c server.yaml
./turntf-port-forward check-config client -c client.yaml
```

服务端每条 `rules` 规则通过 `name` 暴露固定的 `network` 和 `target`，并用 `allowed_clients` 限制可接入的 turntf 用户。客户端每条 `forwards` 规则声明本地 `listen`、远端 `server_user` 和 `remote_rule`；同一客户端进程可以连接不同的服务端用户。

默认值：

- turntf 请求和隧道握手超时：10 秒
- 每条规则最大并发会话：256
- UDP 空闲回收：2 分钟
- UDP 建连期间每个本地源最多暂存 64 个数据报

密码仅支持 `source: plain`，与当前 turntf 服务端的认证契约一致。凭据会出现在登录请求中，生产环境必须使用 `https://` 的 `base_url`（对应 WSS）保护传输，并限制配置文件权限；程序不会自动创建 turntf 用户。

## 运行

```bash
./turntf-port-forward server -c server.yaml
./turntf-port-forward client -c client.yaml
```

收到 `SIGINT` 或 `SIGTERM` 后，程序停止监听并关闭活动连接。

容器使用同一个镜像运行两种模式，并通过只读挂载提供配置：

```bash
docker run --rm \
  -v "$PWD/server.yaml:/etc/turntf-port-forward/server.yaml:ro" \
  ghcr.io/tursom/turntf-port-forward:latest \
  server -c /etc/turntf-port-forward/server.yaml

docker run --rm \
  -p 127.0.0.1:8080:8080/tcp \
  -p 127.0.0.1:5353:5353/udp \
  -v "$PWD/client.yaml:/etc/turntf-port-forward/client.yaml:ro" \
  ghcr.io/tursom/turntf-port-forward:latest \
  client -c /etc/turntf-port-forward/client.yaml
```

客户端配置中的容器监听地址必须使用 `0.0.0.0`，再分别通过 `/tcp` 和 `/udp` 端口映射暴露给主机。Linux 也可以使用 `--network host`，此时无需 `-p`，监听地址和端口直接位于主机网络命名空间。

## 测试

```bash
go test ./...
./scripts/integration-test.sh
```

集成测试会临时构建并启动本仓库同级的 turntf 服务，创建测试用户，并验证真实 TCP/UDP 转发。

## 协议生成

应用协议源文件是 `proto/tunnel.proto`，生成结果位于 `internal/proto/tunnel.pb.go`。修改协议后运行：

```bash
go generate ./...
```

生成前需要在 `PATH` 中安装 `protoc` 和 `protoc-gen-go`；提交协议变更时必须同时提交生成结果。
