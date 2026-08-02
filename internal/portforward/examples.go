package portforward

const ServerExampleConfig = `# 转发服务端使用专用 turntf 普通用户登录。
turntf:
  base_url: "http://127.0.0.1:8080"
  request_timeout: "10s"
  ping_interval: "30s"
  credentials:
    login_name: "port-forward-server"
    password:
      source: "plain"
      value: "replace-me"

# 每条规则只允许白名单中的 turntf 用户访问固定目标。
rules:
  - name: "ssh"
    network: "tcp"
    target: "127.0.0.1:22"
    dial_timeout: "10s"
    max_sessions: 256
    allowed_clients:
      - node_id: 4096
        user_id: 1025

  - name: "dns"
    network: "udp"
    target: "127.0.0.1:53"
    dial_timeout: "10s"
    udp_idle_timeout: "2m"
    max_sessions: 256
    allowed_clients:
      - node_id: 4096
        user_id: 1025
`

const ClientExampleConfig = `# 转发客户端使用自己的 turntf 普通用户登录。
turntf:
  base_url: "http://127.0.0.1:8080"
  request_timeout: "10s"
  ping_interval: "30s"
  credentials:
    login_name: "port-forward-client"
    password:
      source: "plain"
      value: "replace-me"

# 每条本地监听规则可选择不同的远端服务端用户和规则名。
forwards:
  - name: "local-ssh"
    network: "tcp"
    listen: "127.0.0.1:2222"
    server_user:
      node_id: 4096
      user_id: 1026
    remote_rule: "ssh"
    handshake_timeout: "10s"
    max_sessions: 256

  - name: "local-dns"
    network: "udp"
    listen: "127.0.0.1:5353"
    server_user:
      node_id: 8192
      user_id: 1026
    remote_rule: "dns"
    handshake_timeout: "10s"
    udp_idle_timeout: "2m"
    max_sessions: 256
`
