//go:build integration

package portforward

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	turntf "github.com/tursom/turntf-go"
)

func TestRealTurntfForwardsTCPAndUDPToDifferentServerUsers(t *testing.T) {
	turntfBinary := os.Getenv("TURNTF_BIN")
	if turntfBinary == "" {
		t.Skip("TURNTF_BIN is required for integration tests")
	}
	portForwardBinary := os.Getenv("PORT_FORWARD_BIN")
	if portForwardBinary == "" {
		t.Skip("PORT_FORWARD_BIN is required for integration tests")
	}
	turntfProcess := startTurntfProcess(t, turntfBinary)
	baseURL := turntfProcess.baseURL
	defer turntfProcess.stop()

	adminToken := integrationAdminLogin(t, baseURL)
	tcpServerUser := createIntegrationUser(t, baseURL, adminToken, "forward-tcp-server")
	udpServerUser := createIntegrationUser(t, baseURL, adminToken, "forward-udp-server")
	clientUser := createIntegrationUser(t, baseURL, adminToken, "forward-client")

	tcpTarget, stopTCP := startTCPHalfCloseTarget(t)
	defer stopTCP()
	udpTarget, stopUDP := startUDPEchoTarget(t)
	defer stopUDP()
	tcpListen := reserveAddress(t, NetworkTCP)
	udpListen := reserveAddress(t, NetworkUDP)

	tcpServer := startPortForwardProcess(t, portForwardBinary, "server", integrationServerYAML(
		baseURL, "forward-tcp-server", "forward-tcp-server-password", tcpTarget, NetworkTCP,
		clientUser.NodeID, clientUser.UserID, 256, 2*time.Minute,
	))
	defer tcpServer.stop()
	udpServer := startPortForwardProcess(t, portForwardBinary, "server", integrationServerYAML(
		baseURL, "forward-udp-server", "forward-udp-server-password", udpTarget, NetworkUDP,
		clientUser.NodeID, clientUser.UserID, 2, 1500*time.Millisecond,
	))
	defer udpServer.stop()
	client := startPortForwardProcess(t, portForwardBinary, "client", integrationClientYAML(
		baseURL, tcpListen, udpListen, tcpServerUser, udpServerUser,
	))
	defer client.stop()
	waitForTCPListener(t, tcpListen)

	waitForTCPForward(t, tcpListen, "payload")
	udpA := openUDPForward(t, udpListen)
	defer udpA.Close()
	udpB := openUDPForward(t, udpListen)
	defer udpB.Close()
	udpC := openUDPForward(t, udpListen)
	defer udpC.Close()
	assertUDPConnForward(t, udpA, "first-source")
	assertUDPConnForward(t, udpB, "second-source")
	assertUDPConnNoResponse(t, udpC, "over-capacity")
	time.Sleep(2500 * time.Millisecond)
	assertUDPConnForward(t, udpC, "after-idle-reclaim")

	turntfProcess.stop()
	time.Sleep(250 * time.Millisecond)
	client.assertRunning()
	tcpServer.assertRunning()
	udpServer.assertRunning()
	turntfProcess.start()
	waitForTCPForward(t, tcpListen, "after-reconnect")
	assertUDPConnForward(t, udpC, "udp-after-reconnect")

	client.stop()
	tcpServer.stop()
	udpServer.stop()
}

func createIntegrationUser(t *testing.T, baseURL, token, loginName string) turntf.User {
	t.Helper()
	payload, err := json.Marshal(map[string]string{
		"username":   loginName,
		"login_name": loginName,
		"password":   loginName + "-password",
		"role":       "user",
	})
	if err != nil {
		t.Fatalf("marshal user %s: %v", loginName, err)
	}
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, baseURL+"/users", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("create user request %s: %v", loginName, err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("create user %s: %v", loginName, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("create user %s status %d: %s", loginName, response.StatusCode, body)
	}
	var user turntf.User
	if err := json.NewDecoder(response.Body).Decode(&user); err != nil {
		t.Fatalf("decode user %s: %v", loginName, err)
	}
	return user
}

func integrationAdminLogin(t *testing.T, baseURL string) string {
	t.Helper()
	payload, err := json.Marshal(map[string]string{"login_name": "root", "password": "root"})
	if err != nil {
		t.Fatalf("marshal admin login: %v", err)
	}
	response, err := http.Post(baseURL+"/auth/login", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("admin login: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("admin login status %d: %s", response.StatusCode, body)
	}
	var result struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatalf("decode admin login: %v", err)
	}
	return result.Token
}

type integrationTurntfProcess struct {
	t          *testing.T
	binary     string
	configPath string
	baseURL    string
	command    *exec.Cmd
	output     bytes.Buffer
}

type integrationPortForwardProcess struct {
	t       *testing.T
	command *exec.Cmd
	output  bytes.Buffer
	done    chan error
}

func startPortForwardProcess(t *testing.T, binary, mode, config string) *integrationPortForwardProcess {
	t.Helper()
	configPath := filepath.Join(t.TempDir(), mode+".yaml")
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatalf("write port-forward %s config: %v", mode, err)
	}
	process := &integrationPortForwardProcess{t: t, done: make(chan error, 1)}
	process.command = exec.Command(binary, mode, "-c", configPath)
	process.command.Stdout = &process.output
	process.command.Stderr = &process.output
	if err := process.command.Start(); err != nil {
		t.Fatalf("start port-forward %s: %v", mode, err)
	}
	go func(command *exec.Cmd) { process.done <- command.Wait() }(process.command)
	return process
}

func (p *integrationPortForwardProcess) assertRunning() {
	p.t.Helper()
	if p.command == nil || p.command.Process == nil {
		p.t.Fatal("port-forward process is not running")
	}
	select {
	case err := <-p.done:
		p.command = nil
		p.t.Fatalf("port-forward process exited during turntf outage: %v\n%s", err, p.output.String())
	default:
	}
	if err := p.command.Process.Signal(syscall.Signal(0)); err != nil {
		p.t.Fatalf("inspect port-forward process: %v", err)
	}
}

func (p *integrationPortForwardProcess) stop() {
	p.t.Helper()
	if p.command == nil {
		return
	}
	command := p.command
	p.command = nil
	if err := command.Process.Signal(os.Interrupt); err != nil {
		p.t.Fatalf("signal port-forward process: %v\n%s", err, p.output.String())
	}
	select {
	case err := <-p.done:
		if err != nil {
			p.t.Fatalf("port-forward process did not exit cleanly: %v\n%s", err, p.output.String())
		}
	case <-time.After(10 * time.Second):
		_ = command.Process.Kill()
		<-p.done
		p.t.Fatalf("port-forward process did not stop after SIGINT\n%s", p.output.String())
	}
}

func integrationServerYAML(
	baseURL, loginName, password, target, network string,
	clientNodeID, clientUserID int64,
	maxSessions int,
	idleTimeout time.Duration,
) string {
	return fmt.Sprintf(`turntf:
  base_url: %q
  credentials:
    login_name: %q
    password: {source: plain, value: %q}
  request_timeout: 10s
  ping_interval: 1s
rules:
  - name: %q
    network: %s
    target: %q
    allowed_clients:
      - {node_id: %d, user_id: %d}
    dial_timeout: 10s
    max_sessions: %d
    udp_idle_timeout: %s
`, baseURL, loginName, password, network+"-target", network, target, clientNodeID, clientUserID, maxSessions, idleTimeout)
}

func integrationClientYAML(baseURL, tcpListen, udpListen string, tcpServer, udpServer turntf.User) string {
	return fmt.Sprintf(`turntf:
  base_url: %q
  credentials:
    login_name: forward-client
    password: {source: plain, value: forward-client-password}
  request_timeout: 10s
  ping_interval: 1s
forwards:
  - name: tcp
    network: tcp
    listen: %q
    server_user: {node_id: %d, user_id: %d}
    remote_rule: tcp-target
    handshake_timeout: 10s
    max_sessions: 256
    udp_idle_timeout: 2m
  - name: udp
    network: udp
    listen: %q
    server_user: {node_id: %d, user_id: %d}
    remote_rule: udp-target
    handshake_timeout: 10s
    max_sessions: 3
    udp_idle_timeout: 1500ms
`, baseURL, tcpListen, tcpServer.NodeID, tcpServer.UserID, udpListen, udpServer.NodeID, udpServer.UserID)
}

func startTurntfProcess(t *testing.T, binary string) *integrationTurntfProcess {
	t.Helper()
	dir := t.TempDir()
	address := reserveAddress(t, NetworkTCP)
	configPath := filepath.Join(dir, "config.toml")
	databasePath := filepath.Join(dir, "turntf.db")
	config := fmt.Sprintf(`[services.http]
listen_addr = %q

[store.sqlite]
db_path = %q

[auth]
token_secret = "port-forward-integration-secret"

[cluster]
secret = "port-forward-integration-cluster-secret"

[auth.bootstrap_admin]
username = "root"
login_name = "root"
password_hash = "$2a$10$1gGoT/pdOu8vX1W28skBPOB7ICjISmVgt9lMyZf9c6re6cMHU6mAa"
`, address, databasePath)
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatalf("write turntf config: %v", err)
	}
	process := &integrationTurntfProcess{
		t:          t,
		binary:     binary,
		configPath: configPath,
		baseURL:    "http://" + address,
	}
	process.start()
	return process
}

func (p *integrationTurntfProcess) start() {
	p.t.Helper()
	if p.command != nil {
		p.t.Fatal("turntf process is already running")
	}
	p.output.Reset()
	p.command = exec.Command(p.binary, "serve", "--config", p.configPath)
	p.command.Stdout = &p.output
	p.command.Stderr = &p.output
	if err := p.command.Start(); err != nil {
		p.t.Fatalf("start turntf: %v", err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		response, err := http.Get(p.baseURL + "/healthz")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	_ = p.command.Process.Kill()
	_ = p.command.Wait()
	p.command = nil
	p.t.Fatalf("turntf did not become healthy: %s", p.output.String())
}

func (p *integrationTurntfProcess) stop() {
	p.t.Helper()
	if p.command == nil {
		return
	}
	command := p.command
	p.command = nil
	_ = command.Process.Signal(os.Interrupt)
	wait := make(chan struct{})
	go func() { _ = command.Wait(); close(wait) }()
	select {
	case <-wait:
	case <-time.After(5 * time.Second):
		_ = command.Process.Kill()
		<-wait
	}
}

func startTCPHalfCloseTarget(t *testing.T) (string, func()) {
	t.Helper()
	listener, err := net.ListenTCP(NetworkTCP, &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("listen TCP target: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			conn, err := listener.AcceptTCP()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				body, _ := io.ReadAll(conn)
				_, _ = conn.Write(append([]byte("tcp:"), body...))
				_ = conn.CloseWrite()
			}()
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()
	return listener.Addr().String(), func() { cancel(); _ = listener.Close() }
}

func startUDPEchoTarget(t *testing.T) (string, func()) {
	t.Helper()
	conn, err := net.ListenUDP(NetworkUDP, &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("listen UDP target: %v", err)
	}
	go func() {
		buffer := make([]byte, 65535)
		for {
			n, peer, err := conn.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			_, _ = conn.WriteToUDP(append([]byte("udp:"), buffer[:n]...), peer)
		}
	}()
	return conn.LocalAddr().String(), func() { _ = conn.Close() }
}

func reserveAddress(t *testing.T, network string) string {
	t.Helper()
	if network == NetworkUDP {
		conn, err := net.ListenUDP(NetworkUDP, &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
		if err != nil {
			t.Fatalf("reserve UDP address: %v", err)
		}
		address := conn.LocalAddr().String()
		_ = conn.Close()
		return address
	}
	listener, err := net.ListenTCP(NetworkTCP, &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("reserve TCP address: %v", err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	return address
}

func waitForTCPListener(t *testing.T, address string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout(NetworkTCP, address, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for TCP listener %s", address)
}

func waitForTCPForward(t *testing.T, address, payload string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := net.DialTCP(NetworkTCP, nil, mustResolveTCPAddr(t, address))
		if err == nil {
			_ = conn.SetDeadline(time.Now().Add(time.Second))
			if _, err = conn.Write([]byte(payload)); err == nil {
				err = conn.CloseWrite()
			}
			var response []byte
			if err == nil {
				response, err = io.ReadAll(conn)
				if err == nil && string(response) != "tcp:"+payload {
					err = fmt.Errorf("unexpected response %q", response)
				}
			}
			_ = conn.Close()
			if err == nil {
				return
			}
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("TCP forward did not recover after turntf restart: %v", lastErr)
}

func openUDPForward(t *testing.T, address string) *net.UDPConn {
	t.Helper()
	conn, err := net.DialUDP(NetworkUDP, nil, mustResolveUDPAddr(t, address))
	if err != nil {
		t.Fatalf("dial UDP forward: %v", err)
	}
	return conn
}

func assertUDPConnForward(t *testing.T, conn *net.UDPConn, payload string) {
	t.Helper()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := conn.Write([]byte(payload)); err != nil {
		t.Fatalf("write UDP forward: %v", err)
	}
	buffer := make([]byte, 128)
	n, err := conn.Read(buffer)
	if err != nil {
		t.Fatalf("read UDP forward: %v", err)
	}
	if got := string(buffer[:n]); got != "udp:"+payload {
		t.Fatalf("UDP response = %q", got)
	}
}

func assertUDPConnNoResponse(t *testing.T, conn *net.UDPConn, payload string) {
	t.Helper()
	_ = conn.SetDeadline(time.Now().Add(500 * time.Millisecond))
	if _, err := conn.Write([]byte(payload)); err != nil {
		t.Fatalf("write UDP forward: %v", err)
	}
	buffer := make([]byte, 128)
	if _, err := conn.Read(buffer); err == nil {
		t.Fatal("over-capacity UDP association unexpectedly received a response")
	} else if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		t.Fatalf("over-capacity UDP read error = %v, want timeout", err)
	}
}

func mustResolveTCPAddr(t *testing.T, address string) *net.TCPAddr {
	t.Helper()
	resolved, err := net.ResolveTCPAddr(NetworkTCP, address)
	if err != nil {
		t.Fatalf("resolve TCP address: %v", err)
	}
	return resolved
}

func mustResolveUDPAddr(t *testing.T, address string) *net.UDPAddr {
	t.Helper()
	resolved, err := net.ResolveUDPAddr(NetworkUDP, address)
	if err != nil {
		t.Fatalf("resolve UDP address: %v", err)
	}
	return resolved
}
