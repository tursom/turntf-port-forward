package portforward

import (
	"strings"
	"testing"
	"time"
)

func TestLoadServerConfigAppliesDefaults(t *testing.T) {
	cfg, err := DecodeServerConfig(strings.NewReader(`
turntf:
  base_url: http://127.0.0.1:8080
  credentials:
    login_name: forward-server
    password: {source: plain, value: secret}
rules:
  - name: ssh
    network: tcp
    target: 127.0.0.1:22
    allowed_clients:
      - {node_id: 4096, user_id: 1025}
`))
	if err != nil {
		t.Fatalf("DecodeServerConfig: %v", err)
	}
	if cfg.Rules[0].DialTimeout.Duration != 10*time.Second {
		t.Fatalf("dial timeout = %v", cfg.Rules[0].DialTimeout.Duration)
	}
	if cfg.Rules[0].MaxSessions != 256 {
		t.Fatalf("max sessions = %d", cfg.Rules[0].MaxSessions)
	}
	if cfg.Rules[0].UDPIdleTimeout.Duration != 2*time.Minute {
		t.Fatalf("udp idle timeout = %v", cfg.Rules[0].UDPIdleTimeout.Duration)
	}
}

func TestLoadClientConfigSupportsDifferentServerUsers(t *testing.T) {
	cfg, err := DecodeClientConfig(strings.NewReader(`
turntf:
  base_url: http://127.0.0.1:8080
  credentials:
    login_name: forward-client
    password: {source: plain, value: secret}
forwards:
  - name: ssh
    network: tcp
    listen: 127.0.0.1:2222
    server_user: {node_id: 4096, user_id: 1025}
    remote_rule: ssh
  - name: dns
    network: udp
    listen: 127.0.0.1:5353
    server_user: {node_id: 8192, user_id: 1026}
    remote_rule: dns
`))
	if err != nil {
		t.Fatalf("DecodeClientConfig: %v", err)
	}
	if got := cfg.Forwards[1].ServerUser.ToTurntf(); got.NodeID != 8192 || got.UserID != 1026 {
		t.Fatalf("second server user = %+v", got)
	}
}

func TestPlainPasswordUsesTurntfServerWireContract(t *testing.T) {
	password, err := (PasswordConfig{Source: "plain", Value: "secret"}).ToTurntf()
	if err != nil {
		t.Fatalf("ToTurntf: %v", err)
	}
	if got := password.WireValue(); got != "secret" {
		t.Fatalf("wire password = %q, want plaintext required by turntf authentication", got)
	}
}

func TestConfigRejectsHashedPasswordSource(t *testing.T) {
	_, err := (PasswordConfig{Source: "hashed", Value: "secret"}).ToTurntf()
	if err == nil || !strings.Contains(err.Error(), "must be plain") {
		t.Fatalf("error = %v, want plain-only validation error", err)
	}
}

func TestConfigRejectsUnknownFieldsAndDuplicates(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		load func(string) error
		want string
	}{
		{
			name: "unknown server field",
			yaml: validServerYAML() + "unknown: true\n",
			load: func(value string) error {
				_, err := DecodeServerConfig(strings.NewReader(value))
				return err
			},
			want: "field unknown not found",
		},
		{
			name: "duplicate client listen",
			yaml: validClientYAML() + `  - name: ssh-copy
    network: tcp
    listen: 127.0.0.1:2222
    server_user: {node_id: 4096, user_id: 1025}
    remote_rule: ssh
`,
			load: func(value string) error {
				_, err := DecodeClientConfig(strings.NewReader(value))
				return err
			},
			want: "duplicate listen",
		},
		{
			name: "duplicate server rule",
			yaml: validServerYAML() + `  - name: ssh
    network: tcp
    target: 127.0.0.1:23
    allowed_clients: [{node_id: 4096, user_id: 1025}]
`,
			load: func(value string) error {
				_, err := DecodeServerConfig(strings.NewReader(value))
				return err
			},
			want: "duplicate rule name",
		},
		{
			name: "invalid network",
			yaml: strings.Replace(validServerYAML(), "network: tcp", "network: unix", 1),
			load: func(value string) error {
				_, err := DecodeServerConfig(strings.NewReader(value))
				return err
			},
			want: "network must be tcp or udp",
		},
		{
			name: "invalid target address",
			yaml: strings.Replace(validServerYAML(), "127.0.0.1:22", "127.0.0.1", 1),
			load: func(value string) error {
				_, err := DecodeServerConfig(strings.NewReader(value))
				return err
			},
			want: "missing port in address",
		},
		{
			name: "extra YAML document",
			yaml: validServerYAML() + "---\nrules: []\n",
			load: func(value string) error {
				_, err := DecodeServerConfig(strings.NewReader(value))
				return err
			},
			want: "exactly one YAML document",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.load(tt.yaml)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func validServerYAML() string {
	return `turntf:
  base_url: http://127.0.0.1:8080
  credentials:
    login_name: forward-server
    password: {source: plain, value: secret}
rules:
  - name: ssh
    network: tcp
    target: 127.0.0.1:22
    allowed_clients: [{node_id: 4096, user_id: 1025}]
`
}

func validClientYAML() string {
	return `turntf:
  base_url: http://127.0.0.1:8080
  credentials:
    login_name: forward-client
    password: {source: plain, value: secret}
forwards:
  - name: ssh
    network: tcp
    listen: 127.0.0.1:2222
    server_user: {node_id: 4096, user_id: 1025}
    remote_rule: ssh
`
}
