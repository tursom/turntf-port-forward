package portforward

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	turntf "github.com/tursom/turntf-go"
	"gopkg.in/yaml.v3"
)

const (
	NetworkTCP = "tcp"
	NetworkUDP = "udp"
)

type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode {
		return errors.New("duration must be a scalar")
	}
	parsed, err := time.ParseDuration(strings.TrimSpace(node.Value))
	if err != nil {
		return err
	}
	d.Duration = parsed
	return nil
}

type TurntfConfig struct {
	BaseURL        string            `yaml:"base_url"`
	Credentials    CredentialsConfig `yaml:"credentials"`
	RequestTimeout Duration          `yaml:"request_timeout"`
	PingInterval   Duration          `yaml:"ping_interval"`
}

type CredentialsConfig struct {
	NodeID    int64          `yaml:"node_id"`
	UserID    int64          `yaml:"user_id"`
	LoginName string         `yaml:"login_name"`
	Password  PasswordConfig `yaml:"password"`
}

type PasswordConfig struct {
	Source string `yaml:"source"`
	Value  string `yaml:"value"`
}

type UserRefConfig struct {
	NodeID int64 `yaml:"node_id"`
	UserID int64 `yaml:"user_id"`
}

func (u UserRefConfig) ToTurntf() turntf.UserRef {
	return turntf.UserRef{NodeID: u.NodeID, UserID: u.UserID}
}

type ServerConfig struct {
	Turntf TurntfConfig `yaml:"turntf"`
	Rules  []ServerRule `yaml:"rules"`
}

type ServerRule struct {
	Name           string          `yaml:"name"`
	Network        string          `yaml:"network"`
	Target         string          `yaml:"target"`
	AllowedClients []UserRefConfig `yaml:"allowed_clients"`
	DialTimeout    Duration        `yaml:"dial_timeout"`
	MaxSessions    int             `yaml:"max_sessions"`
	UDPIdleTimeout Duration        `yaml:"udp_idle_timeout"`
}

type ClientConfig struct {
	Turntf   TurntfConfig    `yaml:"turntf"`
	Forwards []ClientForward `yaml:"forwards"`
}

type ClientForward struct {
	Name             string        `yaml:"name"`
	Network          string        `yaml:"network"`
	Listen           string        `yaml:"listen"`
	ServerUser       UserRefConfig `yaml:"server_user"`
	RemoteRule       string        `yaml:"remote_rule"`
	HandshakeTimeout Duration      `yaml:"handshake_timeout"`
	MaxSessions      int           `yaml:"max_sessions"`
	UDPIdleTimeout   Duration      `yaml:"udp_idle_timeout"`
}

func LoadServerConfig(path string) (ServerConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return ServerConfig{}, err
	}
	defer file.Close()
	return DecodeServerConfig(file)
}

func LoadClientConfig(path string) (ClientConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return ClientConfig{}, err
	}
	defer file.Close()
	return DecodeClientConfig(file)
}

func DecodeServerConfig(reader io.Reader) (ServerConfig, error) {
	var cfg ServerConfig
	if err := decodeKnownYAML(reader, &cfg); err != nil {
		return ServerConfig{}, err
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return ServerConfig{}, err
	}
	return cfg, nil
}

func DecodeClientConfig(reader io.Reader) (ClientConfig, error) {
	var cfg ClientConfig
	if err := decodeKnownYAML(reader, &cfg); err != nil {
		return ClientConfig{}, err
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return ClientConfig{}, err
	}
	return cfg, nil
}

func decodeKnownYAML(reader io.Reader, target any) error {
	decoder := yaml.NewDecoder(reader)
	decoder.KnownFields(true)
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err != nil {
			return err
		}
		return errors.New("config must contain exactly one YAML document")
	}
	return nil
}

func (c *ServerConfig) applyDefaults() {
	c.Turntf.applyDefaults()
	for i := range c.Rules {
		applyRuleDefaults(&c.Rules[i].DialTimeout, &c.Rules[i].UDPIdleTimeout, &c.Rules[i].MaxSessions)
	}
}

func (c *ClientConfig) applyDefaults() {
	c.Turntf.applyDefaults()
	for i := range c.Forwards {
		applyRuleDefaults(&c.Forwards[i].HandshakeTimeout, &c.Forwards[i].UDPIdleTimeout, &c.Forwards[i].MaxSessions)
	}
}

func (c *TurntfConfig) applyDefaults() {
	if c.RequestTimeout.Duration == 0 {
		c.RequestTimeout.Duration = 10 * time.Second
	}
	if c.PingInterval.Duration == 0 {
		c.PingInterval.Duration = 30 * time.Second
	}
}

func applyRuleDefaults(operationTimeout, udpIdleTimeout *Duration, maxSessions *int) {
	if operationTimeout.Duration == 0 {
		operationTimeout.Duration = 10 * time.Second
	}
	if udpIdleTimeout.Duration == 0 {
		udpIdleTimeout.Duration = 2 * time.Minute
	}
	if *maxSessions == 0 {
		*maxSessions = 256
	}
}

func (c ServerConfig) Validate() error {
	if err := c.Turntf.Validate(); err != nil {
		return err
	}
	if len(c.Rules) == 0 {
		return errors.New("at least one rule is required")
	}
	seen := make(map[string]struct{}, len(c.Rules))
	for i, rule := range c.Rules {
		if err := rule.Validate(); err != nil {
			return fmt.Errorf("rules[%d]: %w", i, err)
		}
		if _, exists := seen[rule.Name]; exists {
			return fmt.Errorf("duplicate rule name %q", rule.Name)
		}
		seen[rule.Name] = struct{}{}
	}
	return nil
}

func (c ClientConfig) Validate() error {
	if err := c.Turntf.Validate(); err != nil {
		return err
	}
	if len(c.Forwards) == 0 {
		return errors.New("at least one forward is required")
	}
	seenNames := make(map[string]struct{}, len(c.Forwards))
	seenListeners := make(map[string]struct{}, len(c.Forwards))
	for i, forward := range c.Forwards {
		if err := forward.Validate(); err != nil {
			return fmt.Errorf("forwards[%d]: %w", i, err)
		}
		if _, exists := seenNames[forward.Name]; exists {
			return fmt.Errorf("duplicate forward name %q", forward.Name)
		}
		seenNames[forward.Name] = struct{}{}
		listenerKey := forward.Network + "\x00" + forward.Listen
		if _, exists := seenListeners[listenerKey]; exists {
			return fmt.Errorf("duplicate listen %s %q", forward.Network, forward.Listen)
		}
		seenListeners[listenerKey] = struct{}{}
	}
	return nil
}

func (c TurntfConfig) Validate() error {
	if strings.TrimSpace(c.BaseURL) == "" {
		return errors.New("turntf.base_url is required")
	}
	if _, err := c.Credentials.ToTurntf(); err != nil {
		return fmt.Errorf("turntf.credentials: %w", err)
	}
	if c.RequestTimeout.Duration <= 0 || c.PingInterval.Duration <= 0 {
		return errors.New("turntf timeouts must be positive")
	}
	return nil
}

func (r ServerRule) Validate() error {
	if strings.TrimSpace(r.Name) == "" {
		return errors.New("name is required")
	}
	if err := validateNetwork(r.Network); err != nil {
		return err
	}
	if err := validateHostPort(r.Target, true); err != nil {
		return fmt.Errorf("target: %w", err)
	}
	if len(r.AllowedClients) == 0 {
		return errors.New("allowed_clients must not be empty")
	}
	seen := make(map[turntf.UserRef]struct{}, len(r.AllowedClients))
	for i, allowed := range r.AllowedClients {
		user := allowed.ToTurntf()
		if user.NodeID <= 0 || user.UserID <= 0 {
			return fmt.Errorf("allowed_clients[%d] requires positive node_id and user_id", i)
		}
		if _, exists := seen[user]; exists {
			return fmt.Errorf("duplicate allowed client %d:%d", user.NodeID, user.UserID)
		}
		seen[user] = struct{}{}
	}
	return validateLimits(r.DialTimeout.Duration, r.UDPIdleTimeout.Duration, r.MaxSessions)
}

func (f ClientForward) Validate() error {
	if strings.TrimSpace(f.Name) == "" {
		return errors.New("name is required")
	}
	if err := validateNetwork(f.Network); err != nil {
		return err
	}
	if err := validateHostPort(f.Listen, false); err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	if f.ServerUser.NodeID <= 0 || f.ServerUser.UserID <= 0 {
		return errors.New("server_user requires positive node_id and user_id")
	}
	if strings.TrimSpace(f.RemoteRule) == "" {
		return errors.New("remote_rule is required")
	}
	return validateLimits(f.HandshakeTimeout.Duration, f.UDPIdleTimeout.Duration, f.MaxSessions)
}

func validateNetwork(network string) error {
	if network != NetworkTCP && network != NetworkUDP {
		return errors.New("network must be tcp or udp")
	}
	return nil
}

func validateHostPort(address string, requireHost bool) error {
	host, portText, err := net.SplitHostPort(strings.TrimSpace(address))
	if err != nil {
		return err
	}
	if requireHost && strings.TrimSpace(host) == "" {
		return errors.New("host is required")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return errors.New("port must be between 1 and 65535")
	}
	return nil
}

func validateLimits(operationTimeout, udpIdleTimeout time.Duration, maxSessions int) error {
	if operationTimeout <= 0 {
		return errors.New("operation timeout must be positive")
	}
	if udpIdleTimeout <= 0 {
		return errors.New("udp_idle_timeout must be positive")
	}
	if maxSessions <= 0 {
		return errors.New("max_sessions must be positive")
	}
	return nil
}

func (c CredentialsConfig) ToTurntf() (turntf.Credentials, error) {
	password, err := c.Password.ToTurntf()
	if err != nil {
		return turntf.Credentials{}, err
	}
	credentials := turntf.Credentials{
		NodeID:    c.NodeID,
		UserID:    c.UserID,
		LoginName: strings.TrimSpace(c.LoginName),
		Password:  password,
	}
	hasIDs := credentials.NodeID != 0 || credentials.UserID != 0
	hasLoginName := credentials.LoginName != ""
	if hasIDs == hasLoginName {
		return turntf.Credentials{}, errors.New("set exactly one of node_id/user_id or login_name")
	}
	if hasIDs && (credentials.NodeID <= 0 || credentials.UserID <= 0) {
		return turntf.Credentials{}, errors.New("node_id and user_id must be positive and set together")
	}
	return credentials, nil
}

func (p PasswordConfig) ToTurntf() (turntf.PasswordInput, error) {
	switch strings.TrimSpace(p.Source) {
	case string(turntf.PasswordSourcePlain):
		// turntf authenticates the wire value as plaintext. HashedPassword is used
		// here only to prevent the SDK from bcrypt-hashing it before transport.
		password := turntf.HashedPassword(p.Value)
		if err := password.Validate(); err != nil {
			return turntf.PasswordInput{}, err
		}
		return password, nil
	default:
		return turntf.PasswordInput{}, errors.New("password.source must be plain")
	}
}
