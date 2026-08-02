package portforward

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	turntf "github.com/tursom/turntf-go"
	pb "github.com/tursom/turntf-port-forward/internal/proto"
)

type serverRuleRuntime struct {
	config  ServerRule
	allowed map[turntf.UserRef]struct{}
	limit   *sessionLimiter
}

type ServerRuntime struct {
	cfg             ServerConfig
	logger          Logger
	client          *turntf.Client
	relay           *turntf.Relay
	packetSender    func(context.Context, turntf.SendPacketInput) (turntf.RelayAccepted, error)
	sessionResolver func(context.Context, turntf.UserRef) (turntf.ResolvedUserSessions, error)
	udpDial         func(context.Context, string) (net.Conn, error)
	rules           map[string]*serverRuleRuntime
	ctx             context.Context
	cancel          context.CancelFunc
	active          closeGroup
	udpMu           sync.Mutex
	udp             map[serverUDPKey]*serverUDPAssociation
	udpOpening      map[serverUDPKey]*serverUDPOpen
}

func NewServerRuntime(cfg ServerConfig, logger Logger) (*ServerRuntime, error) {
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	handler := &runtimeHandler{logger: logger}
	client, err := newTurntfClient(cfg.Turntf, handler, logger)
	if err != nil {
		return nil, err
	}
	runtime := &ServerRuntime{
		cfg:        cfg,
		logger:     logger,
		client:     client,
		relay:      client.Relay(),
		rules:      make(map[string]*serverRuleRuntime, len(cfg.Rules)),
		udp:        make(map[serverUDPKey]*serverUDPAssociation),
		udpOpening: make(map[serverUDPKey]*serverUDPOpen),
	}
	runtime.packetSender = client.SendPacket
	runtime.sessionResolver = client.ResolveUserSessions
	runtime.udpDial = func(ctx context.Context, target string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, NetworkUDP, target)
	}
	for _, rule := range cfg.Rules {
		allowed := make(map[turntf.UserRef]struct{}, len(rule.AllowedClients))
		for _, user := range rule.AllowedClients {
			allowed[user.ToTurntf()] = struct{}{}
		}
		runtime.rules[rule.Name] = &serverRuleRuntime{
			config:  rule,
			allowed: allowed,
			limit:   newSessionLimiter(rule.MaxSessions),
		}
	}
	handler.onPacket = runtime.handlePacket
	handler.onDisconnect = func(error) { runtime.closeUDPAssociations(false) }
	return runtime, nil
}

func (r *ServerRuntime) Run(ctx context.Context) error {
	r.ctx, r.cancel = context.WithCancel(ctx)
	defer r.cancel()
	defer r.client.Close()
	r.relay.OnConnection(func(conn *turntf.RelayConnection) {
		go r.handleTCPRelay(conn)
	})
	if err := r.client.Connect(r.ctx); err != nil {
		return err
	}
	r.logf("server connected to turntf")
	<-r.ctx.Done()
	r.active.closeAll()
	r.closeUDPAssociations(false)
	return nil
}

func (r *ServerRuntime) handleTCPRelay(conn relayPeerStream) {
	r.active.add(conn, func() { conn.Abort(context.Canceled) })
	defer r.active.remove(conn)

	frame, err := conn.ReceiveTimeout(10 * time.Second)
	if err != nil {
		conn.Abort(err)
		return
	}
	env, err := unmarshalTunnelFrame(frame)
	if err != nil {
		r.rejectTCP(conn, errorInvalidRequest, "invalid tunnel request")
		return
	}
	if protocolErr := validateEnvelope(env); protocolErr != nil {
		r.rejectTCP(conn, protocolErr.Code, protocolErr.Message)
		return
	}
	request := env.GetOpenRequest()
	if request == nil || request.GetNetwork() != pb.Network_NETWORK_TCP {
		r.rejectTCP(conn, errorInvalidRequest, "first frame must open a TCP route")
		return
	}
	rule := r.authorizedRule(request.GetRule(), NetworkTCP, conn.RemotePeer())
	if rule == nil {
		r.rejectTCP(conn, errorRouteUnavailable, "requested route is unavailable")
		return
	}
	if !rule.limit.acquire() {
		r.rejectTCP(conn, errorCapacityExceeded, "route capacity exceeded")
		return
	}
	defer rule.limit.release()

	dialCtx, cancel := context.WithTimeout(r.ctx, rule.config.DialTimeout.Duration)
	defer cancel()
	target, err := (&net.Dialer{}).DialContext(dialCtx, NetworkTCP, rule.config.Target)
	if err != nil {
		r.logf("TCP target %s: %v", rule.config.Name, err)
		r.rejectTCP(conn, errorTargetUnavailable, "target is unavailable")
		return
	}
	tcpTarget, ok := target.(*net.TCPConn)
	if !ok {
		_ = target.Close()
		r.rejectTCP(conn, errorTargetUnavailable, "target is unavailable")
		return
	}
	if err := sendRelayEnvelope(conn, newOpenResponse(true, "", "", nil)); err != nil {
		_ = tcpTarget.Close()
		conn.Abort(err)
		return
	}
	r.logf("TCP route %s opened for %d:%d", rule.config.Name, conn.RemotePeer().NodeID, conn.RemotePeer().UserID)
	if err := bridgeTCP(r.ctx, tcpTarget, conn); err != nil && !errors.Is(err, context.Canceled) {
		r.logf("TCP route %s closed: %v", rule.config.Name, err)
	}
}

func (r *ServerRuntime) authorizedRule(name, network string, user turntf.UserRef) *serverRuleRuntime {
	rule := r.rules[name]
	if rule == nil || rule.config.Network != network {
		return nil
	}
	if _, allowed := rule.allowed[user]; !allowed {
		return nil
	}
	return rule
}

func (r *ServerRuntime) rejectTCP(conn relayStream, code, message string) {
	if err := sendRelayEnvelope(conn, newOpenResponse(false, code, message, nil)); err == nil {
		_, _ = conn.ReceiveTimeout(time.Second)
	}
	_ = conn.Close()
}

func sendRelayEnvelope(conn relayStream, env *pb.TunnelEnvelope) error {
	frame, err := encodeTunnelFrame(env)
	if err != nil {
		return err
	}
	return conn.Send(frame)
}

func (r *ServerRuntime) logf(format string, args ...any) {
	if r.logger != nil {
		r.logger.Printf(format, args...)
	}
}
