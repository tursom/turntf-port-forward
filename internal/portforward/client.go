package portforward

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	turntf "github.com/tursom/turntf-go"
	pb "github.com/tursom/turntf-port-forward/internal/proto"
)

type clientForwardRuntime struct {
	config      ClientForward
	limit       *sessionLimiter
	tcpListener *net.TCPListener
	udpListener *net.UDPConn
	udpMu       sync.Mutex
	udpByLocal  map[string]*clientUDPAssociation
}

type ClientRuntime struct {
	cfg          ClientConfig
	logger       Logger
	client       *turntf.Client
	relay        *turntf.Relay
	packetSender func(context.Context, turntf.SendPacketInput) (turntf.RelayAccepted, error)
	forwards     []*clientForwardRuntime
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	udpMu        sync.Mutex
	udp          map[clientUDPKey]*clientUDPAssociation
}

func NewClientRuntime(cfg ClientConfig, logger Logger) (*ClientRuntime, error) {
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	handler := &runtimeHandler{logger: logger}
	client, err := newTurntfClient(cfg.Turntf, handler, logger)
	if err != nil {
		return nil, err
	}
	runtime := &ClientRuntime{
		cfg:    cfg,
		logger: logger,
		client: client,
		relay:  client.Relay(),
		udp:    make(map[clientUDPKey]*clientUDPAssociation),
	}
	runtime.packetSender = client.SendPacket
	for _, forward := range cfg.Forwards {
		runtime.forwards = append(runtime.forwards, &clientForwardRuntime{
			config:     forward,
			limit:      newSessionLimiter(forward.MaxSessions),
			udpByLocal: make(map[string]*clientUDPAssociation),
		})
	}
	handler.onPacket = runtime.handlePacket
	handler.onDisconnect = func(error) { runtime.closeUDPAssociations(false) }
	return runtime, nil
}

func (r *ClientRuntime) Run(ctx context.Context) error {
	r.ctx, r.cancel = context.WithCancel(ctx)
	defer r.cancel()
	defer r.client.Close()
	if err := r.client.Connect(r.ctx); err != nil {
		return err
	}
	if err := r.startListeners(); err != nil {
		r.closeListeners()
		return err
	}
	r.logf("client connected to turntf")
	<-r.ctx.Done()
	r.closeListeners()
	r.closeUDPAssociations(false)
	r.wg.Wait()
	return nil
}

func (r *ClientRuntime) startListeners() error {
	for _, forward := range r.forwards {
		switch forward.config.Network {
		case NetworkTCP:
			address, err := net.ResolveTCPAddr(NetworkTCP, forward.config.Listen)
			if err != nil {
				return err
			}
			listener, err := net.ListenTCP(NetworkTCP, address)
			if err != nil {
				return fmt.Errorf("listen TCP %s: %w", forward.config.Name, err)
			}
			forward.tcpListener = listener
			r.wg.Add(1)
			go r.runTCPListener(forward)
		case NetworkUDP:
			if err := r.startUDPListener(forward); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *ClientRuntime) runTCPListener(forward *clientForwardRuntime) {
	defer r.wg.Done()
	r.logf("TCP forward %s listening on %s", forward.config.Name, forward.tcpListener.Addr())
	for {
		conn, err := forward.tcpListener.AcceptTCP()
		if err != nil {
			if r.ctx.Err() == nil {
				r.logf("accept TCP %s: %v", forward.config.Name, err)
			}
			return
		}
		if !forward.limit.acquire() {
			r.logf("TCP forward %s capacity exceeded", forward.config.Name)
			_ = conn.Close()
			continue
		}
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			defer forward.limit.release()
			if err := r.handleTCPConnection(forward, conn); err != nil && !errors.Is(err, context.Canceled) {
				r.logf("TCP forward %s: %v", forward.config.Name, err)
			}
		}()
	}
}

func (r *ClientRuntime) handleTCPConnection(forward *clientForwardRuntime, local *net.TCPConn) error {
	defer local.Close()
	handshakeCtx, cancel := context.WithTimeout(r.ctx, forward.config.HandshakeTimeout.Duration)
	defer cancel()
	relayConfig := turntf.DefaultRelayConfig()
	relayConfig.Reliability = turntf.ReliabilityReliableOrdered
	relayConfig.DeliveryMode = turntf.DeliveryModeRouteRetry
	conn, err := r.relay.Connect(handshakeCtx, forward.config.ServerUser.ToTurntf(), &relayConfig)
	if err != nil {
		return err
	}
	request := newOpenRequest(forward.config.RemoteRule, pb.Network_NETWORK_TCP, nil, nil)
	if err := sendRelayEnvelope(conn, request); err != nil {
		conn.Abort(err)
		return err
	}
	if err := waitForOpenResponse(conn, forward.config.HandshakeTimeout.Duration); err != nil {
		conn.Abort(err)
		return err
	}
	return bridgeTCP(r.ctx, local, conn)
}

func (r *ClientRuntime) closeListeners() {
	for _, forward := range r.forwards {
		if forward.tcpListener != nil {
			_ = forward.tcpListener.Close()
		}
		if forward.udpListener != nil {
			_ = forward.udpListener.Close()
		}
	}
}

func (r *ClientRuntime) logf(format string, args ...any) {
	if r.logger != nil {
		r.logger.Printf(format, args...)
	}
}
