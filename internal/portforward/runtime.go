package portforward

import (
	"context"
	"fmt"
	"sync"
	"time"

	turntf "github.com/tursom/turntf-go"
)

type Logger interface {
	Printf(format string, args ...any)
}

type runtimeHandler struct {
	onPacket     func(context.Context, turntf.Packet)
	onDisconnect func(error)
	logger       Logger
}

func (h *runtimeHandler) OnLogin(context.Context, turntf.LoginInfo) {}
func (h *runtimeHandler) OnMessage(context.Context, turntf.Message) {}

func (h *runtimeHandler) OnPacket(ctx context.Context, packet turntf.Packet) {
	if h.onPacket != nil {
		h.onPacket(ctx, packet)
	}
}

func (h *runtimeHandler) OnError(_ context.Context, err error) {
	if h.logger != nil {
		h.logger.Printf("turntf: %v", err)
	}
}

func (h *runtimeHandler) OnDisconnect(_ context.Context, err error) {
	if h.onDisconnect != nil {
		h.onDisconnect(err)
	}
}

func newTurntfClient(cfg TurntfConfig, handler turntf.Handler, logger Logger) (*turntf.Client, error) {
	credentials, err := cfg.Credentials.ToTurntf()
	if err != nil {
		return nil, err
	}
	return turntf.NewClient(turntf.Config{
		BaseURL:        cfg.BaseURL,
		Credentials:    credentials,
		CursorStore:    turntf.NewMemoryCursorStore(),
		Handler:        handler,
		Logger:         logger,
		RequestTimeout: cfg.RequestTimeout.Duration,
		PingInterval:   cfg.PingInterval.Duration,
		TransientOnly:  true,
		RealtimeStream: true,
	})
}

type sessionLimiter struct {
	semaphore chan struct{}
}

func newSessionLimiter(max int) *sessionLimiter {
	return &sessionLimiter{semaphore: make(chan struct{}, max)}
}

func (l *sessionLimiter) acquire() bool {
	select {
	case l.semaphore <- struct{}{}:
		return true
	default:
		return false
	}
}

func (l *sessionLimiter) release() {
	select {
	case <-l.semaphore:
	default:
		panic("portforward: session limiter released without acquisition")
	}
}

type closeGroup struct {
	mu      sync.Mutex
	closers map[any]func()
}

func (g *closeGroup) add(key any, closeFn func()) {
	g.mu.Lock()
	if g.closers == nil {
		g.closers = make(map[any]func())
	}
	g.closers[key] = closeFn
	g.mu.Unlock()
}

func (g *closeGroup) remove(key any) {
	g.mu.Lock()
	delete(g.closers, key)
	g.mu.Unlock()
}

func (g *closeGroup) closeAll() {
	g.mu.Lock()
	closers := make([]func(), 0, len(g.closers))
	for _, closeFn := range g.closers {
		closers = append(closers, closeFn)
	}
	g.mu.Unlock()
	for _, closeFn := range closers {
		closeFn()
	}
}

func waitForOpenResponse(conn relayStream, timeout time.Duration) error {
	frame, err := conn.ReceiveTimeout(timeout)
	if err != nil {
		return err
	}
	env, err := decodeTunnelFrame(frame)
	if err != nil {
		return err
	}
	response := env.GetOpenResponse()
	if response == nil {
		return fmt.Errorf("expected open_response, got %T", env.GetBody())
	}
	if !response.GetAccepted() {
		return &protocolError{Code: response.GetCode(), Message: response.GetMessage()}
	}
	return nil
}
