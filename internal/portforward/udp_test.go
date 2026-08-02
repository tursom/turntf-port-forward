package portforward

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	turntf "github.com/tursom/turntf-go"
	pb "github.com/tursom/turntf-port-forward/internal/proto"
)

func TestServerUDPAssociationsAreIsolatedBySourceSession(t *testing.T) {
	target, err := net.ListenUDP(NetworkUDP, &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("listen UDP target: %v", err)
	}
	defer target.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	remote := turntf.UserRef{NodeID: 4096, UserID: 1025}
	sessionA := turntf.SessionRef{ServingNodeID: 4096, SessionID: "session-a"}
	sessionB := turntf.SessionRef{ServingNodeID: 4096, SessionID: "session-b"}
	rule := &serverRuleRuntime{
		config: ServerRule{
			Name:           "dns",
			Network:        NetworkUDP,
			Target:         target.LocalAddr().String(),
			DialTimeout:    Duration{Duration: time.Second},
			UDPIdleTimeout: Duration{Duration: time.Minute},
		},
		allowed: map[turntf.UserRef]struct{}{remote: {}},
		limit:   newSessionLimiter(2),
	}
	responses := make(chan *pb.OpenResponse, 2)
	runtime := &ServerRuntime{
		ctx:   ctx,
		cfg:   ServerConfig{Turntf: TurntfConfig{RequestTimeout: Duration{Duration: time.Second}}},
		rules: map[string]*serverRuleRuntime{"dns": rule},
		udp:   make(map[serverUDPKey]*serverUDPAssociation),
	}
	runtime.packetSender = func(_ context.Context, input turntf.SendPacketInput) (turntf.RelayAccepted, error) {
		envelope, decodeErr := decodePacketFrame(input.Body)
		if decodeErr != nil {
			t.Errorf("decode response: %v", decodeErr)
			return turntf.RelayAccepted{}, nil
		}
		responses <- envelope.GetOpenResponse()
		return turntf.RelayAccepted{}, nil
	}
	runtime.sessionResolver = func(context.Context, turntf.UserRef) (turntf.ResolvedUserSessions, error) {
		return turntf.ResolvedUserSessions{Sessions: []turntf.ResolvedSession{
			{Session: sessionA, TransientCapable: true},
			{Session: sessionB, TransientCapable: true},
		}}, nil
	}

	id := []byte("same-id-016-byte")
	packet := turntf.Packet{Sender: remote}
	for _, session := range []turntf.SessionRef{sessionA, sessionB} {
		request := newOpenRequest("dns", pb.Network_NETWORK_UDP, &pb.SessionRef{
			ServingNodeId: session.ServingNodeID,
			SessionId:     session.SessionID,
		}, id)
		runtime.handleUDPOpen(packet, request, request.GetOpenRequest())
		select {
		case response := <-responses:
			if response == nil || !response.GetAccepted() {
				t.Fatalf("open response = %+v", response)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for open response")
		}
	}
	runtime.udpMu.Lock()
	associationCount := len(runtime.udp)
	runtime.udpMu.Unlock()
	if associationCount != 2 {
		t.Fatalf("association count = %d, want one per source session", associationCount)
	}
	runtime.closeUDPAssociations(false)
}

func TestServerUDPDuplicateOpenDuringDialReusesAssociation(t *testing.T) {
	target, err := net.ListenUDP(NetworkUDP, &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("listen UDP target: %v", err)
	}
	defer target.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	remote := turntf.UserRef{NodeID: 4096, UserID: 1025}
	session := turntf.SessionRef{ServingNodeID: 4096, SessionID: "session-a"}
	rule := &serverRuleRuntime{
		config: ServerRule{
			Name:           "dns",
			Network:        NetworkUDP,
			Target:         target.LocalAddr().String(),
			DialTimeout:    Duration{Duration: time.Second},
			UDPIdleTimeout: Duration{Duration: time.Minute},
		},
		allowed: map[turntf.UserRef]struct{}{remote: {}},
		limit:   newSessionLimiter(1),
	}
	responses := make(chan *pb.OpenResponse, 2)
	runtime := &ServerRuntime{
		ctx:        ctx,
		cfg:        ServerConfig{Turntf: TurntfConfig{RequestTimeout: Duration{Duration: time.Second}}},
		rules:      map[string]*serverRuleRuntime{"dns": rule},
		udp:        make(map[serverUDPKey]*serverUDPAssociation),
		udpOpening: make(map[serverUDPKey]*serverUDPOpen),
	}
	runtime.packetSender = func(_ context.Context, input turntf.SendPacketInput) (turntf.RelayAccepted, error) {
		envelope, decodeErr := decodePacketFrame(input.Body)
		if decodeErr != nil {
			t.Errorf("decode response: %v", decodeErr)
			return turntf.RelayAccepted{}, nil
		}
		if response := envelope.GetOpenResponse(); response != nil {
			responses <- response
		}
		return turntf.RelayAccepted{}, nil
	}
	runtime.sessionResolver = func(context.Context, turntf.UserRef) (turntf.ResolvedUserSessions, error) {
		return turntf.ResolvedUserSessions{Sessions: []turntf.ResolvedSession{{Session: session, TransientCapable: true}}}, nil
	}
	dialStarted := make(chan struct{})
	releaseDial := make(chan struct{})
	var dialMu sync.Mutex
	dialCount := 0
	runtime.udpDial = func(ctx context.Context, address string) (net.Conn, error) {
		dialMu.Lock()
		dialCount++
		if dialCount == 1 {
			close(dialStarted)
		}
		dialMu.Unlock()
		select {
		case <-releaseDial:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return (&net.Dialer{}).DialContext(ctx, NetworkUDP, address)
	}
	id := []byte("duplicate-open-1")
	request := newOpenRequest("dns", pb.Network_NETWORK_UDP, &pb.SessionRef{
		ServingNodeId: session.ServingNodeID,
		SessionId:     session.SessionID,
	}, id)
	packet := turntf.Packet{Sender: remote}
	var handlers sync.WaitGroup
	handlers.Add(2)
	go func() {
		defer handlers.Done()
		runtime.handleUDPOpen(packet, request, request.GetOpenRequest())
	}()
	<-dialStarted
	go func() {
		defer handlers.Done()
		runtime.handleUDPOpen(packet, request, request.GetOpenRequest())
	}()
	time.Sleep(50 * time.Millisecond)
	dialMu.Lock()
	gotDialCount := dialCount
	dialMu.Unlock()
	if gotDialCount != 1 {
		t.Fatalf("target dial count while opening = %d, want 1", gotDialCount)
	}
	close(releaseDial)
	handlers.Wait()
	for i := 0; i < 2; i++ {
		select {
		case response := <-responses:
			if !response.GetAccepted() {
				t.Fatalf("duplicate OPEN response = %+v, want accepted", response)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for duplicate OPEN response")
		}
	}
	runtime.udpMu.Lock()
	associationCount := len(runtime.udp)
	runtime.udpMu.Unlock()
	if associationCount != 1 {
		t.Fatalf("association count = %d, want 1", associationCount)
	}
	runtime.closeUDPAssociations(false)
}

func TestServerUDPRouteErrorsAreStableAndHideAuthorization(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	allowed := turntf.UserRef{NodeID: 4096, UserID: 1025}
	unauthorized := turntf.UserRef{NodeID: 4096, UserID: 1026}
	session := turntf.SessionRef{ServingNodeID: 4096, SessionID: "client-session"}
	rule := &serverRuleRuntime{
		config: ServerRule{
			Name:           "known",
			Network:        NetworkUDP,
			Target:         "127.0.0.1:1",
			DialTimeout:    Duration{Duration: time.Second},
			UDPIdleTimeout: Duration{Duration: time.Minute},
		},
		allowed: map[turntf.UserRef]struct{}{allowed: {}},
		limit:   newSessionLimiter(1),
	}
	responses := make(chan *pb.OpenResponse, 4)
	runtime := &ServerRuntime{
		ctx:   ctx,
		cfg:   ServerConfig{Turntf: TurntfConfig{RequestTimeout: Duration{Duration: time.Second}}},
		rules: map[string]*serverRuleRuntime{"known": rule},
		udp:   make(map[serverUDPKey]*serverUDPAssociation),
	}
	runtime.packetSender = func(_ context.Context, input turntf.SendPacketInput) (turntf.RelayAccepted, error) {
		envelope, err := decodePacketFrame(input.Body)
		if err != nil {
			t.Errorf("decode response: %v", err)
			return turntf.RelayAccepted{}, nil
		}
		responses <- envelope.GetOpenResponse()
		return turntf.RelayAccepted{}, nil
	}
	runtime.sessionResolver = func(context.Context, turntf.UserRef) (turntf.ResolvedUserSessions, error) {
		return turntf.ResolvedUserSessions{Sessions: []turntf.ResolvedSession{{Session: session, TransientCapable: true}}}, nil
	}

	unknown := runUDPServerOpen(t, runtime, responses, unauthorized, session, "missing", protocolVersion, 1)
	denied := runUDPServerOpen(t, runtime, responses, unauthorized, session, "known", protocolVersion, 2)
	if unknown.GetCode() != errorRouteUnavailable || denied.GetCode() != errorRouteUnavailable {
		t.Fatalf("route errors = %q and %q, want %q", unknown.GetCode(), denied.GetCode(), errorRouteUnavailable)
	}
	if unknown.GetMessage() != denied.GetMessage() {
		t.Fatalf("unknown and unauthorized messages differ: %q != %q", unknown.GetMessage(), denied.GetMessage())
	}

	protocolResponse := runUDPServerOpen(t, runtime, responses, allowed, session, "known", "tunnel-v0", 3)
	if protocolResponse.GetCode() != errorProtocolMismatch {
		t.Fatalf("protocol response code = %q, want %q", protocolResponse.GetCode(), errorProtocolMismatch)
	}
	if !rule.limit.acquire() {
		t.Fatal("failed to occupy route capacity")
	}
	defer rule.limit.release()
	capacityResponse := runUDPServerOpen(t, runtime, responses, allowed, session, "known", protocolVersion, 4)
	if capacityResponse.GetCode() != errorCapacityExceeded {
		t.Fatalf("capacity response code = %q, want %q", capacityResponse.GetCode(), errorCapacityExceeded)
	}
}

func runUDPServerOpen(
	t *testing.T,
	runtime *ServerRuntime,
	responses <-chan *pb.OpenResponse,
	remote turntf.UserRef,
	session turntf.SessionRef,
	rule, version string,
	idByte byte,
) *pb.OpenResponse {
	t.Helper()
	id := make([]byte, associationIDSize)
	for i := range id {
		id[i] = idByte
	}
	request := newOpenRequest(rule, pb.Network_NETWORK_UDP, &pb.SessionRef{
		ServingNodeId: session.ServingNodeID,
		SessionId:     session.SessionID,
	}, id)
	request.ProtocolVersion = version
	runtime.handleUDPOpen(turntf.Packet{Sender: remote}, request, request.GetOpenRequest())
	select {
	case response := <-responses:
		if response == nil || response.GetAccepted() {
			t.Fatalf("open response = %+v, want rejection", response)
		}
		return response
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for UDP rejection")
		return nil
	}
}

func TestUDPAssociationRetriesOpenAndUsesBestEffortForData(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := turntf.UserRef{NodeID: 8192, UserID: 1025}
	id := []byte("0123456789abcdef")
	forward := &clientForwardRuntime{
		config: ClientForward{
			Name:             "dns",
			Network:          NetworkUDP,
			RemoteRule:       "dns",
			HandshakeTimeout: Duration{Duration: 2500 * time.Millisecond},
			UDPIdleTimeout:   Duration{Duration: time.Minute},
		},
		limit:      newSessionLimiter(1),
		udpByLocal: make(map[string]*clientUDPAssociation),
	}
	if !forward.limit.acquire() {
		t.Fatal("failed to reserve association capacity")
	}
	runtime := &ClientRuntime{
		ctx: ctx,
		udp: make(map[clientUDPKey]*clientUDPAssociation),
	}
	association := &clientUDPAssociation{
		runtime: runtime,
		forward: forward,
		key: clientUDPKey{
			server:        server,
			associationID: string(id),
		},
		id:      append([]byte(nil), id...),
		session: turntf.SessionRef{ServingNodeID: 4096, SessionID: "client-session"},
		queue:   make(chan []byte, udpPendingDatagrams),
		opened:  make(chan *pb.OpenResponse, 1),
		closed:  make(chan struct{}),
	}
	association.touch()
	runtime.udp[association.key] = association

	var mu sync.Mutex
	var opens int
	dataSent := make(chan *pb.TunnelEnvelope, 3)
	runtime.packetSender = func(_ context.Context, input turntf.SendPacketInput) (turntf.RelayAccepted, error) {
		envelope, err := decodePacketFrame(input.Body)
		if err != nil {
			t.Errorf("decode packet: %v", err)
			return turntf.RelayAccepted{}, nil
		}
		if envelope.GetOpenRequest() != nil {
			if input.DeliveryMode != turntf.DeliveryModeRouteRetry {
				t.Errorf("OPEN delivery mode = %q", input.DeliveryMode)
			}
			mu.Lock()
			opens++
			count := opens
			mu.Unlock()
			if count == 2 {
				association.opened <- &pb.OpenResponse{Accepted: true}
			}
		} else if envelope.GetData() != nil {
			if input.DeliveryMode != turntf.DeliveryModeBestEffort {
				t.Errorf("DATA delivery mode = %q", input.DeliveryMode)
			}
			dataSent <- envelope
		}
		return turntf.RelayAccepted{}, nil
	}

	done := make(chan struct{})
	go func() {
		association.run()
		close(done)
	}()
	datagrams := []string{"first", "second-datagram", "third"}
	for _, datagram := range datagrams {
		association.queue <- []byte(datagram)
	}
	for _, want := range datagrams {
		select {
		case envelope := <-dataSent:
			if string(envelope.GetData().GetPayload()) != want {
				t.Fatalf("DATA payload = %q, want %q", envelope.GetData().GetPayload(), want)
			}
			if string(envelope.GetAssociationId()) != string(id) {
				t.Fatalf("DATA association ID = %x", envelope.GetAssociationId())
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for UDP datagram %q", want)
		}
	}
	mu.Lock()
	gotOpens := opens
	mu.Unlock()
	if gotOpens != 2 {
		t.Fatalf("OPEN attempts = %d, want 2", gotOpens)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("association did not stop after cancellation")
	}
}

type recordingLogger struct {
	mu      sync.Mutex
	entries []string
}

func (l *recordingLogger) Printf(format string, args ...any) {
	l.mu.Lock()
	l.entries = append(l.entries, format)
	l.mu.Unlock()
}

func TestUDPAssociationQueueFullDropsAndLogs(t *testing.T) {
	logger := &recordingLogger{}
	runtime := &ClientRuntime{logger: logger}
	association := &clientUDPAssociation{
		forward: &clientForwardRuntime{config: ClientForward{Name: "dns"}},
		queue:   make(chan []byte, udpPendingDatagrams),
	}
	for i := 0; i < udpPendingDatagrams; i++ {
		if !runtime.enqueueUDPDatagram(association, []byte{byte(i)}) {
			t.Fatalf("datagram %d was dropped before the queue was full", i)
		}
	}
	if runtime.enqueueUDPDatagram(association, []byte("overflow")) {
		t.Fatal("overflow datagram was queued")
	}
	if len(association.queue) != udpPendingDatagrams {
		t.Fatalf("queue length = %d, want %d", len(association.queue), udpPendingDatagrams)
	}
	logger.mu.Lock()
	defer logger.mu.Unlock()
	if len(logger.entries) != 1 || !strings.Contains(logger.entries[0], "queue is full") {
		t.Fatalf("log entries = %v, want one queue-full message", logger.entries)
	}
}
