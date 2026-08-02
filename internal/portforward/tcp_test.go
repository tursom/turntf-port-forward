package portforward

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	turntf "github.com/tursom/turntf-go"
	pb "github.com/tursom/turntf-port-forward/internal/proto"
	"google.golang.org/protobuf/proto"
)

type testRelayStream struct {
	sent      chan []byte
	received  chan []byte
	closed    chan struct{}
	closeOnce sync.Once
	remote    turntf.UserRef
}

type blockingReceiveRelayStream struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingReceiveRelayStream) Send([]byte) error { return errors.New("closed") }

func (s *blockingReceiveRelayStream) ReceiveTimeout(time.Duration) ([]byte, error) {
	s.once.Do(func() { close(s.started) })
	<-s.release
	return nil, errors.New("closed")
}

func (s *blockingReceiveRelayStream) Close() error { return nil }
func (s *blockingReceiveRelayStream) Abort(error)  {}

func newTestRelayStream() *testRelayStream {
	return &testRelayStream{
		sent:     make(chan []byte, 16),
		received: make(chan []byte, 16),
		closed:   make(chan struct{}),
	}
}

func (s *testRelayStream) Send(data []byte) error {
	select {
	case s.sent <- append([]byte(nil), data...):
		return nil
	case <-s.closed:
		return errors.New("closed")
	}
}

func (s *testRelayStream) ReceiveTimeout(timeout time.Duration) ([]byte, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case data := <-s.received:
		return data, nil
	case <-s.closed:
		return nil, errors.New("closed")
	case <-timer.C:
		return nil, errors.New("timeout")
	}
}

func (s *testRelayStream) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

func (s *testRelayStream) Abort(error) {
	s.closeOnce.Do(func() { close(s.closed) })
}

func (s *testRelayStream) RemotePeer() turntf.UserRef { return s.remote }

func TestBridgeTCPTransfersBothDirectionsAndHalfCloses(t *testing.T) {
	client, bridgeSide := tcpPair(t)
	defer client.Close()
	stream := newTestRelayStream()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- bridgeTCP(ctx, bridgeSide, stream)
	}()

	request := bytes.Repeat([]byte("request-data-"), 9000)
	writeDone := make(chan error, 1)
	go func() {
		if _, err := client.Write(request); err != nil {
			writeDone <- err
			return
		}
		writeDone <- client.CloseWrite()
	}()
	var forwardedRequest []byte
	for {
		frame := mustReceiveTunnelFrame(t, stream.sent)
		if data := frame.GetData(); data != nil {
			if len(data.GetPayload()) > tcpChunkBytes {
				t.Fatalf("TCP frame size = %d, want at most %d", len(data.GetPayload()), tcpChunkBytes)
			}
			forwardedRequest = append(forwardedRequest, data.GetPayload()...)
			continue
		}
		if frame.GetHalfClose() == nil {
			t.Fatalf("unexpected request frame %+v", frame)
		}
		break
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("write and half-close request: %v", err)
	}
	if !bytes.Equal(forwardedRequest, request) {
		t.Fatalf("forwarded request length = %d, want %d", len(forwardedRequest), len(request))
	}

	response := bytes.Repeat([]byte("response-data-"), 8500)
	for offset := 0; offset < len(response); offset += tcpChunkBytes {
		end := offset + tcpChunkBytes
		if end > len(response) {
			end = len(response)
		}
		frame, err := encodeTunnelFrame(newData(response[offset:end], nil))
		if err != nil {
			t.Fatalf("encode response: %v", err)
		}
		stream.received <- frame
	}
	halfClose, err := encodeTunnelFrame(newHalfClose())
	if err != nil {
		t.Fatalf("encode half close: %v", err)
	}
	stream.received <- halfClose

	got, err := io.ReadAll(client)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if !bytes.Equal(got, response) {
		t.Fatalf("response length = %d, want %d", len(got), len(response))
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("bridgeTCP: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("bridge did not finish after both half-closes")
	}
}

func TestBridgeTCPCancellationWaitsForBothCopyDirections(t *testing.T) {
	client, bridgeSide := tcpPair(t)
	defer client.Close()
	stream := &blockingReceiveRelayStream{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- bridgeTCP(ctx, bridgeSide, stream) }()

	select {
	case <-stream.started:
	case <-time.After(time.Second):
		t.Fatal("relay receive direction did not start")
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("bridge returned before receive direction exited: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(stream.release)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("bridge error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("bridge did not finish after both directions exited")
	}
}

func TestServerTCPRouteErrorsAreStableAndHideAuthorization(t *testing.T) {
	allowed := turntf.UserRef{NodeID: 4096, UserID: 1025}
	remote := turntf.UserRef{NodeID: 4096, UserID: 1026}
	rule := &serverRuleRuntime{
		config:  ServerRule{Name: "known", Network: NetworkTCP, Target: "127.0.0.1:1", DialTimeout: Duration{time.Second}},
		allowed: map[turntf.UserRef]struct{}{allowed: {}},
		limit:   newSessionLimiter(1),
	}
	runtime := &ServerRuntime{
		ctx:   context.Background(),
		rules: map[string]*serverRuleRuntime{"known": rule},
	}

	unknown := runTCPServerOpen(t, runtime, remote, newOpenRequest("missing", pb.Network_NETWORK_TCP, nil, nil))
	unauthorized := runTCPServerOpen(t, runtime, remote, newOpenRequest("known", pb.Network_NETWORK_TCP, nil, nil))
	if unknown.GetCode() != errorRouteUnavailable || unauthorized.GetCode() != errorRouteUnavailable {
		t.Fatalf("route errors = %q and %q, want %q", unknown.GetCode(), unauthorized.GetCode(), errorRouteUnavailable)
	}
	if unknown.GetMessage() != unauthorized.GetMessage() {
		t.Fatalf("unknown and unauthorized messages differ: %q != %q", unknown.GetMessage(), unauthorized.GetMessage())
	}

	badVersion := newOpenRequest("known", pb.Network_NETWORK_TCP, nil, nil)
	badVersion.ProtocolVersion = "tunnel-v0"
	protocolResponse := runTCPServerOpen(t, runtime, allowed, badVersion)
	if protocolResponse.GetCode() != errorProtocolMismatch {
		t.Fatalf("protocol response code = %q, want %q", protocolResponse.GetCode(), errorProtocolMismatch)
	}

	if !rule.limit.acquire() {
		t.Fatal("failed to occupy route capacity")
	}
	defer rule.limit.release()
	capacityResponse := runTCPServerOpen(t, runtime, allowed, newOpenRequest("known", pb.Network_NETWORK_TCP, nil, nil))
	if capacityResponse.GetCode() != errorCapacityExceeded {
		t.Fatalf("capacity response code = %q, want %q", capacityResponse.GetCode(), errorCapacityExceeded)
	}
}

func runTCPServerOpen(t *testing.T, runtime *ServerRuntime, remote turntf.UserRef, request *pb.TunnelEnvelope) *pb.OpenResponse {
	t.Helper()
	frame, err := proto.Marshal(request)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	stream := newTestRelayStream()
	stream.remote = remote
	stream.received <- frame
	stream.received <- []byte("close acknowledgement")
	runtime.handleTCPRelay(stream)
	response := mustReceiveTunnelFrame(t, stream.sent).GetOpenResponse()
	if response == nil || response.GetAccepted() {
		t.Fatalf("open response = %+v, want rejection", response)
	}
	return response
}

func tcpPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("listen TCP: %v", err)
	}
	defer listener.Close()

	accepted := make(chan *net.TCPConn, 1)
	go func() {
		conn, acceptErr := listener.AcceptTCP()
		if acceptErr == nil {
			accepted <- conn
		}
	}()
	client, err := net.DialTCP("tcp", nil, listener.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatalf("dial TCP: %v", err)
	}
	select {
	case server := <-accepted:
		return client, server
	case <-time.After(time.Second):
		client.Close()
		t.Fatal("timed out accepting TCP connection")
		return nil, nil
	}
}

func mustReceiveTunnelFrame(t *testing.T, frames <-chan []byte) *pb.TunnelEnvelope {
	t.Helper()
	select {
	case frame := <-frames:
		env, err := decodeTunnelFrame(frame)
		if err != nil {
			t.Fatalf("decode tunnel frame: %v", err)
		}
		return env
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for tunnel frame")
		return nil
	}
}
