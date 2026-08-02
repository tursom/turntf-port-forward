package portforward

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	turntf "github.com/tursom/turntf-go"
)

const tcpChunkBytes = 32 << 10

type relayStream interface {
	Send([]byte) error
	ReceiveTimeout(time.Duration) ([]byte, error)
	Close() error
	Abort(error)
}

type relayPeerStream interface {
	relayStream
	RemotePeer() turntf.UserRef
}

func bridgeTCP(ctx context.Context, socket *net.TCPConn, relay relayStream) error {
	results := make(chan error, 2)
	go func() { results <- copyTCPToRelay(socket, relay) }()
	go func() { results <- copyRelayToTCP(ctx, relay, socket) }()

	var firstErr error
	ctxDone := ctx.Done()
	for completed := 0; completed < 2; completed++ {
		select {
		case err := <-results:
			if err != nil && firstErr == nil {
				firstErr = err
				_ = socket.Close()
				relay.Abort(err)
			}
		case <-ctxDone:
			if firstErr == nil {
				firstErr = ctx.Err()
			}
			_ = socket.Close()
			relay.Abort(ctx.Err())
			ctxDone = nil
			completed--
		}
	}

	_ = socket.Close()
	if firstErr != nil {
		return firstErr
	}
	return relay.Close()
}

func copyTCPToRelay(socket *net.TCPConn, relay relayStream) error {
	buffer := make([]byte, tcpChunkBytes)
	for {
		n, err := socket.Read(buffer)
		if n > 0 {
			frame, encodeErr := encodeTunnelFrame(newData(buffer[:n], nil))
			if encodeErr != nil {
				return encodeErr
			}
			if sendErr := relay.Send(frame); sendErr != nil {
				return sendErr
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				frame, encodeErr := encodeTunnelFrame(newHalfClose())
				if encodeErr != nil {
					return encodeErr
				}
				return relay.Send(frame)
			}
			return err
		}
	}
}

func copyRelayToTCP(ctx context.Context, relay relayStream, socket *net.TCPConn) error {
	for {
		frame, err := relay.ReceiveTimeout(time.Second)
		if err != nil {
			if isRelayReceiveTimeout(err) {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					continue
				}
			}
			return err
		}
		env, err := decodeTunnelFrame(frame)
		if err != nil {
			return err
		}
		if data := env.GetData(); data != nil {
			if err := writeAll(socket, data.GetPayload()); err != nil {
				return err
			}
			continue
		}
		if env.GetHalfClose() != nil {
			return socket.CloseWrite()
		}
		return fmt.Errorf("unexpected TCP tunnel frame %T", env.GetBody())
	}
}

func isRelayReceiveTimeout(err error) bool {
	var relayErr *turntf.RelayError
	return errors.As(err, &relayErr) && relayErr.Code == turntf.RelayErrorReceiveTimeout
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
