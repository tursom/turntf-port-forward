package portforward

import (
	"bytes"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	pb "github.com/tursom/turntf-port-forward/internal/proto"
)

const (
	protocolVersion   = "tunnel-v1alpha1"
	associationIDSize = 16

	errorInvalidRequest    = "invalid_request"
	errorProtocolMismatch  = "protocol_mismatch"
	errorRouteUnavailable  = "route_unavailable"
	errorTargetUnavailable = "target_unavailable"
	errorCapacityExceeded  = "capacity_exceeded"
)

var packetMagic = []byte{0x00, 'T', 'P', 'F', 0x01}

type protocolError struct {
	Code    string
	Message string
}

func (e *protocolError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func encodeTunnelFrame(env *pb.TunnelEnvelope) ([]byte, error) {
	if err := validateEnvelope(env); err != nil {
		return nil, err
	}
	return proto.Marshal(env)
}

func decodeTunnelFrame(data []byte) (*pb.TunnelEnvelope, error) {
	env, err := unmarshalTunnelFrame(data)
	if err != nil {
		return nil, err
	}
	if err := validateEnvelope(env); err != nil {
		return nil, err
	}
	return env, nil
}

func unmarshalTunnelFrame(data []byte) (*pb.TunnelEnvelope, error) {
	var env pb.TunnelEnvelope
	if err := proto.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("decode tunnel frame: %w", err)
	}
	return &env, nil
}

func encodePacketFrame(env *pb.TunnelEnvelope) ([]byte, error) {
	payload, err := encodeTunnelFrame(env)
	if err != nil {
		return nil, err
	}
	frame := make([]byte, 0, len(packetMagic)+len(payload))
	frame = append(frame, packetMagic...)
	frame = append(frame, payload...)
	return frame, nil
}

func decodePacketFrame(data []byte) (*pb.TunnelEnvelope, error) {
	env, err := unmarshalPacketFrame(data)
	if err != nil {
		return nil, err
	}
	if err := validateEnvelope(env); err != nil {
		return nil, err
	}
	return env, nil
}

func unmarshalPacketFrame(data []byte) (*pb.TunnelEnvelope, error) {
	if len(data) <= len(packetMagic) || !bytes.Equal(data[:len(packetMagic)], packetMagic) {
		return nil, errors.New("packet is not a port-forward frame")
	}
	return unmarshalTunnelFrame(data[len(packetMagic):])
}

func validateEnvelope(env *pb.TunnelEnvelope) *protocolError {
	if env == nil {
		return &protocolError{Code: errorInvalidRequest, Message: "envelope is required"}
	}
	if env.GetProtocolVersion() != protocolVersion {
		return &protocolError{
			Code:    errorProtocolMismatch,
			Message: fmt.Sprintf("unsupported protocol version %q", env.GetProtocolVersion()),
		}
	}
	if env.GetBody() == nil {
		return &protocolError{Code: errorInvalidRequest, Message: "envelope body is required"}
	}
	return nil
}

func newOpenRequest(rule string, network pb.Network, session *pb.SessionRef, associationID []byte) *pb.TunnelEnvelope {
	return &pb.TunnelEnvelope{
		ProtocolVersion: protocolVersion,
		AssociationId:   append([]byte(nil), associationID...),
		SourceSession:   cloneProtoSessionRef(session),
		Body: &pb.TunnelEnvelope_OpenRequest{OpenRequest: &pb.OpenRequest{
			Rule:          rule,
			Network:       network,
			SourceSession: session,
		}},
	}
}

func cloneProtoSessionRef(session *pb.SessionRef) *pb.SessionRef {
	if session == nil {
		return nil
	}
	return &pb.SessionRef{ServingNodeId: session.GetServingNodeId(), SessionId: session.GetSessionId()}
}

func newOpenResponse(accepted bool, code, message string, associationID []byte) *pb.TunnelEnvelope {
	return &pb.TunnelEnvelope{
		ProtocolVersion: protocolVersion,
		AssociationId:   append([]byte(nil), associationID...),
		Body: &pb.TunnelEnvelope_OpenResponse{OpenResponse: &pb.OpenResponse{
			Accepted: accepted,
			Code:     code,
			Message:  message,
		}},
	}
}

func newData(payload, associationID []byte) *pb.TunnelEnvelope {
	return &pb.TunnelEnvelope{
		ProtocolVersion: protocolVersion,
		AssociationId:   append([]byte(nil), associationID...),
		Body: &pb.TunnelEnvelope_Data{Data: &pb.Data{
			Payload: append([]byte(nil), payload...),
		}},
	}
}

func newHalfClose() *pb.TunnelEnvelope {
	return &pb.TunnelEnvelope{
		ProtocolVersion: protocolVersion,
		Body:            &pb.TunnelEnvelope_HalfClose{HalfClose: &pb.HalfClose{}},
	}
}

func newClose(code string, associationID []byte) *pb.TunnelEnvelope {
	return &pb.TunnelEnvelope{
		ProtocolVersion: protocolVersion,
		AssociationId:   append([]byte(nil), associationID...),
		Body:            &pb.TunnelEnvelope_Close{Close: &pb.Close{Code: code}},
	}
}
