package portforward

import (
	"bytes"
	"testing"

	pb "github.com/tursom/turntf-port-forward/internal/proto"
)

func TestPacketFrameRoundTripUsesRelayProofPrefix(t *testing.T) {
	want := &pb.TunnelEnvelope{
		ProtocolVersion: protocolVersion,
		AssociationId:   bytes.Repeat([]byte{0x42}, associationIDSize),
		Body:            &pb.TunnelEnvelope_Data{Data: &pb.Data{Payload: []byte("dns datagram")}},
	}

	encoded, err := encodePacketFrame(want)
	if err != nil {
		t.Fatalf("encodePacketFrame: %v", err)
	}
	if !bytes.HasPrefix(encoded, packetMagic) {
		t.Fatalf("packet frame prefix = %x", encoded[:len(packetMagic)])
	}
	if encoded[0] != 0 {
		t.Fatalf("first byte = %#x, want illegal protobuf field prefix", encoded[0])
	}

	got, err := decodePacketFrame(encoded)
	if err != nil {
		t.Fatalf("decodePacketFrame: %v", err)
	}
	if !bytes.Equal(got.GetAssociationId(), want.GetAssociationId()) || string(got.GetData().GetPayload()) != "dns datagram" {
		t.Fatalf("decoded envelope = %+v", got)
	}
}

func TestPacketFrameRejectsMissingMagic(t *testing.T) {
	if _, err := decodePacketFrame([]byte{0x0a, 0x01, 'x'}); err == nil {
		t.Fatal("expected missing magic error")
	}
}

func TestValidateEnvelopeRejectsProtocolBeforeBody(t *testing.T) {
	env := &pb.TunnelEnvelope{
		ProtocolVersion: "tunnel-v0",
		Body:            &pb.TunnelEnvelope_OpenRequest{OpenRequest: &pb.OpenRequest{Rule: "ssh", Network: pb.Network_NETWORK_TCP}},
	}
	err := validateEnvelope(env)
	if err == nil || err.Code != errorProtocolMismatch {
		t.Fatalf("error = %+v, want protocol mismatch", err)
	}
}
