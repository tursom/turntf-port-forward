package portforward

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	turntf "github.com/tursom/turntf-go"
	pb "github.com/tursom/turntf-port-forward/internal/proto"
)

const udpPendingDatagrams = 64

type serverUDPKey struct {
	user          turntf.UserRef
	session       turntf.SessionRef
	associationID string
}

type serverUDPAssociation struct {
	runtime *ServerRuntime
	key     serverUDPKey
	rule    *serverRuleRuntime
	session turntf.SessionRef
	conn    net.Conn
	last    atomic.Int64
	once    sync.Once
}

type serverUDPOpen struct {
	done     chan struct{}
	cancel   context.CancelFunc
	response *pb.OpenResponse
}

func (r *ServerRuntime) handlePacket(_ context.Context, packet turntf.Packet) {
	env, err := unmarshalPacketFrame(packet.Body)
	if err != nil {
		return
	}
	go r.dispatchUDPPacket(packet, env)
}

func (r *ServerRuntime) dispatchUDPPacket(packet turntf.Packet, env *pb.TunnelEnvelope) {
	if request := env.GetOpenRequest(); request != nil {
		r.handleUDPOpen(packet, env, request)
		return
	}
	if err := validateEnvelope(env); err != nil || len(env.GetAssociationId()) != associationIDSize {
		return
	}
	session := sessionRefFromProto(env.GetSourceSession())
	if !session.Valid() {
		return
	}
	key := serverUDPKey{user: packet.Sender, session: session, associationID: string(env.GetAssociationId())}
	r.udpMu.Lock()
	association := r.udp[key]
	r.udpMu.Unlock()
	if association == nil {
		return
	}
	if data := env.GetData(); data != nil {
		association.touch()
		if _, err := association.conn.Write(data.GetPayload()); err != nil {
			r.logf("UDP route %s target write: %v", association.rule.config.Name, err)
			association.close(true, "target_write_failed")
		}
		return
	}
	if env.GetClose() != nil {
		association.close(false, "remote_close")
	}
}

func (r *ServerRuntime) handleUDPOpen(packet turntf.Packet, env *pb.TunnelEnvelope, request *pb.OpenRequest) {
	session := sessionRefFromProto(request.GetSourceSession())
	if len(env.GetAssociationId()) != associationIDSize || session.ServingNodeID <= 0 || session.SessionID == "" {
		return
	}
	if protocolErr := validateEnvelope(env); protocolErr != nil {
		r.sendUDPResponse(packet.Sender, session, newOpenResponse(false, protocolErr.Code, protocolErr.Message, env.GetAssociationId()))
		return
	}
	if sourceSession := sessionRefFromProto(env.GetSourceSession()); sourceSession != session {
		r.sendUDPResponse(packet.Sender, session, newOpenResponse(false, errorInvalidRequest, "source session is unavailable", env.GetAssociationId()))
		return
	}
	if request.GetNetwork() != pb.Network_NETWORK_UDP {
		r.sendUDPResponse(packet.Sender, session, newOpenResponse(false, errorInvalidRequest, "open request must use UDP", env.GetAssociationId()))
		return
	}
	rule := r.authorizedRule(request.GetRule(), NetworkUDP, packet.Sender)
	if rule == nil {
		r.sendUDPResponse(packet.Sender, session, newOpenResponse(false, errorRouteUnavailable, "requested route is unavailable", env.GetAssociationId()))
		return
	}
	if !r.sessionBelongsTo(packet.Sender, session) {
		r.sendUDPResponse(packet.Sender, session, newOpenResponse(false, errorInvalidRequest, "source session is unavailable", env.GetAssociationId()))
		return
	}

	key := serverUDPKey{user: packet.Sender, session: session, associationID: string(env.GetAssociationId())}
	r.udpMu.Lock()
	existing := r.udp[key]
	if existing != nil {
		r.udpMu.Unlock()
		existing.touch()
		r.sendUDPResponse(packet.Sender, session, newOpenResponse(true, "", "", env.GetAssociationId()))
		return
	}
	if opening := r.udpOpening[key]; opening != nil {
		r.udpMu.Unlock()
		select {
		case <-opening.done:
			if opening.response != nil {
				r.sendUDPResponse(packet.Sender, session, &pb.TunnelEnvelope{
					ProtocolVersion: protocolVersion,
					AssociationId:   append([]byte(nil), env.GetAssociationId()...),
					Body:            &pb.TunnelEnvelope_OpenResponse{OpenResponse: opening.response},
				})
			}
		case <-r.ctx.Done():
		}
		return
	}
	if !rule.limit.acquire() {
		r.udpMu.Unlock()
		r.sendUDPResponse(packet.Sender, session, newOpenResponse(false, errorCapacityExceeded, "route capacity exceeded", env.GetAssociationId()))
		return
	}

	dialCtx, cancel := context.WithTimeout(r.ctx, rule.config.DialTimeout.Duration)
	opening := &serverUDPOpen{done: make(chan struct{}), cancel: cancel}
	if r.udpOpening == nil {
		r.udpOpening = make(map[serverUDPKey]*serverUDPOpen)
	}
	r.udpOpening[key] = opening
	r.udpMu.Unlock()
	dial := r.udpDial
	if dial == nil {
		dial = func(ctx context.Context, target string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, NetworkUDP, target)
		}
	}
	conn, err := dial(dialCtx, rule.config.Target)
	cancel()
	if err != nil {
		rule.limit.release()
		r.logf("UDP target %s: %v", rule.config.Name, err)
		response := newOpenResponse(false, errorTargetUnavailable, "target is unavailable", env.GetAssociationId())
		if r.completeUDPOpen(key, opening, response.GetOpenResponse()) {
			r.sendUDPResponse(packet.Sender, session, response)
		}
		return
	}
	association := &serverUDPAssociation{
		runtime: r,
		key:     key,
		rule:    rule,
		session: session,
		conn:    conn,
	}
	association.touch()
	installed := false
	r.udpMu.Lock()
	if r.udpOpening[key] == opening {
		delete(r.udpOpening, key)
		r.udp[key] = association
		opening.response = &pb.OpenResponse{Accepted: true}
		close(opening.done)
		existing = nil
		installed = true
	} else {
		existing = r.udp[key]
	}
	r.udpMu.Unlock()
	if existing != nil {
		_ = conn.Close()
		rule.limit.release()
		existing.touch()
		r.sendUDPResponse(packet.Sender, session, newOpenResponse(true, "", "", env.GetAssociationId()))
		return
	}
	if !installed {
		_ = conn.Close()
		rule.limit.release()
		return
	}
	r.logf("UDP route %s opened for %d:%d", rule.config.Name, packet.Sender.NodeID, packet.Sender.UserID)
	go association.run()
	if err := r.sendPacket(r.ctx, packet.Sender, session, newOpenResponse(true, "", "", env.GetAssociationId()), turntf.DeliveryModeRouteRetry); err != nil {
		association.close(false, "open_response_failed")
	}
}

func (r *ServerRuntime) completeUDPOpen(key serverUDPKey, opening *serverUDPOpen, response *pb.OpenResponse) bool {
	r.udpMu.Lock()
	defer r.udpMu.Unlock()
	if r.udpOpening[key] == opening {
		delete(r.udpOpening, key)
		opening.response = response
		close(opening.done)
		return true
	}
	return false
}

func (r *ServerRuntime) sessionBelongsTo(user turntf.UserRef, session turntf.SessionRef) bool {
	ctx, cancel := context.WithTimeout(r.ctx, r.cfg.Turntf.RequestTimeout.Duration)
	defer cancel()
	resolved, err := r.sessionResolver(ctx, user)
	if err != nil {
		return false
	}
	for _, candidate := range resolved.Sessions {
		if candidate.TransientCapable && candidate.Session == session {
			return true
		}
	}
	return false
}

func (r *ServerRuntime) sendUDPResponse(user turntf.UserRef, session turntf.SessionRef, env *pb.TunnelEnvelope) {
	if err := r.sendPacket(r.ctx, user, session, env, turntf.DeliveryModeRouteRetry); err != nil {
		r.logf("send UDP control response to %d:%d: %v", user.NodeID, user.UserID, err)
	}
}

func (r *ServerRuntime) sendPacket(ctx context.Context, user turntf.UserRef, session turntf.SessionRef, env *pb.TunnelEnvelope, mode turntf.DeliveryMode) error {
	frame, err := encodePacketFrame(env)
	if err != nil {
		return err
	}
	_, err = r.packetSender(ctx, turntf.SendPacketInput{
		Target:        user,
		TargetSession: session,
		DeliveryMode:  mode,
		Body:          frame,
	})
	return err
}

func (a *serverUDPAssociation) run() {
	buffer := make([]byte, 65535)
	for {
		_ = a.conn.SetReadDeadline(time.Now().Add(time.Second))
		n, err := a.conn.Read(buffer)
		if n > 0 {
			a.touch()
			env := newData(buffer[:n], []byte(a.key.associationID))
			if sendErr := a.runtime.sendPacket(a.runtime.ctx, a.key.user, a.session, env, turntf.DeliveryModeBestEffort); sendErr != nil {
				if a.runtime.ctx.Err() == nil {
					a.runtime.logf("UDP route %s response: %v", a.rule.config.Name, sendErr)
				}
				a.close(false, "send_failed")
				return
			}
		}
		if err == nil {
			continue
		}
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			if time.Since(time.Unix(0, a.last.Load())) >= a.rule.config.UDPIdleTimeout.Duration {
				a.close(true, "idle_timeout")
				return
			}
			continue
		}
		a.close(true, "target_read_failed")
		return
	}
}

func (a *serverUDPAssociation) touch() {
	a.last.Store(time.Now().UnixNano())
}

func (a *serverUDPAssociation) close(notify bool, code string) {
	a.once.Do(func() {
		a.runtime.udpMu.Lock()
		if a.runtime.udp[a.key] == a {
			delete(a.runtime.udp, a.key)
		}
		a.runtime.udpMu.Unlock()
		_ = a.conn.Close()
		a.rule.limit.release()
		if notify && a.runtime.ctx != nil && a.runtime.ctx.Err() == nil {
			go a.runtime.sendUDPResponse(a.key.user, a.session, newClose(code, []byte(a.key.associationID)))
		}
	})
}

func (r *ServerRuntime) closeUDPAssociations(notify bool) {
	r.udpMu.Lock()
	associations := make([]*serverUDPAssociation, 0, len(r.udp))
	for _, association := range r.udp {
		associations = append(associations, association)
	}
	openings := make([]*serverUDPOpen, 0, len(r.udpOpening))
	for key, opening := range r.udpOpening {
		delete(r.udpOpening, key)
		opening.response = nil
		close(opening.done)
		openings = append(openings, opening)
	}
	r.udpMu.Unlock()
	for _, opening := range openings {
		opening.cancel()
	}
	for _, association := range associations {
		association.close(notify, "shutdown")
	}
}

type clientUDPKey struct {
	server        turntf.UserRef
	associationID string
}

type clientUDPAssociation struct {
	runtime *ClientRuntime
	forward *clientForwardRuntime
	key     clientUDPKey
	id      []byte
	session turntf.SessionRef
	local   *net.UDPAddr
	queue   chan []byte
	opened  chan *pb.OpenResponse
	closed  chan struct{}
	last    atomic.Int64
	once    sync.Once
}

func (r *ClientRuntime) startUDPListener(forward *clientForwardRuntime) error {
	address, err := net.ResolveUDPAddr(NetworkUDP, forward.config.Listen)
	if err != nil {
		return err
	}
	listener, err := net.ListenUDP(NetworkUDP, address)
	if err != nil {
		return fmt.Errorf("listen UDP %s: %w", forward.config.Name, err)
	}
	forward.udpListener = listener
	r.wg.Add(1)
	go r.runUDPListener(forward)
	return nil
}

func (r *ClientRuntime) runUDPListener(forward *clientForwardRuntime) {
	defer r.wg.Done()
	r.logf("UDP forward %s listening on %s", forward.config.Name, forward.udpListener.LocalAddr())
	buffer := make([]byte, 65535)
	for {
		n, source, err := forward.udpListener.ReadFromUDP(buffer)
		if err != nil {
			if r.ctx.Err() == nil {
				r.logf("read UDP %s: %v", forward.config.Name, err)
			}
			return
		}
		association := r.clientUDPAssociation(forward, source)
		if association == nil {
			continue
		}
		association.touch()
		payload := append([]byte(nil), buffer[:n]...)
		r.enqueueUDPDatagram(association, payload)
	}
}

func (r *ClientRuntime) enqueueUDPDatagram(association *clientUDPAssociation, payload []byte) bool {
	select {
	case association.queue <- payload:
		return true
	default:
		r.logf("UDP forward %s dropped datagram while association queue is full", association.forward.config.Name)
		return false
	}
}

func (r *ClientRuntime) clientUDPAssociation(forward *clientForwardRuntime, source *net.UDPAddr) *clientUDPAssociation {
	localKey := source.String()
	forward.udpMu.Lock()
	association := forward.udpByLocal[localKey]
	forward.udpMu.Unlock()
	if association != nil {
		return association
	}
	login, ok := r.client.CurrentLogin()
	if !ok || !forward.limit.acquire() {
		return nil
	}
	id := make([]byte, associationIDSize)
	if _, err := rand.Read(id); err != nil {
		forward.limit.release()
		r.logf("generate UDP association ID: %v", err)
		return nil
	}
	server := forward.config.ServerUser.ToTurntf()
	association = &clientUDPAssociation{
		runtime: r,
		forward: forward,
		key:     clientUDPKey{server: server, associationID: string(id)},
		id:      id,
		session: login.SessionRef,
		local:   cloneUDPAddr(source),
		queue:   make(chan []byte, udpPendingDatagrams),
		opened:  make(chan *pb.OpenResponse, 1),
		closed:  make(chan struct{}),
	}
	association.touch()
	forward.udpMu.Lock()
	if existing := forward.udpByLocal[localKey]; existing != nil {
		forward.udpMu.Unlock()
		forward.limit.release()
		return existing
	}
	forward.udpByLocal[localKey] = association
	forward.udpMu.Unlock()
	r.udpMu.Lock()
	r.udp[association.key] = association
	r.udpMu.Unlock()
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		association.run()
	}()
	return association
}

func (a *clientUDPAssociation) run() {
	request := newOpenRequest(
		a.forward.config.RemoteRule,
		pb.Network_NETWORK_UDP,
		&pb.SessionRef{ServingNodeId: a.session.ServingNodeID, SessionId: a.session.SessionID},
		a.id,
	)
	deadline := time.NewTimer(a.forward.config.HandshakeTimeout.Duration)
	retry := time.NewTicker(time.Second)
	defer deadline.Stop()
	defer retry.Stop()
	if err := a.send(request, turntf.DeliveryModeRouteRetry); err != nil {
		a.close(false, "open_failed")
		return
	}
	for {
		select {
		case response := <-a.opened:
			if !response.GetAccepted() {
				a.runtime.logf("UDP forward %s rejected: %s", a.forward.config.Name, response.GetCode())
				a.close(false, response.GetCode())
				return
			}
			goto opened
		case <-retry.C:
			if err := a.send(request, turntf.DeliveryModeRouteRetry); err != nil {
				a.close(false, "open_failed")
				return
			}
		case <-deadline.C:
			a.close(false, "open_timeout")
			return
		case <-a.closed:
			return
		case <-a.runtime.ctx.Done():
			a.close(false, "shutdown")
			return
		}
	}

opened:
	idle := time.NewTicker(time.Second)
	defer idle.Stop()
	for {
		select {
		case payload := <-a.queue:
			if err := a.send(newData(payload, a.id), turntf.DeliveryModeBestEffort); err != nil {
				a.close(false, "send_failed")
				return
			}
		case <-idle.C:
			if time.Since(time.Unix(0, a.last.Load())) >= a.forward.config.UDPIdleTimeout.Duration {
				a.close(true, "idle_timeout")
				return
			}
		case <-a.closed:
			return
		case <-a.runtime.ctx.Done():
			a.close(false, "shutdown")
			return
		}
	}
}

func (a *clientUDPAssociation) send(env *pb.TunnelEnvelope, mode turntf.DeliveryMode) error {
	env.SourceSession = &pb.SessionRef{ServingNodeId: a.session.ServingNodeID, SessionId: a.session.SessionID}
	frame, err := encodePacketFrame(env)
	if err != nil {
		return err
	}
	_, err = a.runtime.packetSender(a.runtime.ctx, turntf.SendPacketInput{
		Target:        a.key.server,
		TargetSession: turntf.SessionRef{},
		DeliveryMode:  mode,
		Body:          frame,
	})
	return err
}

func (r *ClientRuntime) handlePacket(_ context.Context, packet turntf.Packet) {
	env, err := decodePacketFrame(packet.Body)
	if err != nil || len(env.GetAssociationId()) != associationIDSize {
		return
	}
	key := clientUDPKey{server: packet.Sender, associationID: string(env.GetAssociationId())}
	r.udpMu.Lock()
	association := r.udp[key]
	r.udpMu.Unlock()
	if association == nil {
		return
	}
	if response := env.GetOpenResponse(); response != nil {
		select {
		case association.opened <- response:
		default:
		}
		return
	}
	if data := env.GetData(); data != nil {
		association.touch()
		if _, err := association.forward.udpListener.WriteToUDP(data.GetPayload(), association.local); err != nil {
			r.logf("UDP forward %s local write: %v", association.forward.config.Name, err)
			association.close(true, "local_write_failed")
		}
		return
	}
	if env.GetClose() != nil {
		association.close(false, "remote_close")
	}
}

func (a *clientUDPAssociation) touch() {
	a.last.Store(time.Now().UnixNano())
}

func (a *clientUDPAssociation) close(notify bool, code string) {
	a.once.Do(func() {
		localKey := a.local.String()
		a.forward.udpMu.Lock()
		if a.forward.udpByLocal[localKey] == a {
			delete(a.forward.udpByLocal, localKey)
		}
		a.forward.udpMu.Unlock()
		a.runtime.udpMu.Lock()
		if a.runtime.udp[a.key] == a {
			delete(a.runtime.udp, a.key)
		}
		a.runtime.udpMu.Unlock()
		a.forward.limit.release()
		if notify && a.runtime.ctx != nil && a.runtime.ctx.Err() == nil {
			_ = a.send(newClose(code, a.id), turntf.DeliveryModeRouteRetry)
		}
		close(a.closed)
	})
}

func (r *ClientRuntime) closeUDPAssociations(notify bool) {
	r.udpMu.Lock()
	associations := make([]*clientUDPAssociation, 0, len(r.udp))
	for _, association := range r.udp {
		associations = append(associations, association)
	}
	r.udpMu.Unlock()
	for _, association := range associations {
		association.close(notify, "shutdown")
	}
}

func sessionRefFromProto(session *pb.SessionRef) turntf.SessionRef {
	if session == nil {
		return turntf.SessionRef{}
	}
	return turntf.SessionRef{ServingNodeID: session.GetServingNodeId(), SessionID: session.GetSessionId()}
}

func cloneUDPAddr(address *net.UDPAddr) *net.UDPAddr {
	if address == nil {
		return nil
	}
	return &net.UDPAddr{IP: append(net.IP(nil), address.IP...), Port: address.Port, Zone: address.Zone}
}
