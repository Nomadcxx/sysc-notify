package presenter

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Nomadcxx/sysc-notify/internal/history"
	"github.com/Nomadcxx/sysc-notify/internal/notify"
	"github.com/Nomadcxx/sysc-notify/internal/state"
	"github.com/Nomadcxx/sysc-notify/protocol"
)

func TestConnectionQueueBoundsCloseOnlyConnection(t *testing.T) {
	messageBound := newConnection(nil, 1)
	for range protocol.MaxPresenterQueueMessages {
		if !messageBound.prepare(outbound{data: []byte{1}, sequence: 1}) {
			t.Fatal("queue closed at the documented message bound")
		}
	}
	if messageBound.prepare(outbound{data: []byte{1}, sequence: 1}) {
		t.Fatal("queue accepted message above bound")
	}
	assertClosed(t, messageBound.closed)

	byteBound := newConnection(nil, 2)
	large := make([]byte, protocol.MaxFrameSize)
	for range 2 {
		if !byteBound.prepare(outbound{data: large, sequence: 1}) {
			t.Fatal("queue closed at the documented byte bound")
		}
	}
	if byteBound.prepare(outbound{data: []byte{1}, sequence: 1}) {
		t.Fatal("queue accepted bytes above bound")
	}
	assertClosed(t, byteBound.closed)
}

func TestCommandsReceiveMatchingRepliesAndDuplicatesClose(t *testing.T) {
	clock := newPresenterClock()
	store, err := history.OpenAt(t.TempDir(), clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	h := startHistoryHarness(t, clock, store)
	closedID := addCandidate(t, h.owner, "history", time.Minute)
	doState(t, h.owner, state.Command{Kind: state.Dismiss, ID: closedID})
	action := notify.Candidate{
		Summary: "action", Urgency: protocol.UrgencyNormal, Resident: true,
		Actions: []protocol.Action{{Key: "open", Label: "Open"}},
	}
	actionID := addFullCandidate(t, h.owner, action)
	replyCandidate := notify.Candidate{Summary: "reply", Urgency: protocol.UrgencyNormal, Resident: true, InlineReply: true}
	replyID := addFullCandidate(t, h.owner, replyCandidate)

	client := connectPresenter(t, h.server.SocketPath())
	defer client.conn.Close()
	commands := []protocol.Command{
		{Kind: protocol.CommandHistoryMarkSeen, IDs: []uint32{closedID}},
		{Kind: protocol.CommandPresentationRenew, Presentations: []protocol.Presentation{{ID: actionID, State: protocol.PresentationVisible}}},
		{Kind: protocol.CommandAction, ID: actionID, ActionKey: "open"},
		{Kind: protocol.CommandReply, ID: replyID, Text: "answer"},
		{Kind: protocol.CommandHistoryClear},
		{Kind: protocol.CommandDismiss, ID: actionID},
		{Kind: protocol.CommandDismissAll},
	}
	for i, command := range commands {
		requestID := uint64(i + 1)
		if err := writeEnvelope(client.conn, protocol.KindCommand, requestID, 0, command); err != nil {
			t.Fatal(err)
		}
		reply := readReply(t, client.conn, requestID)
		if !reply.OK || reply.Error != nil {
			t.Fatalf("reply %d = %#v", requestID, reply)
		}
	}
	snapshot, err := h.owner.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Active) != 0 || len(snapshot.History) == 0 {
		t.Fatalf("command result snapshot = %#v", snapshot)
	}

	if err := writeEnvelope(client.conn, protocol.KindCommand, uint64(len(commands)), 0, protocol.Command{Kind: protocol.CommandDismissAll}); err != nil {
		t.Fatal(err)
	}
	if err := client.conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := protocol.ReadFrame(client.conn); err == nil {
		t.Fatal("duplicate request ID remained connected")
	}
}

func TestMalformedMessagesAndSequenceGapsReconnectCleanly(t *testing.T) {
	for name, send := range map[string]func(*testing.T, *net.UnixConn){
		"duplicate JSON": func(t *testing.T, conn *net.UnixConn) {
			if err := protocol.WriteFrame(conn, []byte(`{"kind":"command","request_id":1,"request_id":2,"payload":{}}`)); err != nil {
				t.Fatal(err)
			}
		},
		"unknown kind": func(t *testing.T, conn *net.UnixConn) {
			if err := writeEnvelope(conn, "future", 1, 0, map[string]any{}); err != nil {
				t.Fatal(err)
			}
		},
		"stale sequence": func(t *testing.T, conn *net.UnixConn) {
			if err := writeEnvelope(conn, protocol.KindCommand, 1, 1, protocol.Command{Kind: protocol.CommandDismissAll}); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := startServerHarness(t, nil)
			client := connectPresenter(t, h.server.SocketPath())
			send(t, client.conn)
			assertSocketClosed(t, client.conn)
			reconnected := connectPresenter(t, h.server.SocketPath())
			_ = reconnected.conn.Close()
		})
	}

	h := startServerHarness(t, nil)
	client := connectPresenter(t, h.server.SocketPath())
	gap := state.Event{
		Sequence: client.snapshot.Sequence + 2,
		Delta:    &protocol.Delta{Kind: protocol.DeltaClosed, ID: 1, CloseReason: protocol.CloseUndefined},
	}
	if h.server.Publish(gap) {
		t.Fatal("sequence gap was accepted")
	}
	assertSocketClosed(t, client.conn)
	reconnected := connectPresenter(t, h.server.SocketPath())
	_ = reconnected.conn.Close()
}

func TestDisconnectAndReplacementReleasePresentationLease(t *testing.T) {
	for name, release := range map[string]func(*testing.T, serverHarness, *net.UnixConn){
		"disconnect": func(t *testing.T, h serverHarness, conn *net.UnixConn) {
			_ = conn.Close()
			waitNoPresenter(t, h.server)
		},
		"replacement": func(t *testing.T, h serverHarness, old *net.UnixConn) {
			replacement := connectPresenter(t, h.server.SocketPath())
			assertSocketClosed(t, old)
			_ = replacement.conn.Close()
			waitNoPresenter(t, h.server)
		},
	} {
		t.Run(name, func(t *testing.T) {
			clock := newPresenterClock()
			h := startServerHarness(t, clock)
			id := addCandidate(t, h.owner, name, 10*time.Second)
			client := connectPresenter(t, h.server.SocketPath())
			command := protocol.Command{
				Kind:          protocol.CommandPresentationRenew,
				Presentations: []protocol.Presentation{{ID: id, State: protocol.PresentationQueued}},
			}
			if err := writeEnvelope(client.conn, protocol.KindCommand, 1, 0, command); err != nil {
				t.Fatal(err)
			}
			if reply := readReply(t, client.conn, 1); !reply.OK {
				t.Fatalf("presentation reply = %#v", reply)
			}
			release(t, h, client.conn)
			clock.Advance(10 * time.Second)
			waitForInactive(t, h.owner, id)
		})
	}
}

func TestReconnectGetsFreshActiveAndHistorySnapshot(t *testing.T) {
	clock := newPresenterClock()
	store, err := history.OpenAt(t.TempDir(), clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	h := startHistoryHarness(t, clock, store)
	historyID := addCandidate(t, h.owner, "closed", time.Minute)
	doState(t, h.owner, state.Command{Kind: state.Dismiss, ID: historyID})
	first := connectPresenter(t, h.server.SocketPath())
	if len(first.snapshot.Active) != 0 || !historyIDs(first.snapshot, historyID) {
		t.Fatalf("first snapshot = %#v", first.snapshot)
	}
	_ = first.conn.Close()
	activeID := addCandidate(t, h.owner, "active", 0)
	second := connectPresenter(t, h.server.SocketPath())
	defer second.conn.Close()
	if !activeIDs(second.snapshot, activeID) || !historyIDs(second.snapshot, historyID) {
		t.Fatalf("reconnect snapshot = %#v", second.snapshot)
	}
}

func startHistoryHarness(t *testing.T, clock state.Clock, store *history.Store) serverHarness {
	t.Helper()
	runtimeDir := privateTempDir(t)
	server := NewAt(runtimeDir)
	owner := state.StartWithHistory(clock, server, store)
	if err := server.Serve(owner); err != nil {
		_ = owner.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = server.Close()
		_ = owner.Close()
	})
	return serverHarness{server: server, owner: owner, runtimeDir: runtimeDir}
}

func readReply(t *testing.T, conn net.Conn, requestID uint64) protocol.Reply {
	t.Helper()
	for {
		envelope := readEnvelope(t, conn)
		if envelope.Kind != protocol.KindReply {
			continue
		}
		if envelope.RequestID != requestID {
			t.Fatalf("reply request ID = %d, want %d", envelope.RequestID, requestID)
		}
		var reply protocol.Reply
		decodePayload(t, envelope, &reply)
		if err := reply.Validate(); err != nil {
			t.Fatal(err)
		}
		return reply
	}
}

func assertClosed(t *testing.T, closed <-chan struct{}) {
	t.Helper()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("connection did not close")
	}
}

func assertSocketClosed(t *testing.T, conn *net.UnixConn) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := protocol.ReadFrame(conn); err == nil {
		t.Fatal("socket remained open")
	}
	_ = conn.Close()
}

func doState(t *testing.T, owner *state.Owner, command state.Command) {
	t.Helper()
	if _, err := owner.Do(context.Background(), command); err != nil {
		t.Fatal(err)
	}
}

func addFullCandidate(t *testing.T, owner *state.Owner, candidate notify.Candidate) uint32 {
	t.Helper()
	result, err := owner.Do(context.Background(), state.Command{Kind: state.Add, Candidate: candidate})
	if err != nil {
		t.Fatal(err)
	}
	return result.ID
}

func activeIDs(snapshot protocol.Snapshot, ids ...uint32) bool {
	for _, id := range ids {
		found := false
		for _, record := range snapshot.Active {
			found = found || record.ID == id
		}
		if !found {
			return false
		}
	}
	return true
}

func historyIDs(snapshot protocol.Snapshot, ids ...uint32) bool {
	for _, id := range ids {
		found := false
		for _, record := range snapshot.History {
			found = found || record.ID == id
		}
		if !found {
			return false
		}
	}
	return true
}

func waitNoPresenter(t *testing.T, server *Server) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		server.mu.Lock()
		current := server.current
		server.mu.Unlock()
		if current == nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("presenter generation did not disconnect")
}

func waitForInactive(t *testing.T, owner *state.Owner, id uint32) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		snapshot, err := owner.Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !activeIDs(snapshot, id) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("notification %d remains active", id)
}

type presenterClock struct {
	mu     sync.Mutex
	now    time.Time
	timers map[*presenterTimer]struct{}
}

func newPresenterClock() *presenterClock {
	return &presenterClock{now: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), timers: make(map[*presenterTimer]struct{})}
}

func (c *presenterClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *presenterClock) NewTimer(duration time.Duration) state.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := &presenterTimer{clock: c, at: c.now.Add(duration), ch: make(chan time.Time, 1)}
	c.timers[timer] = struct{}{}
	return timer
}

func (c *presenterClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	now := c.now
	for timer := range c.timers {
		if !timer.stopped && !timer.at.After(now) {
			timer.stopped = true
			timer.ch <- now
		}
	}
	c.mu.Unlock()
}

type presenterTimer struct {
	clock   *presenterClock
	at      time.Time
	ch      chan time.Time
	stopped bool
}

func (t *presenterTimer) C() <-chan time.Time { return t.ch }

func (t *presenterTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	active := !t.stopped
	t.stopped = true
	delete(t.clock.timers, t)
	return active
}
