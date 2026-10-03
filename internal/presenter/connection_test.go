package presenter

import (
	"context"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"

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

func TestFailDropsCommandBlockedOnOwner(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	sink := funcSink(func(event state.Event) bool {
		if event.Delta != nil && event.Delta.Notification != nil && event.Delta.Notification.Summary == "blocker" {
			once.Do(func() {
				close(started)
				<-release
			})
		}
		return true
	})
	owner := state.Start(newPresenterClock(), sink)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		_ = owner.Close()
	})

	id := addCandidate(t, owner, "keep", 0)
	doState(t, owner, state.Command{
		Kind: state.PresentationRenew, Generation: 1,
		Presentations: []protocol.Presentation{{ID: id, State: protocol.PresentationQueued}},
	})
	blockerDone := make(chan struct{})
	go func() {
		defer close(blockerDone)
		_, _ = owner.Do(context.Background(), state.Command{Kind: state.Add, Candidate: notify.Candidate{
			Summary: "blocker", Urgency: protocol.UrgencyNormal,
		}})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("owner did not block in publish")
	}

	client, server := unixPair(t)
	defer client.Close()
	conn := newConnection(server, 1)
	defer conn.fail()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn.readLoop(owner)
	}()
	if err := writeEnvelope(client, protocol.KindCommand, 1, 0, protocol.Command{Kind: protocol.CommandDismiss, ID: id}); err != nil {
		t.Fatal(err)
	}
	waitForStack(t, "(*connection).readLoop", "(*Owner).Do")
	conn.fail()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("failed connection left its command blocked in Do")
	}
	close(release)
	select {
	case <-blockerDone:
	case <-time.After(time.Second):
		t.Fatal("owner did not finish the blocked publish")
	}
	if !activeIDs(snapshotOwner(t, owner), id) {
		t.Fatal("command blocked on the replaced connection still dismissed the notification")
	}
}

func TestExecuteHistoryRemoveReachesState(t *testing.T) {
	clock := newPresenterClock()
	store, err := history.OpenAt(t.TempDir(), clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	h := startHistoryHarness(t, clock, store)
	id := addCandidate(t, h.owner, "gone", time.Minute)
	doState(t, h.owner, state.Command{Kind: state.Dismiss, ID: id})

	reply := executeCommand(h.owner, 0, protocol.Command{
		Kind: protocol.CommandHistoryRemove,
		IDs:  []uint32{id},
	})
	if !reply.OK {
		t.Fatalf("reply = %+v, want OK", reply)
	}
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

type funcSink func(state.Event) bool

func (f funcSink) Publish(event state.Event) bool { return f(event) }

func unixPair(t *testing.T) (*net.UnixConn, *net.UnixConn) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	return unixConn(t, fds[0]), unixConn(t, fds[1])
}

func unixConn(t *testing.T, fd int) *net.UnixConn {
	t.Helper()
	file := os.NewFile(uintptr(fd), "unix")
	defer file.Close()
	conn, err := net.FileConn(file)
	if err != nil {
		t.Fatal(err)
	}
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		_ = conn.Close()
		t.Fatal("socketpair is not a unix connection")
	}
	return unixConn
}

func waitForStack(t *testing.T, fragments ...string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	buf := make([]byte, 1<<20)
	for time.Now().Before(deadline) {
		n := runtime.Stack(buf, true)
		for _, stack := range strings.Split(string(buf[:n]), "\n\n") {
			found := true
			for _, fragment := range fragments {
				found = found && strings.Contains(stack, fragment)
			}
			if found {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for a goroutine containing %q", fragments)
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
