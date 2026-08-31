package sender

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/Nomadcxx/sysc-notify/protocol"
)

func TestCaptureWalksParentChain(t *testing.T) {
	root := t.TempDir()
	writeStat(t, root, 300, "leaf ) worker", 200, 30)
	writeStat(t, root, 200, "middle parent", 1, 20)
	writeStat(t, root, 1, "init", 0, 10)

	want := []protocol.Process{
		{PID: 300, StartTime: 30},
		{PID: 200, StartTime: 20},
		{PID: 1, StartTime: 10},
	}
	if got := Capture(root, 300); !reflect.DeepEqual(got, want) {
		t.Fatalf("Capture() = %#v, want %#v", got, want)
	}
}

func TestCaptureStopsAtReusedParentPID(t *testing.T) {
	root := t.TempDir()
	writeStat(t, root, 300, "child", 200, 20)
	writeStat(t, root, 200, "newer process in parent slot", 1, 30)

	want := []protocol.Process{{PID: 300, StartTime: 20}}
	if got := Capture(root, 300); !reflect.DeepEqual(got, want) {
		t.Fatalf("Capture() = %#v, want %#v", got, want)
	}
}

func TestCaptureStopsAtCyclesAndReadFailures(t *testing.T) {
	t.Run("cycle", func(t *testing.T) {
		root := t.TempDir()
		writeStat(t, root, 300, "a", 200, 20)
		writeStat(t, root, 200, "b", 300, 10)
		want := []protocol.Process{{PID: 300, StartTime: 20}, {PID: 200, StartTime: 10}}
		if got := Capture(root, 300); !reflect.DeepEqual(got, want) {
			t.Fatalf("Capture() = %#v, want %#v", got, want)
		}
	})

	for _, test := range []struct {
		name    string
		prepare func(*testing.T, string)
	}{
		{name: "disappearing", prepare: func(t *testing.T, root string) {
			writeStat(t, root, 300, "child", 200, 20)
		}},
		{name: "permission", prepare: func(t *testing.T, root string) {
			writeStat(t, root, 300, "child", 200, 20)
			dir := filepath.Join(root, "200")
			if err := os.Mkdir(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "stat")
			if err := os.WriteFile(path, []byte("unreadable"), 0o000); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Chmod(path, 0o600) })
		}},
		{name: "malformed", prepare: func(t *testing.T, root string) {
			writeStat(t, root, 300, "child", 200, 20)
			dir := filepath.Join(root, "200")
			if err := os.Mkdir(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "stat"), []byte("200 (broken) S nope"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			test.prepare(t, root)
			want := []protocol.Process{{PID: 300, StartTime: 20}}
			if got := Capture(root, 300); !reflect.DeepEqual(got, want) {
				t.Fatalf("Capture() = %#v, want %#v", got, want)
			}
		})
	}
}

func TestCaptureBoundsLongChains(t *testing.T) {
	root := t.TempDir()
	for pid := uint32(100); pid < 120; pid++ {
		parent := pid + 1
		if pid == 119 {
			parent = 0
		}
		writeStat(t, root, pid, fmt.Sprintf("process %d", pid), parent, uint64(1000-pid))
	}

	got := Capture(root, 100)
	if len(got) != protocol.MaxLineageEntries {
		t.Fatalf("lineage length = %d, want %d", len(got), protocol.MaxLineageEntries)
	}
	if got[0].PID != 100 || got[len(got)-1].PID != 115 {
		t.Fatalf("bounded lineage = %#v", got)
	}
}

func TestCaptureRejectsMalformedInitialStat(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "42")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, contents := range []string{
		"42 no-parentheses S 1",
		"42 (short) S 1",
		"42 (bad ppid) S x 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 10",
		"42 (bad start) S 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 nope",
	} {
		if err := os.WriteFile(filepath.Join(dir, "stat"), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
		if got := Capture(root, 42); len(got) != 0 {
			t.Fatalf("Capture(%q) = %#v, want empty", contents, got)
		}
	}
}

func writeStat(t *testing.T, root string, pid uint32, comm string, parent uint32, start uint64) {
	t.Helper()
	dir := filepath.Join(root, strconv.FormatUint(uint64(pid), 10))
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	fields := make([]string, 20)
	for i := range fields {
		fields[i] = "0"
	}
	fields[0] = "S"
	fields[1] = strconv.FormatUint(uint64(parent), 10)
	fields[19] = strconv.FormatUint(start, 10)
	contents := fmt.Sprintf("%d (%s) %s\n", pid, comm, strings.Join(fields, " "))
	if err := os.WriteFile(filepath.Join(dir, "stat"), []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}
