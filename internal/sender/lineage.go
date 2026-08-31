package sender

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/Nomadcxx/sysc-notify/protocol"
)

func Capture(procRoot string, pid uint32) []protocol.Process {
	var lineage []protocol.Process
	seen := make(map[uint32]struct{}, protocol.MaxLineageEntries)
	var childStart uint64
	for pid != 0 && len(lineage) < protocol.MaxLineageEntries {
		if _, exists := seen[pid]; exists {
			break
		}
		parent, start, ok := readStat(procRoot, pid)
		if !ok || (childStart != 0 && start > childStart) {
			break
		}
		seen[pid] = struct{}{}
		lineage = append(lineage, protocol.Process{PID: pid, StartTime: start})
		pid, childStart = parent, start
	}
	return lineage
}

func readStat(procRoot string, pid uint32) (uint32, uint64, bool) {
	data, err := os.ReadFile(filepath.Join(procRoot, strconv.FormatUint(uint64(pid), 10), "stat"))
	if err != nil {
		return 0, 0, false
	}
	stat := strings.TrimSpace(string(data))
	close := strings.LastIndex(stat, ") ")
	open := strings.IndexByte(stat, '(')
	if open < 1 || close <= open {
		return 0, 0, false
	}
	parsedPID, err := strconv.ParseUint(strings.TrimSpace(stat[:open]), 10, 32)
	if err != nil || uint32(parsedPID) != pid {
		return 0, 0, false
	}
	fields := strings.Fields(stat[close+2:])
	if len(fields) < 20 || len(fields[0]) != 1 {
		return 0, 0, false
	}
	parent, err := strconv.ParseUint(fields[1], 10, 32)
	if err != nil {
		return 0, 0, false
	}
	start, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || start == 0 {
		return 0, 0, false
	}
	return uint32(parent), start, true
}
