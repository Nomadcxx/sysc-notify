//go:build linux

package history

import (
	"os"
	"syscall"
)

func sameFileChangeTime(a, b os.FileInfo) bool {
	statA, okA := a.Sys().(*syscall.Stat_t)
	statB, okB := b.Sys().(*syscall.Stat_t)
	return okA && okB && statA.Ctim == statB.Ctim
}
