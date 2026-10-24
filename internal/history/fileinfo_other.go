//go:build !linux

package history

import "os"

// Other targets have no portable change-time field in os.FileInfo; the caller
// still checks file identity, size and modtime.
func sameFileChangeTime(os.FileInfo, os.FileInfo) bool {
	return true
}
