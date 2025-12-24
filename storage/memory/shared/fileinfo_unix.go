//go:build unix

package shared

import (
	"os"
	"syscall"
)

// getPackfileIdentity extracts filesystem identity on Unix-like systems
// (Linux, macOS, BSD, etc.)
func getPackfileIdentity(filePath string) (PackfileIdentity, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		return PackfileIdentity{}, err
	}

	stat, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return PackfileIdentity{}, os.ErrInvalid
	}

	return PackfileIdentity{
		Inode:  stat.Ino,
		Device: uint64(stat.Dev),
		Size:   fi.Size(),
		Mtime:  fi.ModTime().UnixNano(),
	}, nil
}
