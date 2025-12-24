//go:build plan9

package shared

import (
	"os"
	"syscall"
)

// getPackfileIdentity extracts filesystem identity on Plan9 systems.
// Plan9 provides Qid which contains path (unique per-file identifier) and
// vers (version/modification number).
func getPackfileIdentity(filePath string) (PackfileIdentity, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		return PackfileIdentity{}, err
	}

	stat, ok := fi.Sys().(*syscall.Dir)
	if !ok {
		return PackfileIdentity{}, os.ErrInvalid
	}

	// Use Qid.Path as inode equivalent and Qid.Vers for additional uniqueness
	// Plan9's Qid.Path is unique per file on a server
	return PackfileIdentity{
		Inode:  stat.Qid.Path,
		Device: uint64(stat.Qid.Vers), // Use version as part of identity
		Size:   fi.Size(),
		Mtime:  fi.ModTime().UnixNano(),
	}, nil
}
