package shared

// PackfileIdentity uniquely identifies a packfile using filesystem metadata.
// This allows multiple memory storage instances to recognize when they're
// working with the same underlying packfile and share the data in memory.
type PackfileIdentity struct {
	// Inode number - unique per filesystem
	Inode uint64
	// Device ID - distinguishes files across mount points
	Device uint64
	// Size in bytes
	Size int64
	// Modification time (Unix nanoseconds)
	Mtime int64
}

// GetPackfileIdentity extracts filesystem identity from a file.
// This is implemented in platform-specific files (fileinfo_unix.go, fileinfo_windows.go).
func GetPackfileIdentity(filePath string) (PackfileIdentity, error) {
	return getPackfileIdentity(filePath)
}
