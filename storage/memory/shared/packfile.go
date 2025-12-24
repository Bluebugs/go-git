package shared

import (
	"bytes"
	"io"
	"sync"
	"unique"
)

// PackfileData holds the canonical packfile data.
// Multiple storage instances can share the same PackfileData in memory.
type PackfileData struct {
	// Raw packfile bytes (including PACK header and footer)
	Data []byte
	// ReaderAt interface for lazy object access
	Reader io.ReaderAt
}

// Global packfile store for deduplication
var packfileStore struct {
	mu      sync.RWMutex
	handles map[PackfileIdentity]unique.Handle[*PackfileData]
}

func init() {
	packfileStore.handles = make(map[PackfileIdentity]unique.Handle[*PackfileData])
}

// GetOrCreatePackfile returns a canonical handle for the packfile.
// If a packfile with the same identity already exists in the store,
// returns the existing handle. Otherwise, creates a new canonical entry.
//
// The data parameter is only used when creating a new entry. If an entry
// already exists, the data parameter is ignored.
//
// This function is thread-safe.
func GetOrCreatePackfile(identity PackfileIdentity, data []byte) unique.Handle[*PackfileData] {
	// First try to get existing handle with read lock
	packfileStore.mu.RLock()
	if handle, ok := packfileStore.handles[identity]; ok {
		packfileStore.mu.RUnlock()
		return handle
	}
	packfileStore.mu.RUnlock()

	// Not found, acquire write lock to create
	packfileStore.mu.Lock()
	defer packfileStore.mu.Unlock()

	// Double-check after acquiring write lock (another goroutine may have created it)
	if handle, ok := packfileStore.handles[identity]; ok {
		return handle
	}

	// Create new packfile data
	packfileData := &PackfileData{
		Data:   data,
		Reader: bytes.NewReader(data),
	}

	// Create canonical handle
	handle := unique.Make(packfileData)

	// Store in map
	packfileStore.handles[identity] = handle

	return handle
}

// GetPackfile retrieves an existing canonical packfile if available.
// Returns the handle and true if found, zero handle and false otherwise.
//
// This function is thread-safe.
func GetPackfile(identity PackfileIdentity) (unique.Handle[*PackfileData], bool) {
	packfileStore.mu.RLock()
	defer packfileStore.mu.RUnlock()

	handle, ok := packfileStore.handles[identity]
	return handle, ok
}

// InvalidatePackfile removes a packfile from the store.
// This should be called when a packfile is modified and the old entry is stale.
//
// This function is thread-safe.
func InvalidatePackfile(identity PackfileIdentity) {
	packfileStore.mu.Lock()
	defer packfileStore.mu.Unlock()

	delete(packfileStore.handles, identity)
}

// Stats holds statistics about the shared packfile store.
type Stats struct {
	// Number of unique packfiles currently in the store
	UniquePackfiles int
	// Total shared memory in megabytes (sum of all packfile sizes)
	SharedMemoryMB int64
}

// GetStats returns current statistics about the shared packfile store.
//
// This function is thread-safe.
func GetStats() Stats {
	packfileStore.mu.RLock()
	defer packfileStore.mu.RUnlock()

	stats := Stats{
		UniquePackfiles: len(packfileStore.handles),
	}

	// Calculate total shared memory
	var totalBytes int64
	for _, handle := range packfileStore.handles {
		data := handle.Value()
		totalBytes += int64(len(data.Data))
	}
	stats.SharedMemoryMB = totalBytes / (1024 * 1024)

	return stats
}

// ClearStore removes all entries from the store.
// This is primarily useful for testing.
//
// This function is thread-safe.
func ClearStore() {
	packfileStore.mu.Lock()
	defer packfileStore.mu.Unlock()

	packfileStore.handles = make(map[PackfileIdentity]unique.Handle[*PackfileData])
}
