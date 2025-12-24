package shared

import (
	"bytes"
	"runtime"
	"sync"
	"testing"
	"time"
	"unique"
)

func TestGetOrCreatePackfile_SameIdentity(t *testing.T) {
	ClearStore()
	defer ClearStore()

	identity := PackfileIdentity{
		Inode:  12345,
		Device: 67890,
		Size:   1024,
		Mtime:  time.Now().UnixNano(),
	}

	data := []byte("test packfile data")

	// Create first handle
	handle1 := GetOrCreatePackfile(identity, data)
	if handle1 == (unique.Handle[*PackfileData]{}) {
		t.Fatal("expected non-zero handle")
	}

	// Create second handle with same identity
	handle2 := GetOrCreatePackfile(identity, []byte("different data that should be ignored"))

	// Handles should be equal (pointing to same canonical data)
	if handle1 != handle2 {
		t.Error("expected same handle for same identity")
	}

	// Verify data is shared
	data1 := handle1.Value().Data
	data2 := handle2.Value().Data

	if !bytes.Equal(data1, data2) {
		t.Error("expected shared data to be identical")
	}

	if !bytes.Equal(data1, data) {
		t.Error("expected shared data to match original data")
	}
}

func TestGetOrCreatePackfile_DifferentIdentities(t *testing.T) {
	ClearStore()
	defer ClearStore()

	identity1 := PackfileIdentity{
		Inode:  12345,
		Device: 67890,
		Size:   1024,
		Mtime:  time.Now().UnixNano(),
	}

	identity2 := PackfileIdentity{
		Inode:  54321,
		Device: 67890,
		Size:   1024,
		Mtime:  time.Now().UnixNano(),
	}

	data1 := []byte("test packfile data 1")
	data2 := []byte("test packfile data 2")

	// Create handles
	handle1 := GetOrCreatePackfile(identity1, data1)
	handle2 := GetOrCreatePackfile(identity2, data2)

	// Handles should be different
	if handle1 == handle2 {
		t.Error("expected different handles for different identities")
	}

	// Verify each has its own data
	if !bytes.Equal(handle1.Value().Data, data1) {
		t.Error("handle1 data mismatch")
	}
	if !bytes.Equal(handle2.Value().Data, data2) {
		t.Error("handle2 data mismatch")
	}
}

func TestGetPackfile(t *testing.T) {
	ClearStore()
	defer ClearStore()

	identity := PackfileIdentity{
		Inode:  12345,
		Device: 67890,
		Size:   1024,
		Mtime:  time.Now().UnixNano(),
	}

	// Try to get non-existent packfile
	_, ok := GetPackfile(identity)
	if ok {
		t.Error("expected GetPackfile to return false for non-existent packfile")
	}

	// Create packfile
	data := []byte("test packfile data")
	handle1 := GetOrCreatePackfile(identity, data)

	// Now get it
	handle2, ok := GetPackfile(identity)
	if !ok {
		t.Error("expected GetPackfile to return true for existing packfile")
	}

	if handle1 != handle2 {
		t.Error("expected same handle from GetPackfile")
	}
}

func TestInvalidatePackfile(t *testing.T) {
	ClearStore()
	defer ClearStore()

	identity := PackfileIdentity{
		Inode:  12345,
		Device: 67890,
		Size:   1024,
		Mtime:  time.Now().UnixNano(),
	}

	// Create packfile
	data := []byte("test packfile data")
	GetOrCreatePackfile(identity, data)

	// Verify it exists
	_, ok := GetPackfile(identity)
	if !ok {
		t.Error("expected packfile to exist before invalidation")
	}

	// Invalidate
	InvalidatePackfile(identity)

	// Verify it's gone
	_, ok = GetPackfile(identity)
	if ok {
		t.Error("expected packfile to be gone after invalidation")
	}
}

func TestGetStats(t *testing.T) {
	ClearStore()
	defer ClearStore()

	// Empty store
	stats := GetStats()
	if stats.UniquePackfiles != 0 {
		t.Errorf("expected 0 packfiles, got %d", stats.UniquePackfiles)
	}
	if stats.SharedMemoryMB != 0 {
		t.Errorf("expected 0 MB, got %d", stats.SharedMemoryMB)
	}

	// Add some packfiles
	identity1 := PackfileIdentity{Inode: 1, Device: 1, Size: 1024, Mtime: 1}
	identity2 := PackfileIdentity{Inode: 2, Device: 1, Size: 2048, Mtime: 1}

	// Create 1MB of data for each
	data1 := make([]byte, 1024*1024)
	data2 := make([]byte, 1024*1024)

	GetOrCreatePackfile(identity1, data1)
	GetOrCreatePackfile(identity2, data2)

	// Check stats
	stats = GetStats()
	if stats.UniquePackfiles != 2 {
		t.Errorf("expected 2 packfiles, got %d", stats.UniquePackfiles)
	}
	if stats.SharedMemoryMB != 2 {
		t.Errorf("expected 2 MB, got %d", stats.SharedMemoryMB)
	}
}

func TestConcurrentAccess(t *testing.T) {
	ClearStore()
	defer ClearStore()

	identity := PackfileIdentity{
		Inode:  12345,
		Device: 67890,
		Size:   1024,
		Mtime:  time.Now().UnixNano(),
	}

	data := []byte("test packfile data")

	// Launch multiple goroutines trying to create the same packfile
	const numGoroutines = 100
	var wg sync.WaitGroup
	handles := make([]unique.Handle[*PackfileData], numGoroutines)

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			handles[idx] = GetOrCreatePackfile(identity, data)
		}(i)
	}

	wg.Wait()

	// All handles should be equal
	firstHandle := handles[0]
	for i := 1; i < numGoroutines; i++ {
		if handles[i] != firstHandle {
			t.Errorf("handle %d differs from first handle", i)
		}
	}

	// Should only have one entry in the store
	stats := GetStats()
	if stats.UniquePackfiles != 1 {
		t.Errorf("expected 1 unique packfile after concurrent access, got %d", stats.UniquePackfiles)
	}
}

func TestPackfileDataReader(t *testing.T) {
	ClearStore()
	defer ClearStore()

	identity := PackfileIdentity{
		Inode:  12345,
		Device: 67890,
		Size:   1024,
		Mtime:  time.Now().UnixNano(),
	}

	data := []byte("test packfile data")

	handle := GetOrCreatePackfile(identity, data)

	// Get the reader
	reader := handle.Value().Reader

	// Read from the beginning
	buf := make([]byte, 4)
	n, err := reader.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if n != 4 {
		t.Errorf("expected to read 4 bytes, got %d", n)
	}
	if !bytes.Equal(buf, []byte("test")) {
		t.Errorf("expected 'test', got '%s'", buf)
	}

	// Read from offset
	n, err = reader.ReadAt(buf, 5)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if n != 4 {
		t.Errorf("expected to read 4 bytes, got %d", n)
	}
	if !bytes.Equal(buf, []byte("pack")) {
		t.Errorf("expected 'pack', got '%s'", buf)
	}
}

func TestHandleEquality(t *testing.T) {
	ClearStore()
	defer ClearStore()

	identity := PackfileIdentity{
		Inode:  12345,
		Device: 67890,
		Size:   1024,
		Mtime:  time.Now().UnixNano(),
	}

	data := []byte("test packfile data")

	handle1 := GetOrCreatePackfile(identity, data)
	handle2 := GetOrCreatePackfile(identity, data)

	// Handles should be comparable and equal
	if handle1 != handle2 {
		t.Error("expected handles to be equal")
	}

	// Compare should be fast (pointer comparison)
	// This is a semantic test - we can't really measure speed here,
	// but we verify the comparison works
	start := time.Now()
	for i := 0; i < 1000000; i++ {
		_ = handle1 == handle2
	}
	elapsed := time.Since(start)

	// Should be very fast (< 10ms for 1M comparisons)
	if elapsed > 100*time.Millisecond {
		t.Logf("Warning: handle comparison seems slow: %v for 1M ops", elapsed)
	}
}

func TestGarbageCollection(t *testing.T) {
	// This test verifies that when handles are released,
	// the canonical data can be garbage collected.
	// Note: This is a best-effort test and may not always trigger GC.

	ClearStore()
	defer ClearStore()

	identity := PackfileIdentity{
		Inode:  12345,
		Device: 67890,
		Size:   1024,
		Mtime:  time.Now().UnixNano(),
	}

	// Create a large packfile
	largeData := make([]byte, 10*1024*1024) // 10MB

	// Create handle and then release it
	{
		handle := GetOrCreatePackfile(identity, largeData)
		_ = handle
		// handle goes out of scope here
	}

	// The unique package uses weak references, so when all handles
	// are released, the canonical data should be eligible for GC.
	// However, our store still holds a reference in the map,
	// so it won't be collected until we remove it.

	// Remove from store
	InvalidatePackfile(identity)

	// Force GC
	runtime.GC()
	runtime.GC()

	// We can't directly verify the memory was freed, but at least
	// verify the entry is gone from our store
	_, ok := GetPackfile(identity)
	if ok {
		t.Error("expected packfile to be gone after invalidation")
	}
}
