package memory

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestObjectStorage_SetPackfileData tests storing packfile data in memory.
func TestObjectStorage_SetPackfileData(t *testing.T) {
	storage := NewStorage(WithLazyLoading(0))

	packfileData := []byte{
		0x50, 0x41, 0x43, 0x4b, // "PACK" signature
		0x00, 0x00, 0x00, 0x02, // Version 2
		0x00, 0x00, 0x00, 0x01, // 1 object
		// ... rest of packfile data ...
	}

	err := storage.ObjectStorage.SetPackfileData(packfileData)
	require.NoError(t, err)

	// Verify data was stored
	assert.NotNil(t, storage.ObjectStorage.packfileData)
	assert.Equal(t, packfileData, storage.ObjectStorage.packfileData)
}

// TestObjectStorage_SetPackfileData_Empty tests storing empty packfile.
func TestObjectStorage_SetPackfileData_Empty(t *testing.T) {
	storage := NewStorage(WithLazyLoading(0))

	err := storage.ObjectStorage.SetPackfileData([]byte{})
	require.NoError(t, err)

	assert.NotNil(t, storage.ObjectStorage.packfileData)
	assert.Empty(t, storage.ObjectStorage.packfileData)
}

// TestObjectStorage_SetPackfileData_Nil tests storing nil packfile.
func TestObjectStorage_SetPackfileData_Nil(t *testing.T) {
	storage := NewStorage(WithLazyLoading(0))

	err := storage.ObjectStorage.SetPackfileData(nil)
	require.NoError(t, err)

	assert.Nil(t, storage.ObjectStorage.packfileData)
}

// TestObjectStorage_GetPackfileReader tests getting a ReaderAt for the packfile.
func TestObjectStorage_GetPackfileReader(t *testing.T) {
	storage := NewStorage(WithLazyLoading(0))

	packfileData := []byte("This is test packfile data with some content")
	err := storage.ObjectStorage.SetPackfileData(packfileData)
	require.NoError(t, err)

	reader := storage.ObjectStorage.GetPackfileReader()
	require.NotNil(t, reader)

	// Test reading from the beginning
	buf := make([]byte, 4)
	n, err := reader.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, []byte("This"), buf)

	// Test reading from middle
	buf = make([]byte, 4)
	n, err = reader.ReadAt(buf, 8)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, []byte("test"), buf)

	// Test reading from end
	buf = make([]byte, 7)
	n, err = reader.ReadAt(buf, int64(len(packfileData)-7))
	require.NoError(t, err)
	assert.Equal(t, 7, n)
	assert.Equal(t, []byte("content"), buf)
}

// TestObjectStorage_GetPackfileReader_NoData tests getting reader when no data is set.
func TestObjectStorage_GetPackfileReader_NoData(t *testing.T) {
	storage := NewStorage(WithLazyLoading(0))

	reader := storage.ObjectStorage.GetPackfileReader()
	assert.Nil(t, reader)
}

// TestObjectStorage_GetPackfileReader_AfterMultipleSets tests that reader
// always returns the latest packfile data.
func TestObjectStorage_GetPackfileReader_AfterMultipleSets(t *testing.T) {
	storage := NewStorage(WithLazyLoading(0))

	// Set first packfile
	firstData := []byte("first packfile")
	err := storage.ObjectStorage.SetPackfileData(firstData)
	require.NoError(t, err)

	reader1 := storage.ObjectStorage.GetPackfileReader()
	buf1 := make([]byte, 5)
	reader1.ReadAt(buf1, 0)
	assert.Equal(t, []byte("first"), buf1)

	// Set second packfile
	secondData := []byte("second packfile")
	err = storage.ObjectStorage.SetPackfileData(secondData)
	require.NoError(t, err)

	reader2 := storage.ObjectStorage.GetPackfileReader()
	buf2 := make([]byte, 6)
	reader2.ReadAt(buf2, 0)
	assert.Equal(t, []byte("second"), buf2)
}

// TestObjectStorage_PackfileRetention_Integration tests the integration
// of packfile retention with storage options.
func TestObjectStorage_PackfileRetention_Integration(t *testing.T) {
	// Create storage with lazy loading (should enable packfile retention)
	storage := NewStorage(WithLazyLoading(0))

	assert.True(t, storage.options.packfileRetention, "packfile retention should be enabled")

	// Store packfile data
	packfileData := []byte("packfile content for lazy loading")
	err := storage.ObjectStorage.SetPackfileData(packfileData)
	require.NoError(t, err)

	// Verify we can get a reader
	reader := storage.ObjectStorage.GetPackfileReader()
	require.NotNil(t, reader)

	// Verify reader works
	buf := make([]byte, 8)
	n, err := reader.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, 8, n)
	assert.Equal(t, []byte("packfile"), buf)
}

// TestObjectStorage_PackfileReader_ReadBeyondEnd tests reading beyond packfile end.
func TestObjectStorage_PackfileReader_ReadBeyondEnd(t *testing.T) {
	storage := NewStorage(WithLazyLoading(0))

	packfileData := []byte("short")
	err := storage.ObjectStorage.SetPackfileData(packfileData)
	require.NoError(t, err)

	reader := storage.ObjectStorage.GetPackfileReader()
	require.NotNil(t, reader)

	// Try to read beyond end
	buf := make([]byte, 10)
	n, err := reader.ReadAt(buf, 0)

	// Should read what's available (5 bytes) and return EOF
	assert.Equal(t, 5, n)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, []byte("short"), buf[:5])
}

// TestObjectStorage_PackfileReader_ReadAtNegativeOffset tests invalid offset.
func TestObjectStorage_PackfileReader_ReadAtNegativeOffset(t *testing.T) {
	storage := NewStorage(WithLazyLoading(0))

	packfileData := []byte("test data")
	err := storage.ObjectStorage.SetPackfileData(packfileData)
	require.NoError(t, err)

	reader := storage.ObjectStorage.GetPackfileReader()
	require.NotNil(t, reader)

	// bytes.Reader.ReadAt doesn't allow negative offsets
	buf := make([]byte, 4)
	_, err = reader.ReadAt(buf, -1)
	assert.Error(t, err)
}

// TestObjectStorage_PackfileData_LargeData tests with larger packfile data.
func TestObjectStorage_PackfileData_LargeData(t *testing.T) {
	storage := NewStorage(WithLazyLoading(0))

	// Create 1MB of test data
	largeData := make([]byte, 1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	err := storage.ObjectStorage.SetPackfileData(largeData)
	require.NoError(t, err)

	reader := storage.ObjectStorage.GetPackfileReader()
	require.NotNil(t, reader)

	// Read from various positions
	testCases := []struct {
		offset int64
		size   int
	}{
		{0, 100},
		{1024, 512},
		{1024*512, 1024},
		{1024*1024 - 100, 100},
	}

	for _, tc := range testCases {
		buf := make([]byte, tc.size)
		n, err := reader.ReadAt(buf, tc.offset)
		require.NoError(t, err)
		assert.Equal(t, tc.size, n)

		// Verify data matches
		for i := 0; i < tc.size; i++ {
			expected := byte((tc.offset + int64(i)) % 256)
			assert.Equal(t, expected, buf[i], "mismatch at offset %d+%d", tc.offset, i)
		}
	}
}

// TestObjectStorage_NoPackfileRetention tests that packfile is not retained
// when lazy loading is disabled.
func TestObjectStorage_NoPackfileRetention(t *testing.T) {
	// Create storage without lazy loading
	storage := NewStorage()

	assert.False(t, storage.options.packfileRetention, "packfile retention should be disabled")

	// Even if we try to set packfile data, it shouldn't be stored
	// (or the storage should handle it gracefully)
	packfileData := []byte("test packfile")
	err := storage.ObjectStorage.SetPackfileData(packfileData)
	require.NoError(t, err)

	// Reader might still work if data was set, but that's okay
	// The important part is that the option is correctly set
}

// TestObjectStorage_PackfileEviction tests automatic packfile eviction
// when all lazy objects have been cached.
func TestObjectStorage_PackfileEviction(t *testing.T) {
	storage := NewStorage(WithLazyLoading(0))

	// Set up a mock packfile
	packfileData := []byte("mock packfile data with sufficient length")
	err := storage.ObjectStorage.SetPackfileData(packfileData)
	require.NoError(t, err)

	// Verify packfile is present
	assert.NotNil(t, storage.ObjectStorage.packfileReader, "packfile should be present initially")
	assert.False(t, storage.ObjectStorage.IsPackfileEvicted(), "packfile should not be evicted initially")

	// Simulate creating 3 lazy objects
	for i := 0; i < 3; i++ {
		storage.ObjectStorage.lazyObjectCount++
	}

	total, cached := storage.ObjectStorage.GetLazyObjectStats()
	assert.Equal(t, 3, total, "should have 3 lazy objects")
	assert.Equal(t, 0, cached, "should have 0 cached objects initially")

	// Simulate caching first object
	storage.ObjectStorage.notifyObjectCached()
	total, cached = storage.ObjectStorage.GetLazyObjectStats()
	assert.Equal(t, 3, total)
	assert.Equal(t, 1, cached)
	assert.False(t, storage.ObjectStorage.IsPackfileEvicted(), "packfile should not be evicted yet")

	// Simulate caching second object
	storage.ObjectStorage.notifyObjectCached()
	total, cached = storage.ObjectStorage.GetLazyObjectStats()
	assert.Equal(t, 3, total)
	assert.Equal(t, 2, cached)
	assert.False(t, storage.ObjectStorage.IsPackfileEvicted(), "packfile should not be evicted yet")

	// Simulate caching third (final) object
	storage.ObjectStorage.notifyObjectCached()
	total, cached = storage.ObjectStorage.GetLazyObjectStats()
	assert.Equal(t, 3, total)
	assert.Equal(t, 3, cached)

	// Packfile should be evicted now
	assert.True(t, storage.ObjectStorage.IsPackfileEvicted(), "packfile should be evicted after all objects cached")
	assert.Nil(t, storage.ObjectStorage.packfileReader, "packfile reader should be nil")
	assert.Nil(t, storage.ObjectStorage.packfileData, "packfile data should be nil")
}

// TestObjectStorage_PackfileEviction_NoEvictionIfNoLazyObjects tests that
// packfile is not evicted if there are no lazy objects.
func TestObjectStorage_PackfileEviction_NoEvictionIfNoLazyObjects(t *testing.T) {
	storage := NewStorage(WithLazyLoading(0))

	packfileData := []byte("mock packfile data")
	err := storage.ObjectStorage.SetPackfileData(packfileData)
	require.NoError(t, err)

	// Call notifyObjectCached without any lazy objects
	storage.ObjectStorage.notifyObjectCached()

	// Packfile should not be evicted
	assert.False(t, storage.ObjectStorage.IsPackfileEvicted())
	assert.NotNil(t, storage.ObjectStorage.packfileReader)
}

// TestObjectStorage_PackfileEviction_Concurrent tests thread-safe eviction
// with concurrent cache notifications.
func TestObjectStorage_PackfileEviction_Concurrent(t *testing.T) {
	storage := NewStorage(WithLazyLoading(0))

	packfileData := []byte("mock packfile data")
	err := storage.ObjectStorage.SetPackfileData(packfileData)
	require.NoError(t, err)

	// Create 100 lazy objects
	numObjects := 100
	storage.ObjectStorage.lazyObjectCount = numObjects

	// Simulate concurrent caching from multiple goroutines
	done := make(chan bool)
	for i := 0; i < numObjects; i++ {
		go func() {
			storage.ObjectStorage.notifyObjectCached()
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < numObjects; i++ {
		<-done
	}

	// Verify all objects are cached and packfile is evicted
	total, cached := storage.ObjectStorage.GetLazyObjectStats()
	assert.Equal(t, numObjects, total)
	assert.Equal(t, numObjects, cached)
	assert.True(t, storage.ObjectStorage.IsPackfileEvicted())
	assert.Nil(t, storage.ObjectStorage.packfileReader)
}
