package plumbing

import (
	"bytes"
	"compress/zlib"
	"io"
	"testing"

	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLazyMemoryObject_ImplementsEncodedObject verifies that LazyMemoryObject
// implements the EncodedObject interface at compile time.
func TestLazyMemoryObject_ImplementsEncodedObject(t *testing.T) {
	var _ EncodedObject = (*LazyMemoryObject)(nil)
}

// TestLazyMemoryObject_BasicProperties tests that LazyMemoryObject correctly
// stores and retrieves basic object properties like type, size, and hash.
func TestLazyMemoryObject_BasicProperties(t *testing.T) {
	testHash := NewHash("1234567890abcdef1234567890abcdef12345678")
	oh := FromObjectFormat(formatcfg.SHA1)

	obj := &LazyMemoryObject{
		t:  BlobObject,
		h:  testHash,
		sz: 1024,
		oh: oh,
	}

	// Test Type()
	assert.Equal(t, BlobObject, obj.Type())

	// Test Size()
	assert.Equal(t, int64(1024), obj.Size())

	// Test Hash()
	assert.Equal(t, testHash, obj.Hash())
}

// TestLazyMemoryObject_SetType tests the SetType method.
func TestLazyMemoryObject_SetType(t *testing.T) {
	obj := &LazyMemoryObject{}

	obj.SetType(TreeObject)
	assert.Equal(t, TreeObject, obj.Type())

	obj.SetType(CommitObject)
	assert.Equal(t, CommitObject, obj.Type())
}

// TestLazyMemoryObject_SetSize tests the SetSize method.
func TestLazyMemoryObject_SetSize(t *testing.T) {
	obj := &LazyMemoryObject{}

	obj.SetSize(2048)
	assert.Equal(t, int64(2048), obj.Size())

	obj.SetSize(0)
	assert.Equal(t, int64(0), obj.Size())
}

// Helper function to compress data using zlib (same as git packfiles)
func zlibCompress(t *testing.T, data []byte) []byte {
	t.Helper()

	var buf bytes.Buffer
	w := zlib.NewWriter(&buf)
	_, err := w.Write(data)
	require.NoError(t, err)
	err = w.Close()
	require.NoError(t, err)

	return buf.Bytes()
}

// Helper function to create a mock packfile with compressed content at a specific offset
func createMockPackfile(t *testing.T, content []byte, offset int64) io.ReaderAt {
	t.Helper()

	// Compress the content
	compressed := zlibCompress(t, content)

	// Create a buffer with some padding before the content to simulate offset
	packfile := make([]byte, offset+int64(len(compressed)))
	copy(packfile[offset:], compressed)

	return bytes.NewReader(packfile)
}

// TestLazyMemoryObject_Reader_DecompressesOnDemand tests that Reader()
// correctly decompresses content from a packfile at a given offset.
func TestLazyMemoryObject_Reader_DecompressesOnDemand(t *testing.T) {
	content := []byte("Hello, World! This is lazy-loaded content.")
	offset := int64(100) // Start compressed data at offset 100

	packfile := createMockPackfile(t, content, offset)
	oh := FromObjectFormat(formatcfg.SHA1)

	obj := &LazyMemoryObject{
		t:             BlobObject,
		h:             NewHash("1234567890abcdef1234567890abcdef12345678"),
		sz:            int64(len(content)),
		oh:            oh,
		packfile:      packfile,
		contentOffset: offset,
		cacheOnRead:   false,
	}

	// Act: Read the content
	reader, err := obj.Reader()
	require.NoError(t, err)
	defer reader.Close()

	actual, err := io.ReadAll(reader)
	require.NoError(t, err)

	// Assert: Content is decompressed correctly
	assert.Equal(t, content, actual)
}

// TestLazyMemoryObject_Reader_MultipleReads tests that calling Reader()
// multiple times works correctly and decompresses the content each time
// (or uses cache if cacheOnRead is true).
func TestLazyMemoryObject_Reader_MultipleReads(t *testing.T) {
	content := []byte("Test content for multiple reads")
	offset := int64(50)

	packfile := createMockPackfile(t, content, offset)
	oh := FromObjectFormat(formatcfg.SHA1)

	obj := &LazyMemoryObject{
		t:             BlobObject,
		sz:            int64(len(content)),
		oh:            oh,
		packfile:      packfile,
		contentOffset: offset,
		cacheOnRead:   false,
	}

	// First read
	reader1, err := obj.Reader()
	require.NoError(t, err)
	content1, err := io.ReadAll(reader1)
	require.NoError(t, err)
	reader1.Close()

	// Second read
	reader2, err := obj.Reader()
	require.NoError(t, err)
	content2, err := io.ReadAll(reader2)
	require.NoError(t, err)
	reader2.Close()

	// Both reads should return the same content
	assert.Equal(t, content, content1)
	assert.Equal(t, content, content2)
}

// TestLazyMemoryObject_Reader_EmptyBlob tests reading an empty blob.
func TestLazyMemoryObject_Reader_EmptyBlob(t *testing.T) {
	content := []byte{} // Empty blob
	offset := int64(0)

	packfile := createMockPackfile(t, content, offset)
	oh := FromObjectFormat(formatcfg.SHA1)

	obj := &LazyMemoryObject{
		t:             BlobObject,
		sz:            0,
		oh:            oh,
		packfile:      packfile,
		contentOffset: offset,
	}

	reader, err := obj.Reader()
	require.NoError(t, err)
	defer reader.Close()

	actual, err := io.ReadAll(reader)
	require.NoError(t, err)

	assert.Empty(t, actual)
}

// TestLazyMemoryObject_Reader_CachedContent tests that when content is cached,
// subsequent reads use the cache instead of decompressing again.
func TestLazyMemoryObject_Reader_CachedContent(t *testing.T) {
	content := []byte("This content should be cached")

	oh := FromObjectFormat(formatcfg.SHA1)

	obj := &LazyMemoryObject{
		t:      BlobObject,
		sz:     int64(len(content)),
		oh:     oh,
		cached: content, // Pre-cached content
	}

	// Read should use cached content (packfile is nil, but that's okay)
	reader, err := obj.Reader()
	require.NoError(t, err)
	defer reader.Close()

	actual, err := io.ReadAll(reader)
	require.NoError(t, err)

	assert.Equal(t, content, actual)
}

// TestLazyMemoryObject_Reader_WithCacheOnRead tests that when cacheOnRead
// is true, content is cached after the first read.
func TestLazyMemoryObject_Reader_WithCacheOnRead(t *testing.T) {
	content := []byte("Content to cache on read")
	offset := int64(200)

	packfile := createMockPackfile(t, content, offset)
	oh := FromObjectFormat(formatcfg.SHA1)

	obj := &LazyMemoryObject{
		t:             BlobObject,
		sz:            int64(len(content)),
		oh:            oh,
		packfile:      packfile,
		contentOffset: offset,
		cacheOnRead:   true,
	}

	// First read - should decompress and cache
	reader1, err := obj.Reader()
	require.NoError(t, err)
	content1, err := io.ReadAll(reader1)
	require.NoError(t, err)
	reader1.Close()

	// Verify content is cached
	assert.NotNil(t, obj.cached)
	assert.Equal(t, content, obj.cached)

	// Second read - should use cache
	reader2, err := obj.Reader()
	require.NoError(t, err)
	content2, err := io.ReadAll(reader2)
	require.NoError(t, err)
	reader2.Close()

	assert.Equal(t, content, content1)
	assert.Equal(t, content, content2)
}

// TestLazyMemoryObject_Writer tests the Writer functionality.
func TestLazyMemoryObject_Writer(t *testing.T) {
	content := []byte("Content written via Writer")
	oh := FromObjectFormat(formatcfg.SHA1)

	obj := NewLazyMemoryObject(oh)
	obj.SetType(BlobObject)

	// Write content
	writer, err := obj.Writer()
	require.NoError(t, err)

	n, err := writer.Write(content)
	require.NoError(t, err)
	assert.Equal(t, len(content), n)

	err = writer.Close()
	require.NoError(t, err)

	// Verify content is cached and size is set
	assert.Equal(t, content, obj.cached)
	assert.Equal(t, int64(len(content)), obj.Size())

	// Verify we can read it back
	reader, err := obj.Reader()
	require.NoError(t, err)
	defer reader.Close()

	actual, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, content, actual)
}

// TestLazyMemoryObject_ConcurrentReads tests that multiple goroutines can
// safely read from the same LazyMemoryObject concurrently.
func TestLazyMemoryObject_ConcurrentReads(t *testing.T) {
	content := []byte("This content will be read by multiple goroutines concurrently")
	offset := int64(100)

	packfile := createMockPackfile(t, content, offset)
	oh := FromObjectFormat(formatcfg.SHA1)

	obj := &LazyMemoryObject{
		t:             BlobObject,
		sz:            int64(len(content)),
		oh:            oh,
		packfile:      packfile,
		contentOffset: offset,
		cacheOnRead:   true, // Enable caching
	}

	const numGoroutines = 50
	errCh := make(chan error, numGoroutines)
	resultCh := make(chan []byte, numGoroutines)

	// Start many goroutines that all read concurrently
	for i := 0; i < numGoroutines; i++ {
		go func() {
			reader, err := obj.Reader()
			if err != nil {
				errCh <- err
				return
			}
			defer reader.Close()

			data, err := io.ReadAll(reader)
			if err != nil {
				errCh <- err
				return
			}
			resultCh <- data
			errCh <- nil
		}()
	}

	// Collect results
	for i := 0; i < numGoroutines; i++ {
		err := <-errCh
		require.NoError(t, err, "goroutine %d failed", i)
	}

	// Verify all results are correct
	for i := 0; i < numGoroutines; i++ {
		result := <-resultCh
		assert.Equal(t, content, result, "content mismatch in goroutine result")
	}
}

// TestLazyMemoryObject_ConcurrentReadWrite tests concurrent reads and writes.
func TestLazyMemoryObject_ConcurrentReadWrite(t *testing.T) {
	content := []byte("Original content for concurrent read/write test")
	offset := int64(50)

	packfile := createMockPackfile(t, content, offset)
	oh := FromObjectFormat(formatcfg.SHA1)

	obj := &LazyMemoryObject{
		t:             BlobObject,
		sz:            int64(len(content)),
		oh:            oh,
		packfile:      packfile,
		contentOffset: offset,
		cacheOnRead:   true,
	}

	const numReaders = 20

	// Start readers
	readersDone := make(chan bool, numReaders)
	for i := 0; i < numReaders; i++ {
		go func() {
			defer func() { readersDone <- true }()
			for j := 0; j < 10; j++ {
				reader, err := obj.Reader()
				if err != nil {
					t.Error(err)
					return
				}
				_, err = io.ReadAll(reader)
				reader.Close()
				if err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}

	// Wait for all readers
	for i := 0; i < numReaders; i++ {
		<-readersDone
	}

	// Verify object is still valid
	reader, err := obj.Reader()
	require.NoError(t, err)
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	reader.Close()
	assert.Equal(t, content, data)
}

// TestLazyMemoryObject_CorruptedPackfile tests behavior with corrupted zlib data.
func TestLazyMemoryObject_CorruptedPackfile(t *testing.T) {
	oh := FromObjectFormat(formatcfg.SHA1)

	// Create a packfile with invalid zlib data
	corruptedData := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05} // Not valid zlib
	packfile := bytes.NewReader(corruptedData)

	obj := &LazyMemoryObject{
		t:             BlobObject,
		sz:            100,
		oh:            oh,
		packfile:      packfile,
		contentOffset: 0,
		cacheOnRead:   false,
	}

	// Reading should fail with a zlib error
	_, err := obj.Reader()
	assert.Error(t, err, "should fail with corrupted zlib data")
	assert.Contains(t, err.Error(), "zlib", "error should mention zlib")
}

// TestLazyMemoryObject_NoPackfile tests reading when packfile is nil.
func TestLazyMemoryObject_NoPackfile(t *testing.T) {
	oh := FromObjectFormat(formatcfg.SHA1)

	obj := &LazyMemoryObject{
		t:        BlobObject,
		sz:       100,
		oh:       oh,
		packfile: nil, // No packfile set
	}

	// Should return empty reader, not an error
	reader, err := obj.Reader()
	require.NoError(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Empty(t, data, "should return empty data when packfile is nil")
}

// TestLazyMemoryObject_CacheNotifier tests that the cache notifier is called.
func TestLazyMemoryObject_CacheNotifier(t *testing.T) {
	content := []byte("Content that triggers cache notification")
	offset := int64(0)

	packfile := createMockPackfile(t, content, offset)
	oh := FromObjectFormat(formatcfg.SHA1)

	notifierCalled := false
	obj := &LazyMemoryObject{
		t:             BlobObject,
		sz:            int64(len(content)),
		oh:            oh,
		packfile:      packfile,
		contentOffset: offset,
		cacheOnRead:   true,
		onCached: func() {
			notifierCalled = true
		},
	}

	// First read should trigger notifier
	reader, err := obj.Reader()
	require.NoError(t, err)
	_, err = io.ReadAll(reader)
	require.NoError(t, err)
	reader.Close()

	assert.True(t, notifierCalled, "cache notifier should have been called")

	// Reset and read again - notifier should NOT be called again
	notifierCalled = false
	reader, err = obj.Reader()
	require.NoError(t, err)
	_, err = io.ReadAll(reader)
	require.NoError(t, err)
	reader.Close()

	assert.False(t, notifierCalled, "cache notifier should not be called on subsequent reads")
}