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

// TestLazyDeltaObject_ImplementsEncodedObject verifies that LazyDeltaObject
// implements the EncodedObject interface at compile time.
func TestLazyDeltaObject_ImplementsEncodedObject(t *testing.T) {
	var _ EncodedObject = (*LazyDeltaObject)(nil)
}

// TestLazyDeltaObject_BasicProperties tests that LazyDeltaObject correctly
// stores and retrieves basic object properties.
func TestLazyDeltaObject_BasicProperties(t *testing.T) {
	testHash := NewHash("1234567890abcdef1234567890abcdef12345678")
	oh := FromObjectFormat(formatcfg.SHA1)

	obj := &LazyDeltaObject{
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

// TestLazyDeltaObject_SetType tests the SetType method.
func TestLazyDeltaObject_SetType(t *testing.T) {
	obj := &LazyDeltaObject{}

	obj.SetType(TreeObject)
	assert.Equal(t, TreeObject, obj.Type())

	obj.SetType(CommitObject)
	assert.Equal(t, CommitObject, obj.Type())
}

// TestLazyDeltaObject_SetSize tests the SetSize method.
func TestLazyDeltaObject_SetSize(t *testing.T) {
	obj := &LazyDeltaObject{}

	obj.SetSize(2048)
	assert.Equal(t, int64(2048), obj.Size())

	obj.SetSize(0)
	assert.Equal(t, int64(0), obj.Size())
}

// mockLazyDeltaResolver is a test implementation of LazyDeltaResolver.
type mockLazyDeltaResolver struct {
	objects         map[Hash]EncodedObject
	objectsByOffset map[int64]EncodedObject
}

func newMockLazyDeltaResolver() *mockLazyDeltaResolver {
	return &mockLazyDeltaResolver{
		objects:         make(map[Hash]EncodedObject),
		objectsByOffset: make(map[int64]EncodedObject),
	}
}

func (m *mockLazyDeltaResolver) EncodedObject(t ObjectType, h Hash) (EncodedObject, error) {
	if obj, ok := m.objects[h]; ok {
		return obj, nil
	}
	return nil, ErrObjectNotFound
}

func (m *mockLazyDeltaResolver) EncodedObjectByOffset(offset int64) (EncodedObject, error) {
	if obj, ok := m.objectsByOffset[offset]; ok {
		return obj, nil
	}
	return nil, ErrObjectNotFound
}

// mockEncodedObject is a simple test object.
type mockEncodedObject struct {
	t       ObjectType
	h       Hash
	sz      int64
	content []byte
}

func (m *mockEncodedObject) Type() ObjectType { return m.t }
func (m *mockEncodedObject) SetType(t ObjectType) { m.t = t }
func (m *mockEncodedObject) Size() int64 { return m.sz }
func (m *mockEncodedObject) SetSize(s int64) { m.sz = s }
func (m *mockEncodedObject) Hash() Hash { return m.h }
func (m *mockEncodedObject) Reader() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(m.content)), nil
}
func (m *mockEncodedObject) Writer() (io.WriteCloser, error) {
	return nil, nil
}

// createDeltaData creates a simple delta that transforms base into target.
// This is a minimal delta format implementation for testing purposes.
// Delta format: [src_size varint] [target_size varint] [instructions...]
// Instruction: 0x80 | copy_offset_bits | size_bits (copy from base)
//              or 0x01-0x7F (insert that many bytes)
func createDeltaData(t *testing.T, base, target []byte) []byte {
	t.Helper()

	var delta bytes.Buffer

	// Write source size as varint
	writeVarint(&delta, len(base))

	// Write target size as varint
	writeVarint(&delta, len(target))

	// Simple strategy: just insert all target bytes
	// This is not efficient but works for testing
	remaining := len(target)
	offset := 0
	for remaining > 0 {
		// Insert instruction can only handle up to 127 bytes at a time
		insertLen := remaining
		if insertLen > 127 {
			insertLen = 127
		}
		delta.WriteByte(byte(insertLen))
		delta.Write(target[offset : offset+insertLen])
		offset += insertLen
		remaining -= insertLen
	}

	return delta.Bytes()
}

// writeVarint writes a variable-length integer to the buffer.
func writeVarint(buf *bytes.Buffer, n int) {
	for {
		b := byte(n & 0x7F)
		n >>= 7
		if n > 0 {
			b |= 0x80
		}
		buf.WriteByte(b)
		if n == 0 {
			break
		}
	}
}

// zlibCompressDelta compresses delta data for storing in a mock packfile.
func zlibCompressDelta(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zlib.NewWriter(&buf)
	_, err := w.Write(data)
	require.NoError(t, err)
	err = w.Close()
	require.NoError(t, err)
	return buf.Bytes()
}

// createMockPackfileWithDelta creates a mock packfile with compressed delta at offset.
func createMockPackfileWithDelta(t *testing.T, deltaData []byte, offset int64) io.ReaderAt {
	t.Helper()
	compressed := zlibCompressDelta(t, deltaData)
	packfile := make([]byte, offset+int64(len(compressed)))
	copy(packfile[offset:], compressed)
	return bytes.NewReader(packfile)
}

// mockDeltaPatcher is a simple delta patcher that uses the same algorithm as createDeltaData.
func mockDeltaPatcher(base, delta []byte) ([]byte, error) {
	// Read varints for source and target size
	srcSize, delta := readVarint(delta)
	_ = srcSize // not used in this simple patcher
	targetSize, delta := readVarint(delta)
	
	result := make([]byte, 0, targetSize)
	
	for len(delta) > 0 {
		cmd := delta[0]
		delta = delta[1:]
		
		if cmd&0x80 != 0 {
			// Copy command - not implemented in this simple version
			// Skip for now since createDeltaData only uses insert
			continue
		} else if cmd > 0 {
			// Insert command
			insertLen := int(cmd)
			if insertLen > len(delta) {
				insertLen = len(delta)
			}
			result = append(result, delta[:insertLen]...)
			delta = delta[insertLen:]
		}
	}
	
	return result, nil
}

func readVarint(data []byte) (int, []byte) {
	var n int
	shift := uint(0)
	for len(data) > 0 {
		b := data[0]
		data = data[1:]
		n |= int(b&0x7F) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
	}
	return n, data
}

// TestLazyDeltaObject_Reader_WithCachedContent tests that when content is cached,
// subsequent reads use the cache.
func TestLazyDeltaObject_Reader_WithCachedContent(t *testing.T) {
	content := []byte("This content is already cached")
	oh := FromObjectFormat(formatcfg.SHA1)

	obj := &LazyDeltaObject{
		t:      BlobObject,
		sz:     int64(len(content)),
		oh:     oh,
		cached: content, // Pre-cached content
	}

	// Read should use cached content
	reader, err := obj.Reader()
	require.NoError(t, err)
	defer reader.Close()

	actual, err := io.ReadAll(reader)
	require.NoError(t, err)

	assert.Equal(t, content, actual)
}

// TestLazyDeltaObject_Reader_ResolvesREFDelta tests resolving REF_DELTA.
func TestLazyDeltaObject_Reader_ResolvesREFDelta(t *testing.T) {
	baseContent := []byte("Hello, World! This is the base content.")
	targetContent := []byte("Hello, World! This is the modified target content.")
	
	deltaData := createDeltaData(t, baseContent, targetContent)
	oh := FromObjectFormat(formatcfg.SHA1)
	baseHash := NewHash("0000000000000000000000000000000000000001")
	deltaOffset := int64(100)

	// Create mock base object
	baseObj := &mockEncodedObject{
		t:       BlobObject,
		h:       baseHash,
		sz:      int64(len(baseContent)),
		content: baseContent,
	}

	// Create resolver with base object
	resolver := newMockLazyDeltaResolver()
	resolver.objects[baseHash] = baseObj

	// Create packfile with compressed delta
	packfile := createMockPackfileWithDelta(t, deltaData, deltaOffset)

	obj := &LazyDeltaObject{
		t:           BlobObject,
		h:           NewHash("1234567890abcdef1234567890abcdef12345678"),
		sz:          int64(len(targetContent)),
		oh:          oh,
		deltaType:   REFDelta,
		baseHash:    baseHash,
		packfile:    packfile,
		deltaOffset: deltaOffset,
		resolver:    resolver,
		patcher:     mockDeltaPatcher,
		cacheOnRead: true,
	}

	// Act: Read the content
	reader, err := obj.Reader()
	require.NoError(t, err)
	defer reader.Close()

	actual, err := io.ReadAll(reader)
	require.NoError(t, err)

	// Assert: Content is resolved correctly
	assert.Equal(t, targetContent, actual)
}

// TestLazyDeltaObject_Reader_ResolvesOFSDelta tests resolving OFS_DELTA.
func TestLazyDeltaObject_Reader_ResolvesOFSDelta(t *testing.T) {
	baseContent := []byte("Base content for OFS_DELTA test")
	targetContent := []byte("Target content after OFS_DELTA resolution")
	
	deltaData := createDeltaData(t, baseContent, targetContent)
	oh := FromObjectFormat(formatcfg.SHA1)
	baseOffset := int64(500)
	deltaOffset := int64(100)

	// Create mock base object
	baseObj := &mockEncodedObject{
		t:       BlobObject,
		sz:      int64(len(baseContent)),
		content: baseContent,
	}

	// Create resolver with base object by offset
	resolver := newMockLazyDeltaResolver()
	resolver.objectsByOffset[baseOffset] = baseObj

	// Create packfile with compressed delta
	packfile := createMockPackfileWithDelta(t, deltaData, deltaOffset)

	obj := &LazyDeltaObject{
		t:           BlobObject,
		h:           NewHash("abcdef1234567890abcdef1234567890abcdef12"),
		sz:          int64(len(targetContent)),
		oh:          oh,
		deltaType:   OFSDelta,
		baseOffset:  baseOffset,
		packfile:    packfile,
		deltaOffset: deltaOffset,
		resolver:    resolver,
		patcher:     mockDeltaPatcher,
		cacheOnRead: true,
	}

	// Act: Read the content
	reader, err := obj.Reader()
	require.NoError(t, err)
	defer reader.Close()

	actual, err := io.ReadAll(reader)
	require.NoError(t, err)

	// Assert: Content is resolved correctly
	assert.Equal(t, targetContent, actual)
}

// TestLazyDeltaObject_Writer tests the Writer functionality.
func TestLazyDeltaObject_Writer(t *testing.T) {
	content := []byte("Content written via Writer")
	oh := FromObjectFormat(formatcfg.SHA1)

	obj := &LazyDeltaObject{
		oh: oh,
	}
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

// TestLazyDeltaObject_Reader_NoResolver tests reading when resolver is nil.
func TestLazyDeltaObject_Reader_NoResolver(t *testing.T) {
	oh := FromObjectFormat(formatcfg.SHA1)

	obj := &LazyDeltaObject{
		t:        BlobObject,
		sz:       100,
		oh:       oh,
		resolver: nil, // No resolver set
	}

	// Should return error because resolver is required
	_, err := obj.Reader()
	assert.Error(t, err, "should fail when resolver is nil")
}

// TestLazyDeltaObject_Reader_BaseNotFound tests error handling when
// base object cannot be found.
func TestLazyDeltaObject_Reader_BaseNotFound(t *testing.T) {
	oh := FromObjectFormat(formatcfg.SHA1)
	baseHash := NewHash("0000000000000000000000000000000000000999")
	deltaOffset := int64(100)
	deltaData := createDeltaData(t, []byte("base"), []byte("target"))
	packfile := createMockPackfileWithDelta(t, deltaData, deltaOffset)

	// Create empty resolver (no base object)
	resolver := newMockLazyDeltaResolver()

	obj := &LazyDeltaObject{
		t:           BlobObject,
		sz:          100,
		oh:          oh,
		deltaType:   REFDelta,
		baseHash:    baseHash,
		packfile:    packfile,
		deltaOffset: deltaOffset,
		resolver:    resolver,
		patcher:     mockDeltaPatcher,
	}

	// Should return error because base is not found
	_, err := obj.Reader()
	assert.Error(t, err, "should fail when base object not found")
}

// TestLazyDeltaObject_HasCachedContent tests the HasCachedContent method.
func TestLazyDeltaObject_HasCachedContent(t *testing.T) {
	oh := FromObjectFormat(formatcfg.SHA1)

	// Not cached initially
	obj := &LazyDeltaObject{
		t:  BlobObject,
		oh: oh,
	}
	assert.False(t, obj.HasCachedContent())

	// Cached when content is set
	obj.cached = []byte("cached content")
	assert.True(t, obj.HasCachedContent())
}

// TestNewLazyDeltaObject tests the constructor.
func TestNewLazyDeltaObject(t *testing.T) {
	oh := FromObjectFormat(formatcfg.SHA1)

	obj := NewLazyDeltaObject(oh)

	assert.NotNil(t, obj)
	assert.Equal(t, oh, obj.oh)
	assert.Empty(t, obj.h)
	assert.Equal(t, ObjectType(0), obj.t)
	assert.Equal(t, int64(0), obj.sz)
}

// TestLazyDeltaObject_SetLazyDeltaFields tests setting lazy delta fields.
func TestLazyDeltaObject_SetLazyDeltaFields(t *testing.T) {
	oh := FromObjectFormat(formatcfg.SHA1)
	resolver := newMockLazyDeltaResolver()
	baseHash := NewHash("0000000000000000000000000000000000000005")
	packfile := bytes.NewReader([]byte{})

	obj := NewLazyDeltaObject(oh)
	obj.SetLazyDeltaFields(
		packfile,
		100,          // deltaOffset
		REFDelta,     // deltaType
		baseHash,     // baseHash
		0,            // baseOffset (not used for REFDelta)
		resolver,
		mockDeltaPatcher,
		true, // cacheOnRead
	)

	assert.Equal(t, packfile, obj.packfile)
	assert.Equal(t, int64(100), obj.deltaOffset)
	assert.Equal(t, REFDelta, obj.deltaType)
	assert.Equal(t, baseHash, obj.baseHash)
	assert.Equal(t, resolver, obj.resolver)
	assert.True(t, obj.cacheOnRead)
}

// TestLazyDeltaObject_CacheNotifier tests that the cache notifier is called.
func TestLazyDeltaObject_CacheNotifier(t *testing.T) {
	baseContent := []byte("Base for notifier test")
	targetContent := []byte("Target for notifier test")
	
	deltaData := createDeltaData(t, baseContent, targetContent)
	oh := FromObjectFormat(formatcfg.SHA1)
	baseHash := NewHash("0000000000000000000000000000000000000006")
	deltaOffset := int64(100)

	// Create mock base object
	baseObj := &mockEncodedObject{
		t:       BlobObject,
		h:       baseHash,
		sz:      int64(len(baseContent)),
		content: baseContent,
	}

	// Create resolver with base object
	resolver := newMockLazyDeltaResolver()
	resolver.objects[baseHash] = baseObj

	// Create packfile with compressed delta
	packfile := createMockPackfileWithDelta(t, deltaData, deltaOffset)

	notifierCalled := false
	obj := &LazyDeltaObject{
		t:           BlobObject,
		sz:          int64(len(targetContent)),
		oh:          oh,
		deltaType:   REFDelta,
		baseHash:    baseHash,
		packfile:    packfile,
		deltaOffset: deltaOffset,
		resolver:    resolver,
		patcher:     mockDeltaPatcher,
		cacheOnRead: true,
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
