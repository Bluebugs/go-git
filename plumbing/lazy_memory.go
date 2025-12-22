package plumbing

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
	"sync"
)

// Compile-time interface assertion
var _ EncodedObject = (*LazyMemoryObject)(nil)

// ZlibReaderFunc is a function that creates a zlib reader from an io.Reader.
// The returned io.ReadCloser must be closed after use.
// The second return value is an optional cleanup function that should be called
// after the reader is closed (e.g., to return the reader to a pool).
type ZlibReaderFunc func(r io.Reader) (io.ReadCloser, func(), error)

// DefaultZlibReaderFunc creates a standard zlib reader without pooling.
func DefaultZlibReaderFunc(r io.Reader) (io.ReadCloser, func(), error) {
	zr, err := zlib.NewReader(r)
	return zr, func() {}, err
}

// CacheNotifier is a callback function that is called when a LazyMemoryObject
// caches its content for the first time.
type CacheNotifier func()

// NewLazyMemoryObject creates a new LazyMemoryObject with the given object hasher.
func NewLazyMemoryObject(oh *ObjectHasher) *LazyMemoryObject {
	return &LazyMemoryObject{oh: oh}
}

// LazyMemoryObject is an in-memory implementation of EncodedObject that defers
// decompression until Reader() is called. This is useful for memory-efficient
// operations where objects may not need to be read.
//
// Unlike MemoryObject which stores the full decompressed content in memory,
// LazyMemoryObject stores a reference to the packfile and the offset where
// the compressed content is located. The content is only decompressed when
// Reader() is called.
type LazyMemoryObject struct {
	t  ObjectType
	h  Hash
	sz int64
	oh *ObjectHasher

	// Lazy loading fields
	packfile      io.ReaderAt    // Reference to packfile in memory
	contentOffset int64          // Offset to compressed data in packfile
	zlibReader    ZlibReaderFunc // Optional pooled zlib reader factory

	// Cached content (populated after first read if cacheOnRead is true)
	mu          sync.Mutex
	cached      []byte
	cacheOnRead bool
	onCached    CacheNotifier // Called when content is first cached
}

// Hash returns the object Hash. For LazyMemoryObject, the hash must be set
// explicitly as it cannot be computed from content that hasn't been decompressed yet.
func (o *LazyMemoryObject) Hash() Hash {
	return o.h
}

// Type returns the ObjectType.
func (o *LazyMemoryObject) Type() ObjectType {
	return o.t
}

// SetType sets the ObjectType.
func (o *LazyMemoryObject) SetType(t ObjectType) {
	o.t = t
}

// Size returns the size of the object.
func (o *LazyMemoryObject) Size() int64 {
	return o.sz
}

// SetSize sets the object size.
func (o *LazyMemoryObject) SetSize(s int64) {
	o.sz = s
}

// Reader returns an io.ReadCloser used to read the object's content.
// This is where the lazy decompression happens - content is decompressed
// on-demand when this method is called.
//
// For a LazyMemoryObject, this reader is seekable if the content has been
// cached or if it's the first read.
func (o *LazyMemoryObject) Reader() (io.ReadCloser, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	// If we have cached content, return it
	if o.cached != nil {
		return nopCloser{bytes.NewReader(o.cached)}, nil
	}

	// If packfile is not set, return empty reader
	if o.packfile == nil {
		return nopCloser{bytes.NewReader([]byte{})}, nil
	}

	// Decompress content from packfile at the specified offset
	content, err := o.decompressContent()
	if err != nil {
		return nil, fmt.Errorf("failed to decompress lazy object content: %w", err)
	}

	// Cache if configured to do so
	if o.cacheOnRead {
		o.cached = content
		// Notify storage that this object has been cached (for packfile eviction)
		if o.onCached != nil {
			o.onCached()
		}
	}

	return nopCloser{bytes.NewReader(content)}, nil
}

// decompressContent decompresses the content from the packfile.
// This method must be called with the mutex held.
func (o *LazyMemoryObject) decompressContent() ([]byte, error) {
	// Create a section reader starting at the content offset.
	// We don't know the exact compressed size, so we use a large max size.
	// Note: This 1GB limit is a safeguard against reading beyond the packfile.
	// The zlib reader will stop at the end of the compressed stream, so this
	// limit only affects malformed packfiles. For objects larger than 1GB,
	// consider using filesystem-based storage instead of memory storage.
	const maxCompressedSize = 1 << 30 // 1GB max compressed data read limit
	sectionReader := io.NewSectionReader(o.packfile, o.contentOffset, maxCompressedSize)

	// Create zlib reader to decompress (use pooled reader if available)
	zlibReaderFunc := o.zlibReader
	if zlibReaderFunc == nil {
		zlibReaderFunc = DefaultZlibReaderFunc
	}
	zlibReader, cleanup, err := zlibReaderFunc(sectionReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create zlib reader: %w", err)
	}
	defer zlibReader.Close()
	defer cleanup()

	// Read the decompressed content
	var buf bytes.Buffer
	if o.sz > 0 {
		buf.Grow(int(o.sz)) // Pre-allocate if we know the size
	}

	_, err = io.Copy(&buf, zlibReader)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress content: %w", err)
	}

	return buf.Bytes(), nil
}

// Writer returns an io.WriteCloser used to write the object's content.
// For LazyMemoryObject, writing will cache the content in memory.
func (o *LazyMemoryObject) Writer() (io.WriteCloser, error) {
	return &lazyMemoryObjectWriter{obj: o}, nil
}

// lazyMemoryObjectWriter is a writer that caches content in the LazyMemoryObject.
type lazyMemoryObjectWriter struct {
	obj *LazyMemoryObject
	buf bytes.Buffer
}

func (w *lazyMemoryObjectWriter) Write(p []byte) (n int, err error) {
	return w.buf.Write(p)
}

func (w *lazyMemoryObjectWriter) Close() error {
	w.obj.mu.Lock()
	defer w.obj.mu.Unlock()

	w.obj.cached = w.buf.Bytes()
	w.obj.sz = int64(len(w.obj.cached))

	return nil
}

// SetHash sets the object hash. This is used when creating a LazyMemoryObject
// from packfile data where the hash has already been calculated.
func (o *LazyMemoryObject) SetHash(h Hash) {
	o.h = h
}

// SetLazyFields sets the lazy loading specific fields for this object.
// This configures the object to decompress on-demand from the packfile.
func (o *LazyMemoryObject) SetLazyFields(packfile io.ReaderAt, contentOffset int64, cacheOnRead bool) {
	o.packfile = packfile
	o.contentOffset = contentOffset
	o.cacheOnRead = cacheOnRead
}

// SetZlibReaderFunc sets a custom zlib reader factory function.
// This can be used to inject a pooled zlib reader for better performance.
func (o *LazyMemoryObject) SetZlibReaderFunc(fn ZlibReaderFunc) {
	o.zlibReader = fn
}

// SetCacheNotifier sets a callback function that will be called when the object's
// content is cached for the first time. This is useful for tracking when lazy
// objects are fully loaded and enabling packfile eviction.
func (o *LazyMemoryObject) SetCacheNotifier(notifier CacheNotifier) {
	o.onCached = notifier
}

// HasCachedContent returns true if this lazy object has already decompressed
// and cached its content in memory. This is useful for verifying that lazy
// loading is working correctly - objects should not have cached content until
// they are actually read.
func (o *LazyMemoryObject) HasCachedContent() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.cached != nil
}
