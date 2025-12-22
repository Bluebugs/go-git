package plumbing

import (
	"bytes"
	"fmt"
	"io"
	"sync"
)

// Compile-time interface assertion
var _ EncodedObject = (*LazyDeltaObject)(nil)

// DeltaType represents the type of delta reference.
type DeltaType int

const (
	// OFSDelta is an offset-based delta (OFS_DELTA).
	OFSDelta DeltaType = iota
	// REFDelta is a reference-based delta (REF_DELTA).
	REFDelta
)

// LazyDeltaResolver is an interface for resolving base objects needed for delta patching.
// This is typically implemented by ObjectStorage to fetch base objects.
type LazyDeltaResolver interface {
	// EncodedObject retrieves an object by type and hash.
	EncodedObject(t ObjectType, h Hash) (EncodedObject, error)
}

// DeltaPatcher applies delta instructions to a base object.
// This function signature matches the packfile.PatchDelta function.
type DeltaPatcher func(base, delta []byte) ([]byte, error)

// NewLazyDeltaObject creates a new LazyDeltaObject with the given object hasher.
func NewLazyDeltaObject(oh *ObjectHasher) *LazyDeltaObject {
	return &LazyDeltaObject{oh: oh}
}

// LazyDeltaObject is an in-memory implementation of EncodedObject that defers
// delta resolution until Reader() is called. This is useful for memory-efficient
// operations where delta objects may not need to be read.
//
// Unlike eagerly-resolved delta objects which store the full patched content,
// LazyDeltaObject stores the delta instructions and base object reference.
// The content is only computed when Reader() is called.
//
// Delta chain resolution:
// - For OFS_DELTA: Uses baseOffset to locate base object in packfile
// - For REF_DELTA: Uses baseHash to look up base object in storage
// - Base objects may themselves be lazy deltas (recursion is handled)
type LazyDeltaObject struct {
	t  ObjectType // Final (resolved) type, not delta type
	h  Hash
	sz int64
	oh *ObjectHasher

	// Delta reference fields (one of these is used based on deltaType)
	deltaType  DeltaType
	baseHash   Hash  // For REF_DELTA: hash of base object
	baseOffset int64 // For OFS_DELTA: offset to base object in packfile

	// Packfile access for reading delta instructions
	packfile    io.ReaderAt    // Reference to packfile in memory
	deltaOffset int64          // Offset to delta instructions in packfile
	zlibReader  ZlibReaderFunc // Optional pooled zlib reader factory

	// Resolver for fetching base objects
	resolver LazyDeltaResolver

	// Delta patcher function (injected to avoid circular imports)
	patcher DeltaPatcher

	// Cached content (populated after first read if cacheOnRead is true)
	mu          sync.Mutex
	cached      []byte
	cacheOnRead bool
	onCached    CacheNotifier // Called when content is first cached
}

// Hash returns the object Hash.
func (o *LazyDeltaObject) Hash() Hash {
	return o.h
}

// Type returns the ObjectType (the resolved type, not delta type).
func (o *LazyDeltaObject) Type() ObjectType {
	return o.t
}

// SetType sets the ObjectType.
func (o *LazyDeltaObject) SetType(t ObjectType) {
	o.t = t
}

// Size returns the size of the resolved object.
func (o *LazyDeltaObject) Size() int64 {
	return o.sz
}

// SetSize sets the object size.
func (o *LazyDeltaObject) SetSize(s int64) {
	o.sz = s
}

// Reader returns an io.ReadCloser used to read the object's content.
// This is where the lazy delta resolution happens - delta is applied
// on-demand when this method is called.
//
// The resolution process:
// 1. Fetch the base object (may recurse if base is also a lazy delta)
// 2. Read and decompress delta instructions from packfile
// 3. Apply delta patch to get final content
// 4. Cache result if cacheOnRead is true
func (o *LazyDeltaObject) Reader() (io.ReadCloser, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	// If we have cached content, return it
	if o.cached != nil {
		return nopCloser{bytes.NewReader(o.cached)}, nil
	}

	// If resolver is not set, we can't resolve the delta
	if o.resolver == nil {
		return nil, fmt.Errorf("no resolver set for lazy delta object")
	}

	// If packfile is not set, return error
	if o.packfile == nil {
		return nil, fmt.Errorf("packfile not set for lazy delta object")
	}

	// If patcher is not set, return error
	if o.patcher == nil {
		return nil, fmt.Errorf("delta patcher not set for lazy delta object")
	}

	// Resolve and apply delta
	content, err := o.resolveDelta()
	if err != nil {
		return nil, fmt.Errorf("failed to resolve lazy delta: %w", err)
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

// resolveDelta fetches base object, reads delta instructions, and applies the patch.
// This method must be called with the mutex held.
func (o *LazyDeltaObject) resolveDelta() ([]byte, error) {
	// 1. Get base object content
	baseContent, err := o.getBaseContent()
	if err != nil {
		return nil, fmt.Errorf("failed to get base content: %w", err)
	}

	// 2. Read and decompress delta instructions from packfile
	deltaData, err := o.readDeltaInstructions()
	if err != nil {
		return nil, fmt.Errorf("failed to read delta instructions: %w", err)
	}

	// 3. Apply delta patch
	result, err := o.patcher(baseContent, deltaData)
	if err != nil {
		return nil, fmt.Errorf("failed to apply delta: %w", err)
	}

	return result, nil
}

// getBaseContent retrieves the content of the base object.
// For OFS_DELTA, we need a special mechanism since we have an offset.
// For REF_DELTA, we use the resolver to fetch by hash.
func (o *LazyDeltaObject) getBaseContent() ([]byte, error) {
	var baseObj EncodedObject
	var err error

	switch o.deltaType {
	case REFDelta:
		// Fetch base by hash
		baseObj, err = o.resolver.EncodedObject(AnyObject, o.baseHash)
		if err != nil {
			return nil, fmt.Errorf("base object %s not found: %w", o.baseHash, err)
		}
	case OFSDelta:
		// For OFS_DELTA, we need to use the OffsetResolver if available
		if offsetResolver, ok := o.resolver.(OffsetResolver); ok {
			baseObj, err = offsetResolver.EncodedObjectByOffset(o.baseOffset)
			if err != nil {
				return nil, fmt.Errorf("base object at offset %d not found: %w", o.baseOffset, err)
			}
		} else {
			return nil, fmt.Errorf("resolver does not support offset-based lookup")
		}
	default:
		return nil, fmt.Errorf("unknown delta type: %d", o.deltaType)
	}

	// Read base content (this may trigger recursive resolution if base is also lazy)
	reader, err := baseObj.Reader()
	if err != nil {
		return nil, fmt.Errorf("failed to read base object: %w", err)
	}
	defer reader.Close()

	content, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read base content: %w", err)
	}

	return content, nil
}

// readDeltaInstructions reads and decompresses the delta instructions from the packfile.
func (o *LazyDeltaObject) readDeltaInstructions() ([]byte, error) {
	// Create a section reader starting at the delta offset.
	// Use a large max size as a safeguard.
	const maxCompressedSize = 1 << 30 // 1GB max compressed data read limit
	sectionReader := io.NewSectionReader(o.packfile, o.deltaOffset, maxCompressedSize)

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

	// Read all decompressed delta instructions
	deltaData, err := io.ReadAll(zlibReader)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress delta: %w", err)
	}

	return deltaData, nil
}

// Writer returns an io.WriteCloser used to write the object's content.
// For LazyDeltaObject, writing will cache the content in memory
// (bypassing delta resolution).
func (o *LazyDeltaObject) Writer() (io.WriteCloser, error) {
	return &lazyDeltaObjectWriter{obj: o}, nil
}

// lazyDeltaObjectWriter is a writer that caches content in the LazyDeltaObject.
type lazyDeltaObjectWriter struct {
	obj *LazyDeltaObject
	buf bytes.Buffer
}

func (w *lazyDeltaObjectWriter) Write(p []byte) (n int, err error) {
	return w.buf.Write(p)
}

func (w *lazyDeltaObjectWriter) Close() error {
	w.obj.mu.Lock()
	defer w.obj.mu.Unlock()

	w.obj.cached = w.buf.Bytes()
	w.obj.sz = int64(len(w.obj.cached))

	return nil
}

// SetHash sets the object hash.
func (o *LazyDeltaObject) SetHash(h Hash) {
	o.h = h
}

// SetLazyDeltaFields sets the lazy delta-specific fields for this object.
// This configures the object to resolve deltas on-demand.
//
// Parameters:
//   - packfile: io.ReaderAt for accessing packfile data
//   - deltaOffset: offset to compressed delta instructions in packfile
//   - deltaType: OFSDelta or REFDelta
//   - baseHash: hash of base object (for REFDelta)
//   - baseOffset: offset of base object in packfile (for OFSDelta)
//   - resolver: for fetching base objects
//   - patcher: function to apply delta instructions
//   - cacheOnRead: whether to cache resolved content
func (o *LazyDeltaObject) SetLazyDeltaFields(
	packfile io.ReaderAt,
	deltaOffset int64,
	deltaType DeltaType,
	baseHash Hash,
	baseOffset int64,
	resolver LazyDeltaResolver,
	patcher DeltaPatcher,
	cacheOnRead bool,
) {
	o.packfile = packfile
	o.deltaOffset = deltaOffset
	o.deltaType = deltaType
	o.baseHash = baseHash
	o.baseOffset = baseOffset
	o.resolver = resolver
	o.patcher = patcher
	o.cacheOnRead = cacheOnRead
}

// SetZlibReaderFunc sets a custom zlib reader factory function.
// This can be used to inject a pooled zlib reader for better performance.
func (o *LazyDeltaObject) SetZlibReaderFunc(fn ZlibReaderFunc) {
	o.zlibReader = fn
}

// SetCacheNotifier sets a callback function that will be called when the object's
// content is cached for the first time.
func (o *LazyDeltaObject) SetCacheNotifier(notifier CacheNotifier) {
	o.onCached = notifier
}

// HasCachedContent returns true if this lazy object has already resolved
// and cached its content in memory.
func (o *LazyDeltaObject) HasCachedContent() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.cached != nil
}

// OffsetResolver is an interface for resolving objects by their packfile offset.
// This is needed for OFS_DELTA resolution where we only have an offset, not a hash.
type OffsetResolver interface {
	LazyDeltaResolver
	// EncodedObjectByOffset retrieves an object by its offset in the packfile.
	EncodedObjectByOffset(offset int64) (EncodedObject, error)
}
