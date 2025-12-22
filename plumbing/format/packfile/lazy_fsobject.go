package packfile

import (
	"bufio"
	"io"
	"sync"

	billy "github.com/go-git/go-billy/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/cache"
	"github.com/go-git/go-git/v6/plumbing/format/idxfile"
	"github.com/go-git/go-git/v6/utils/ioutil"
	gogitsync "github.com/go-git/go-git/v6/utils/sync"
)

// LazyFSObject is a fully lazy object loaded from a packfile via a trusted index.
//
// Unlike FSObject, which requires the type and size to be known upfront (from
// parsing the packfile header), LazyFSObject defers even header parsing until
// the object's Type(), Size(), or Reader() is first accessed.
//
// This enables loading objects from a trusted pack index without any packfile
// I/O until the object is actually needed. This is useful for:
//   - Opening existing repositories without decompressing anything
//   - Sparse checkouts where most objects are never accessed
//   - Operations that only need object hashes (already in the index)
//
// The tradeoff is that Type() and Size() now require I/O (header parsing),
// whereas FSObject has these available immediately.
type LazyFSObject struct {
	hash     plumbing.Hash
	offset   int64
	index    idxfile.Index
	fs       billy.Filesystem
	packPath string
	cache    cache.Object

	// objectIdSize for SHA1 vs SHA256
	objectIdSize int

	// Lazily loaded header information
	mu       sync.Mutex
	resolved bool
	typ      plumbing.ObjectType
	size     int64
	// contentOffset is where the compressed data starts (after header)
	contentOffset int64
	resolveErr    error
}

// NewLazyFSObject creates a new fully lazy filesystem object.
//
// The object's type and size are not known until the header is parsed,
// which happens lazily on first access to Type(), Size(), or Reader().
//
// Parameters:
//   - hash: The object's hash (from the index, trusted)
//   - offset: The object's offset in the packfile (from the index)
//   - index: The pack index for potential delta resolution
//   - fs: The filesystem containing the packfile
//   - packPath: Path to the packfile
//   - cache: Object cache for resolved objects
//   - objectIdSize: Size of object IDs (20 for SHA1, 32 for SHA256)
func NewLazyFSObject(
	hash plumbing.Hash,
	offset int64,
	index idxfile.Index,
	fs billy.Filesystem,
	packPath string,
	cache cache.Object,
	objectIdSize int,
) *LazyFSObject {
	return &LazyFSObject{
		hash:         hash,
		offset:       offset,
		index:        index,
		fs:           fs,
		packPath:     packPath,
		cache:        cache,
		objectIdSize: objectIdSize,
	}
}

// resolve parses the object header from the packfile to get type and size.
// This is called lazily on first access to Type(), Size(), or Reader().
func (o *LazyFSObject) resolve() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.resolved {
		return o.resolveErr
	}

	o.resolved = true
	o.resolveErr = o.doResolve()
	return o.resolveErr
}

func (o *LazyFSObject) doResolve() error {
	// Open packfile
	f, err := o.fs.Open(o.packPath)
	if err != nil {
		return err
	}
	defer f.Close()

	// Seek to object offset
	_, err = f.Seek(o.offset, io.SeekStart)
	if err != nil {
		return err
	}

	// Parse header using scanner
	br := gogitsync.GetBufioReader(f)
	defer gogitsync.PutBufioReader(br)

	var opts []ScannerOption
	if o.objectIdSize == 32 {
		opts = append(opts, WithSHA256())
	}

	scanner := NewScanner(f, opts...)
	err = scanner.SeekFromStart(o.offset)
	if err != nil {
		return err
	}

	if !scanner.Scan() {
		if err := scanner.Error(); err != nil {
			return err
		}
		return plumbing.ErrObjectNotFound
	}

	header := scanner.Data().Value().(ObjectHeader)

	// Handle delta objects by resolving the chain
	if header.Type.IsDelta() {
		// For delta objects, we need to resolve to get the final type and size
		return o.resolveDelta(&header, scanner)
	}

	o.typ = header.Type
	o.size = header.Size
	o.contentOffset = header.ContentOffset
	return nil
}

// resolveDelta handles delta objects by walking the delta chain to find
// the final type and size.
func (o *LazyFSObject) resolveDelta(header *ObjectHeader, scanner *Scanner) error {
	// Find the base object
	var baseOffset int64
	switch header.Type {
	case plumbing.OFSDeltaObject:
		baseOffset = header.OffsetReference
	case plumbing.REFDeltaObject:
		var err error
		baseOffset, err = o.index.FindOffset(header.Reference)
		if err != nil {
			return err
		}
	}

	// Walk the delta chain to find the base object type
	baseType, err := o.findBaseType(baseOffset, scanner)
	if err != nil {
		return err
	}

	// For delta objects, we need to decompress to know the final size.
	// The header.Size is the delta data size, not the resolved object size.
	// We'll get the real size when we actually decompress in Reader().
	// For now, use -1 to indicate unknown size (callers should use Reader()).
	o.typ = baseType
	o.size = -1 // Size is unknown until delta is applied
	o.contentOffset = header.ContentOffset
	return nil
}

// findBaseType walks the delta chain to find the base object type.
func (o *LazyFSObject) findBaseType(offset int64, scanner *Scanner) (plumbing.ObjectType, error) {
	err := scanner.SeekFromStart(offset)
	if err != nil {
		return 0, err
	}

	if !scanner.Scan() {
		if err := scanner.Error(); err != nil {
			return 0, err
		}
		return 0, plumbing.ErrObjectNotFound
	}

	header := scanner.Data().Value().(ObjectHeader)

	if header.Type.IsDelta() {
		// Recurse to find the base
		var nextOffset int64
		switch header.Type {
		case plumbing.OFSDeltaObject:
			nextOffset = header.OffsetReference
		case plumbing.REFDeltaObject:
			nextOffset, err = o.index.FindOffset(header.Reference)
			if err != nil {
				return 0, err
			}
		}
		return o.findBaseType(nextOffset, scanner)
	}

	return header.Type, nil
}

// Hash implements the plumbing.EncodedObject interface.
// This is always available without I/O since it comes from the trusted index.
func (o *LazyFSObject) Hash() plumbing.Hash {
	return o.hash
}

// Type implements the plumbing.EncodedObject interface.
// This requires parsing the packfile header on first access.
func (o *LazyFSObject) Type() plumbing.ObjectType {
	if err := o.resolve(); err != nil {
		return plumbing.InvalidObject
	}
	return o.typ
}

// Size implements the plumbing.EncodedObject interface.
// This requires parsing the packfile header on first access.
// For delta objects, this returns -1 as the size is only known after
// decompressing and applying the delta.
func (o *LazyFSObject) Size() int64 {
	if err := o.resolve(); err != nil {
		return 0
	}
	return o.size
}

// Reader implements the plumbing.EncodedObject interface.
// This decompresses the object content on demand.
func (o *LazyFSObject) Reader() (io.ReadCloser, error) {
	// Check cache first
	if obj, ok := o.cache.Get(o.hash); ok && obj != o {
		return obj.Reader()
	}

	// Ensure header is resolved
	if err := o.resolve(); err != nil {
		return nil, err
	}

	// For delta objects, we need to fully resolve
	if o.size < 0 {
		return o.readDeltaObject()
	}

	// For non-delta objects, read directly from packfile
	return o.readDirectObject()
}

// readDirectObject reads a non-delta object directly from the packfile.
func (o *LazyFSObject) readDirectObject() (io.ReadCloser, error) {
	f, err := o.fs.Open(o.packPath)
	if err != nil {
		return nil, err
	}

	_, err = f.Seek(o.contentOffset, io.SeekStart)
	if err != nil {
		f.Close()
		return nil, err
	}

	br := gogitsync.GetBufioReader(f)
	zr, err := gogitsync.GetZlibReader(br)
	if err != nil {
		gogitsync.PutBufioReader(br)
		f.Close()
		return nil, err
	}

	return &lazyFSObjectReader{
		r:    zr,
		f:    f,
		rbuf: br,
	}, nil
}

// readDeltaObject resolves and reads a delta object.
func (o *LazyFSObject) readDeltaObject() (io.ReadCloser, error) {
	// Open packfile and create a Packfile reader for delta resolution
	f, err := o.fs.Open(o.packPath)
	if err != nil {
		return nil, err
	}

	p := &Packfile{
		Index:        o.index,
		file:         f,
		fs:           o.fs,
		objectIdSize: o.objectIdSize,
		cache:        o.cache,
	}

	// Get the fully resolved object
	obj, err := p.GetByOffset(o.offset)
	if err != nil {
		f.Close()
		return nil, err
	}

	// Cache the resolved object
	o.cache.Put(obj)

	// Update our size now that we know it
	o.mu.Lock()
	o.size = obj.Size()
	o.mu.Unlock()

	return obj.Reader()
}

// lazyFSObjectReader wraps the zlib reader with proper cleanup.
type lazyFSObjectReader struct {
	r      *gogitsync.ZLibReader
	f      io.Closer
	rbuf   *bufio.Reader
	closed bool
}

func (r *lazyFSObjectReader) Read(p []byte) (int, error) {
	return r.r.Read(p)
}

func (r *lazyFSObjectReader) Close() (err error) {
	if r.closed {
		return nil
	}
	r.closed = true

	if r.f != nil {
		defer ioutil.CheckClose(r.f, &err)
	}

	defer gogitsync.PutBufioReader(r.rbuf)
	defer gogitsync.PutZlibReader(r.r)

	return r.r.Close()
}

// SetSize implements the plumbing.EncodedObject interface. This method
// is a noop for lazy objects.
func (o *LazyFSObject) SetSize(int64) {}

// SetType implements the plumbing.EncodedObject interface. This method is
// a noop for lazy objects.
func (o *LazyFSObject) SetType(plumbing.ObjectType) {}

// Writer implements the plumbing.EncodedObject interface. This method always
// returns a nil writer for lazy objects.
func (o *LazyFSObject) Writer() (io.WriteCloser, error) {
	return nil, nil
}

// IsLazy returns true indicating this is a lazy object.
func (o *LazyFSObject) IsLazy() bool {
	return true
}

// IsResolved returns true if the object header has been parsed.
func (o *LazyFSObject) IsResolved() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.resolved
}

// Ensure LazyFSObject implements EncodedObject
var _ plumbing.EncodedObject = (*LazyFSObject)(nil)
