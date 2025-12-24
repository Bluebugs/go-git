package packfile

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	stdsync "sync"

	"github.com/go-git/go-git/v6/plumbing"
	format "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage/memory/shared"
	"github.com/go-git/go-git/v6/utils/ioutil"
	"github.com/go-git/go-git/v6/utils/sync"
)

var (
	// ErrReferenceDeltaNotFound is returned when the reference delta is not
	// found.
	ErrReferenceDeltaNotFound = errors.New("reference delta not found")

	// ErrNotSeekableSource is returned when the source for the parser is not
	// seekable and a storage was not provided, so it can't be parsed.
	ErrNotSeekableSource = errors.New("parser source is not seekable and storage was not provided")

	// ErrDeltaNotCached is returned when the delta could not be found in cache.
	ErrDeltaNotCached = errors.New("delta could not be found in cache")
)

// Parser decodes a packfile and calls any observer associated to it. Is used
// to generate indexes.
type Parser struct {
	storage       storer.EncodedObjectStorer
	cache         *parserCache
	lowMemoryMode bool
	packfilePath  string // Optional path for shared packfile deduplication

	scanner   *Scanner
	observers []Observer
	hasher    plumbing.Hasher

	checksum plumbing.Hash
	m        stdsync.Mutex
}

// LowMemoryCapable is implemented by storage types that are capable of
// operating in low-memory mode.
type LowMemoryCapable interface {
	// LowMemoryMode defines whether the storage is able and willing for
	// the parser to operate in low-memory mode.
	LowMemoryMode() bool
}

// PackfileDataStorer is an interface for storage that needs to retain packfile data
// for lazy loading. When implemented, the parser will buffer the packfile and store
// it before parsing.
type PackfileDataStorer interface {
	SetPackfileData(data []byte) error
}

// NewParser creates a new Parser.
// When a storage is set, the objects are written to storage as they
// are parsed.
func NewParser(data io.Reader, opts ...ParserOption) *Parser {
	p := &Parser{
		hasher: plumbing.NewHasher(format.SHA1, plumbing.AnyObject, 0),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(p)
		}
	}

	// Set up scanner - handle lazy loading if storage supports it
	p.scanner = p.initScanner(data)

	if p.storage != nil {
		p.scanner.storage = p.storage

		lm, ok := p.storage.(LowMemoryCapable)
		p.lowMemoryMode = ok && lm.LowMemoryMode()
	}

	if p.scanner.seeker == nil {
		p.lowMemoryMode = false
	}
	p.scanner.lowMemoryMode = p.lowMemoryMode
	p.cache = newParserCache()

	return p
}

// initScanner initializes the scanner, handling lazy loading and shared packfile storage.
// It returns the configured Scanner for the packfile data.
func (p *Parser) initScanner(data io.Reader) *Scanner {
	// Fast path: no storage or lazy loading not enabled
	if !p.isLazyLoadingEnabled() {
		return NewScanner(data)
	}

	// Buffer the packfile data for lazy loading.
	// Note: This reads the entire packfile into memory. For very large
	// repositories, consider using filesystem-based storage with mmap
	// support instead, which avoids the allocation overhead of buffering.
	packfileData, err := io.ReadAll(data)
	if err != nil {
		panic(fmt.Sprintf("go-git: failed to buffer packfile for lazy loading: %v", err))
	}

	// Try to store packfile data (shared or legacy)
	p.storePackfileData(packfileData)

	// Set up delta patcher for lazy delta objects
	if lazyDeltaStorage, ok := p.storage.(LazyDeltaStorageCapable); ok {
		lazyDeltaStorage.SetDeltaPatcher(PatchDelta)
	}

	return NewScanner(bytes.NewReader(packfileData))
}

// isLazyLoadingEnabled checks if the storage supports and has lazy loading enabled.
func (p *Parser) isLazyLoadingEnabled() bool {
	if p.storage == nil {
		return false
	}
	lazyStorage, ok := p.storage.(LazyStorageCapable)
	return ok && lazyStorage.IsLazyLoadingEnabled()
}

// storePackfileData stores packfile data using shared storage if available, otherwise legacy.
func (p *Parser) storePackfileData(packfileData []byte) {
	// Try shared storage if packfile path is available
	if p.packfilePath != "" {
		if p.tryStoreShared(packfileData) {
			return
		}
	}

	// Fall back to legacy storage
	p.storePackfileLegacy(packfileData)
}

// tryStoreShared attempts to store packfile data using shared deduplication.
// Returns true if successful, false if should fall back to legacy storage.
func (p *Parser) tryStoreShared(packfileData []byte) bool {
	sharedStorer, ok := p.storage.(storer.SharedPackfileCapable)
	if !ok {
		return false
	}

	identity, err := p.getPackfileIdentity()
	if err != nil {
		return false
	}

	return sharedStorer.SetPackfileDataShared(packfileData, identity) == nil
}

// storePackfileLegacy stores packfile data using the legacy SetPackfileData method.
func (p *Parser) storePackfileLegacy(packfileData []byte) {
	packfileStorer, ok := p.storage.(PackfileDataStorer)
	if !ok {
		return
	}
	// Ignore error - lazy loading will still work, just without persistence
	_ = packfileStorer.SetPackfileData(packfileData)
}

// getPackfileIdentity extracts the filesystem identity from the packfile path.
func (p *Parser) getPackfileIdentity() (storer.PackfileIdentity, error) {
	identity, err := shared.GetPackfileIdentity(p.packfilePath)
	if err != nil {
		return storer.PackfileIdentity{}, err
	}

	return storer.PackfileIdentity{
		Inode:  identity.Inode,
		Device: identity.Device,
		Size:   identity.Size,
		Mtime:  identity.Mtime,
	}, nil
}

func (p *Parser) storeOrCache(oh *ObjectHeader) error {
	// Only need to store deltas, as the scanner already stored non-delta
	// objects.
	if p.storage != nil && oh.diskType.IsDelta() {
		w, err := p.storage.RawObjectWriter(oh.Type, oh.Size)
		if err != nil {
			return err
		}

		defer func() { _ = w.Close() }()

		_, err = ioutil.CopyBufferPool(w, oh.content)
		if err != nil {
			return err
		}
	}

	if p.cache != nil {
		o := oh
		for p.lowMemoryMode && o.content != nil {
			sync.PutBytesBuffer(o.content)
			o.content = nil

			if o.parent == nil || o.parent.content == nil {
				break
			}
			o = o.parent
		}
		p.cache.Add(oh)
	}

	if err := p.onInflatedObjectHeader(oh.Type, oh.Size, oh.Offset); err != nil {
		return err
	}

	return p.onInflatedObjectContent(oh.Hash, oh.Offset, oh.Crc32, nil)
}

func (p *Parser) resetCache(qty int) {
	if p.cache != nil {
		p.cache.Reset(qty)
	}
}

// Parse start decoding phase of the packfile.
func (p *Parser) Parse() (plumbing.Hash, error) {
	p.m.Lock()
	defer p.m.Unlock()

	// Mark parsing as in progress to prevent premature packfile eviction
	if lazyDeltaStorage, ok := p.storage.(LazyDeltaStorageCapable); ok && lazyDeltaStorage.IsLazyLoadingEnabled() {
		lazyDeltaStorage.SetParsingInProgress(true)
		defer lazyDeltaStorage.SetParsingInProgress(false)
	}

	var pendingDeltas []*ObjectHeader
	var pendingDeltaREFs []*ObjectHeader

	for p.scanner.Scan() {
		data := p.scanner.Data()
		switch data.Section {
		case HeaderSection:
			header := data.Value().(Header)

			p.resetCache(int(header.ObjectsQty))
			_ = p.onHeader(header.ObjectsQty)

		case ObjectSection:
			oh := data.Value().(ObjectHeader)
			if oh.Type.IsDelta() {
				switch oh.Type {
				case plumbing.OFSDeltaObject:
					pendingDeltas = append(pendingDeltas, &oh)
				case plumbing.REFDeltaObject:
					pendingDeltaREFs = append(pendingDeltaREFs, &oh)
				}
				continue
			}

			if p.lowMemoryMode && oh.content != nil {
				sync.PutBytesBuffer(oh.content)
				oh.content = nil
			}

			_ = p.storeOrCache(&oh)

		case FooterSection:
			p.checksum = data.Value().(plumbing.Hash)
		}
	}

	if p.scanner.objects == 0 {
		return plumbing.ZeroHash, ErrEmptyPackfile
	}

	for _, oh := range pendingDeltaREFs {
		err := p.processDelta(oh)
		if err != nil {
			return plumbing.ZeroHash, fmt.Errorf("processing ref-delta at offset %v: %w", oh.Offset, err)
		}
	}

	for _, oh := range pendingDeltas {
		err := p.processDelta(oh)
		if err != nil {
			return plumbing.ZeroHash, fmt.Errorf("processing ofs-delta at offset %v: %w", oh.Offset, err)
		}
	}

	// Return to pool all objects used.
	go func() {
		for _, oh := range p.cache.oi {
			if oh.content != nil {
				sync.PutBytesBuffer(oh.content)
				oh.content = nil
			}
		}
	}()

	return p.checksum, p.onFooter(p.checksum)
}

func (p *Parser) ensureContent(oh *ObjectHeader) error {
	// Skip if this object already has the correct content.
	if oh.content != nil && oh.content.Len() == int(oh.Size) && !oh.Hash.IsZero() {
		return nil
	}

	if oh.content == nil {
		oh.content = sync.GetBytesBuffer()
	}

	var err error
	switch {
	case !p.lowMemoryMode && oh.content != nil && oh.content.Len() > 0:
		source := oh.content
		oh.content = sync.GetBytesBuffer()

		defer sync.PutBytesBuffer(source)

		err = p.applyPatchBaseHeader(oh, source, oh.content, nil)
	case p.scanner.seeker != nil:
		deltaData := sync.GetBytesBuffer()
		defer sync.PutBytesBuffer(deltaData)

		err = p.scanner.inflateContent(oh.ContentOffset, deltaData)
		if err != nil {
			return fmt.Errorf("inflating content at offset %v: %w", oh.ContentOffset, err)
		}

		err = p.applyPatchBaseHeader(oh, deltaData, oh.content, nil)
	default:
		return fmt.Errorf("can't ensure content: %w", plumbing.ErrObjectNotFound)
	}

	if err != nil {
		return fmt.Errorf("apply delta patch: %w", err)
	}
	return nil
}

func (p *Parser) processDelta(oh *ObjectHeader) error {
	switch oh.Type {
	case plumbing.OFSDeltaObject:
		pa, ok := p.cache.oiByOffset[oh.OffsetReference]
		if !ok {
			return plumbing.ErrObjectNotFound
		}
		oh.parent = pa

	case plumbing.REFDeltaObject:
		pa, ok := p.cache.oiByHash[oh.Reference]
		if !ok {
			// can't find referenced object in this pack file
			// this must be a "thin" pack.
			oh.parent = &ObjectHeader{ // Placeholder parent
				Hash:        oh.Reference,
				externalRef: true, // mark as an external reference that must be resolved
				Type:        plumbing.AnyObject,
				diskType:    plumbing.AnyObject,
			}
		} else {
			oh.parent = pa
		}
		p.cache.oiByHash[oh.Reference] = oh.parent

	default:
		return fmt.Errorf("unsupported delta type: %v", oh.Type)
	}

	// Check if we should use lazy delta storage
	if lazyDeltaStorage, ok := p.storage.(LazyDeltaStorageCapable); ok && lazyDeltaStorage.IsLazyLoadingEnabled() {
		return p.processLazyDelta(oh, lazyDeltaStorage)
	}

	if err := p.ensureContent(oh); err != nil {
		return err
	}

	return p.storeOrCache(oh)
}

// processLazyDelta stores a delta object as a LazyDeltaObject without eagerly resolving it.
// The delta will be resolved on-demand when the object is read.
func (p *Parser) processLazyDelta(oh *ObjectHeader, lazyStorage LazyDeltaStorageCapable) error {
	// For lazy delta, we need to calculate the hash without fully resolving the delta.
	// We do this by eagerly resolving once to get hash, type, and size, then storing as lazy.
	//
	// NOTE: We cannot release oh.content here because subsequent delta objects in the
	// packfile might use this object as a base. The content will be released when the
	// parser completes or when the cache evicts the entry.
	//
	// The memory savings from LazyDeltaObject come from:
	// 1. Not storing the resolved content in the final ObjectStorage permanently
	// 2. Re-resolving the delta on-demand when Reader() is called
	// 3. If the object is never read, we save the permanent storage memory

	// First, we need to resolve the delta to get the final hash, type, and size.
	if err := p.ensureContent(oh); err != nil {
		return err
	}

	// Determine delta type for lazy storage
	var deltaType plumbing.DeltaType
	var baseHash plumbing.Hash
	var baseOffset int64

	switch oh.diskType {
	case plumbing.OFSDeltaObject:
		deltaType = plumbing.OFSDelta
		baseOffset = oh.OffsetReference
		// For OFS_DELTA, we need to look up the base hash from the cache
		if oh.parent != nil {
			baseHash = oh.parent.Hash
		}
	case plumbing.REFDeltaObject:
		deltaType = plumbing.REFDelta
		baseHash = oh.Reference
	}

	// Store as lazy delta object
	err := lazyStorage.StoreLazyDeltaObject(
		oh.Type,          // resolved type (not delta type)
		oh.Hash,          // hash of resolved content
		oh.Size,          // size of resolved content
		oh.ContentOffset, // offset to delta instructions
		deltaType,
		baseHash,
		baseOffset,
		oh.Offset, // offset of this object in packfile
	)
	if err != nil {
		return fmt.Errorf("storing lazy delta object: %w", err)
	}

	// Still need to cache in parser for potential future base object lookups
	if p.cache != nil {
		p.cache.Add(oh)
	}

	// Notify observers
	if err := p.onInflatedObjectHeader(oh.Type, oh.Size, oh.Offset); err != nil {
		return err
	}

	if err := p.onInflatedObjectContent(oh.Hash, oh.Offset, oh.Crc32, nil); err != nil {
		return err
	}

	return nil
}

// parentReader returns a [io.ReaderAt] for the decompressed contents
// of the parent.
func (p *Parser) parentReader(parent *ObjectHeader) (io.ReaderAt, error) {
	if parent.content != nil && parent.content.Len() > 0 {
		return bytes.NewReader(parent.content.Bytes()), nil
	}

	// If parent is a Delta object, the inflated object must come
	// from either cache or storage, else we would need to inflate
	// it to then inflate the current object, which could go on
	// indefinitely.
	if p.storage != nil && parent.Hash != plumbing.ZeroHash {
		obj, err := p.storage.EncodedObject(parent.Type, parent.Hash)
		if err == nil {
			// Ensure that external references have the correct type and size.
			parent.Type = obj.Type()
			parent.Size = obj.Size()
			r, err := obj.Reader()
			if err == nil {
				defer func() { _ = r.Close() }()

				if parent.content == nil {
					parent.content = sync.GetBytesBuffer()
				}
				parent.content.Grow(int(parent.Size))

				_, err = ioutil.CopyBufferPool(parent.content, r)
				if err == nil {
					return bytes.NewReader(parent.content.Bytes()), nil
				}
			}
		}
	}

	// If the parent is not an external ref and we don't have the
	// content offset, we won't be able to inflate via seeking through
	// the packfile.
	if !parent.externalRef && parent.ContentOffset == 0 {
		return nil, plumbing.ErrObjectNotFound
	}

	// Not a seeker data source, so avoid seeking the content.
	if p.scanner.seeker == nil {
		return nil, plumbing.ErrObjectNotFound
	}

	if parent.content == nil {
		parent.content = sync.GetBytesBuffer()
	}
	parent.content.Grow(int(parent.Size))

	err := p.scanner.inflateContent(parent.ContentOffset, parent.content)
	if err != nil {
		return nil, ErrReferenceDeltaNotFound
	}
	return bytes.NewReader(parent.content.Bytes()), nil
}

func (p *Parser) applyPatchBaseHeader(ota *ObjectHeader, delta io.Reader, target io.Writer, wh objectHeaderWriter) error {
	if target == nil {
		return fmt.Errorf("cannot apply patch against nil target")
	}

	parentContents, err := p.parentReader(ota.parent)
	if err != nil {
		return err
	}

	typ := ota.Type
	if ota.Hash == plumbing.ZeroHash {
		typ = ota.parent.Type
	}

	sz, h, err := patchDeltaWriter(target, parentContents, delta, typ, wh)
	if err != nil {
		return err
	}

	if ota.Hash == plumbing.ZeroHash {
		ota.Type = typ
		ota.Size = int64(sz)
		ota.Hash = h
	}

	return nil
}

func (p *Parser) forEachObserver(f func(o Observer) error) error {
	for _, o := range p.observers {
		if err := f(o); err != nil {
			return err
		}
	}
	return nil
}

func (p *Parser) onHeader(count uint32) error {
	return p.forEachObserver(func(o Observer) error {
		return o.OnHeader(count)
	})
}

func (p *Parser) onInflatedObjectHeader(
	t plumbing.ObjectType,
	objSize int64,
	pos int64,
) error {
	return p.forEachObserver(func(o Observer) error {
		return o.OnInflatedObjectHeader(t, objSize, pos)
	})
}

func (p *Parser) onInflatedObjectContent(
	h plumbing.Hash,
	pos int64,
	crc uint32,
	content []byte,
) error {
	return p.forEachObserver(func(o Observer) error {
		return o.OnInflatedObjectContent(h, pos, crc, content)
	})
}

func (p *Parser) onFooter(h plumbing.Hash) error {
	return p.forEachObserver(func(o Observer) error {
		return o.OnFooter(h)
	})
}
