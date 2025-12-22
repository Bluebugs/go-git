// Package memory is a storage backend base on memory
package memory

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/index"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/utils/ioutil"
	gogitsync "github.com/go-git/go-git/v6/utils/sync"
)

// ErrUnsupportedObjectType is returned when an unsupported object type is used.
var ErrUnsupportedObjectType = fmt.Errorf("unsupported object type")

// Storage is an implementation of git.Storer that stores data on memory, being
// ephemeral. The use of this storage should be done in controlled environments,
// since the representation in memory of some repository can fill the machine
// memory. in the other hand this storage has the best performance.
type Storage struct {
	ConfigStorage
	ObjectStorage
	ShallowStorage
	IndexStorage
	ReferenceStorage
	ModuleStorage
	options options
}

// NewStorage returns a new in memory Storage base.
func NewStorage(o ...StorageOption) *Storage {
	opts := newOptions()
	for _, opt := range o {
		opt(&opts)
	}

	s := &Storage{
		options:          opts,
		ReferenceStorage: make(ReferenceStorage),
		ConfigStorage:    ConfigStorage{},
		ShallowStorage:   ShallowStorage{},
		ObjectStorage: ObjectStorage{
			oh:            plumbing.FromObjectFormat(opts.objectFormat),
			Objects:       make(map[plumbing.Hash]plumbing.EncodedObject),
			Commits:       make(map[plumbing.Hash]plumbing.EncodedObject),
			Trees:         make(map[plumbing.Hash]plumbing.EncodedObject),
			Blobs:         make(map[plumbing.Hash]plumbing.EncodedObject),
			Tags:          make(map[plumbing.Hash]plumbing.EncodedObject),
			lazyLoadBlobs: opts.lazyLoadBlobs,
			lazyThreshold: opts.lazyThreshold,
		},
		ModuleStorage: make(ModuleStorage),
	}

	if opts.objectFormat == formatcfg.SHA256 {
		cfg, _ := s.Config() // Config() never returns an error.
		cfg.Extensions.ObjectFormat = opts.objectFormat
		cfg.Core.RepositoryFormatVersion = formatcfg.Version1
	}

	return s
}

// ConfigStorage implements config.ConfigStorer for in-memory storage.
type ConfigStorage struct {
	config *config.Config
}

// SetConfig stores the given config.
func (c *ConfigStorage) SetConfig(cfg *config.Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	c.config = cfg
	return nil
}

// Config returns the stored config.
func (c *ConfigStorage) Config() (*config.Config, error) {
	if c.config == nil {
		c.config = config.NewConfig()
	}

	return c.config, nil
}

// IndexStorage implements storer.IndexStorer for in-memory storage.
type IndexStorage struct {
	index *index.Index
}

// SetIndex stores the given index.
func (c *IndexStorage) SetIndex(idx *index.Index) error {
	c.index = idx
	return nil
}

// Index returns the stored index.
func (c *IndexStorage) Index() (*index.Index, error) {
	if c.index == nil {
		c.index = &index.Index{Version: 2}
	}

	return c.index, nil
}

// ObjectStorage implements storer.EncodedObjectStorer for in-memory storage.
type ObjectStorage struct {
	oh      *plumbing.ObjectHasher
	Objects map[plumbing.Hash]plumbing.EncodedObject
	Commits map[plumbing.Hash]plumbing.EncodedObject
	Trees   map[plumbing.Hash]plumbing.EncodedObject
	Blobs   map[plumbing.Hash]plumbing.EncodedObject
	Tags    map[plumbing.Hash]plumbing.EncodedObject

	// Mutex for protecting object maps during concurrent access
	objectsMu sync.RWMutex

	// Packfile retention for lazy loading
	packfileData   []byte      // Raw packfile data in memory
	packfileReader io.ReaderAt // ReaderAt interface for seeking

	// Lazy loading configuration
	lazyLoadBlobs bool
	lazyThreshold int64

	// Packfile eviction tracking
	lazyObjectCount   int        // Total number of lazy objects created
	cachedObjectCount int        // Number of lazy objects that have been cached
	parsingInProgress bool       // True during packfile parsing, prevents premature eviction
	packfileMu        sync.Mutex // Protects packfile eviction

	// Offset-to-object mapping for OFS_DELTA resolution
	// Maps packfile offset to object hash for lazy delta lookups
	objectsByOffset map[int64]plumbing.Hash

	// Delta patcher function (set during lazy loading initialization)
	deltaPatcher plumbing.DeltaPatcher
}

type lazyCloser struct {
	storage *ObjectStorage
	obj     plumbing.EncodedObject
	closer  io.Closer
}

func (c *lazyCloser) Close() error {
	err := c.closer.Close()
	if err != nil {
		return fmt.Errorf("failed to close memory encoded object: %w", err)
	}

	_, err = c.storage.SetEncodedObject(c.obj)
	return err
}

// RawObjectWriter returns a writer for writing a raw object.
func (o *ObjectStorage) RawObjectWriter(typ plumbing.ObjectType, sz int64) (w io.WriteCloser, err error) {
	obj := o.NewEncodedObject()
	obj.SetType(typ)
	obj.SetSize(sz)

	w, err = obj.Writer()
	if err != nil {
		return nil, err
	}

	wc := ioutil.NewWriteCloser(w,
		&lazyCloser{storage: o, obj: obj, closer: w},
	)

	return wc, nil
}

// NewEncodedObject returns a new EncodedObject.
func (o *ObjectStorage) NewEncodedObject() plumbing.EncodedObject {
	return plumbing.NewMemoryObject(o.oh)
}

// SetEncodedObject stores the given EncodedObject.
func (o *ObjectStorage) SetEncodedObject(obj plumbing.EncodedObject) (plumbing.Hash, error) {
	h := obj.Hash()

	o.objectsMu.Lock()
	defer o.objectsMu.Unlock()

	o.Objects[h] = obj

	switch obj.Type() {
	case plumbing.CommitObject:
		o.Commits[h] = o.Objects[h]
	case plumbing.TreeObject:
		o.Trees[h] = o.Objects[h]
	case plumbing.BlobObject:
		o.Blobs[h] = o.Objects[h]
	case plumbing.TagObject:
		o.Tags[h] = o.Objects[h]
	default:
		return h, ErrUnsupportedObjectType
	}

	return h, nil
}

// HasEncodedObject returns nil if the object exists, or an error otherwise.
func (o *ObjectStorage) HasEncodedObject(h plumbing.Hash) (err error) {
	o.objectsMu.RLock()
	_, ok := o.Objects[h]
	o.objectsMu.RUnlock()
	if !ok {
		return plumbing.ErrObjectNotFound
	}
	return nil
}

// EncodedObjectSize returns the size of the object with the given hash.
func (o *ObjectStorage) EncodedObjectSize(h plumbing.Hash) (
	size int64, err error,
) {
	o.objectsMu.RLock()
	obj, ok := o.Objects[h]
	o.objectsMu.RUnlock()
	if !ok {
		return 0, plumbing.ErrObjectNotFound
	}

	return obj.Size(), nil
}

// EncodedObject returns the object with the given type and hash.
func (o *ObjectStorage) EncodedObject(t plumbing.ObjectType, h plumbing.Hash) (plumbing.EncodedObject, error) {
	o.objectsMu.RLock()
	obj, ok := o.Objects[h]
	o.objectsMu.RUnlock()
	if !ok || (plumbing.AnyObject != t && obj.Type() != t) {
		return nil, plumbing.ErrObjectNotFound
	}

	return obj, nil
}

// IterEncodedObjects returns an iterator for all objects of the given type.
func (o *ObjectStorage) IterEncodedObjects(t plumbing.ObjectType) (storer.EncodedObjectIter, error) {
	o.objectsMu.RLock()
	defer o.objectsMu.RUnlock()

	var series []plumbing.EncodedObject
	switch t {
	case plumbing.AnyObject:
		series = flattenObjectMap(o.Objects)
	case plumbing.CommitObject:
		series = flattenObjectMap(o.Commits)
	case plumbing.TreeObject:
		series = flattenObjectMap(o.Trees)
	case plumbing.BlobObject:
		series = flattenObjectMap(o.Blobs)
	case plumbing.TagObject:
		series = flattenObjectMap(o.Tags)
	}

	return storer.NewEncodedObjectSliceIter(series), nil
}

func flattenObjectMap(m map[plumbing.Hash]plumbing.EncodedObject) []plumbing.EncodedObject {
	objects := make([]plumbing.EncodedObject, 0, len(m))
	for _, obj := range m {
		objects = append(objects, obj)
	}
	return objects
}

// Begin returns a new transaction.
func (o *ObjectStorage) Begin() storer.Transaction {
	return &TxObjectStorage{
		Storage: o,
		Objects: make(map[plumbing.Hash]plumbing.EncodedObject),
	}
}

// ForEachObjectHash calls the given function for each object hash.
func (o *ObjectStorage) ForEachObjectHash(fun func(plumbing.Hash) error) error {
	o.objectsMu.RLock()
	// Copy hashes to avoid holding lock during callback
	hashes := make([]plumbing.Hash, 0, len(o.Objects))
	for h := range o.Objects {
		hashes = append(hashes, h)
	}
	o.objectsMu.RUnlock()

	for _, h := range hashes {
		err := fun(h)
		if err != nil {
			if errors.Is(err, storer.ErrStop) {
				return nil
			}
			return err
		}
	}
	return nil
}

// ObjectPacks returns the list of object packs (always empty for in-memory storage).
func (o *ObjectStorage) ObjectPacks() ([]plumbing.Hash, error) {
	return nil, nil
}

// DeleteOldObjectPackAndIndex is a no-op for in-memory storage.
func (o *ObjectStorage) DeleteOldObjectPackAndIndex(plumbing.Hash, time.Time) error {
	return nil
}

var errNotSupported = fmt.Errorf("not supported")

// LooseObjectTime returns an error as loose objects are not supported.
func (o *ObjectStorage) LooseObjectTime(_ plumbing.Hash) (time.Time, error) {
	return time.Time{}, errNotSupported
}

// DeleteLooseObject returns an error as loose objects are not supported.
func (o *ObjectStorage) DeleteLooseObject(plumbing.Hash) error {
	return errNotSupported
}

// AddAlternate returns an error as alternates are not supported.
func (o *ObjectStorage) AddAlternate(_ string) error {
	return errNotSupported
}

// SetPackfileData stores the raw packfile data in memory for lazy loading.
// This enables LazyMemoryObjects to decompress content on-demand by reading
// from the packfile at specific offsets.
//
// The packfile data is stored as-is (compressed) which is more memory-efficient
// than decompressing all objects upfront.
//
// Parameters:
//   - data: Raw packfile bytes (including PACK header and footer)
//
// This method is typically called during clone/fetch operations when
// lazy loading is enabled.
func (o *ObjectStorage) SetPackfileData(data []byte) error {
	o.packfileData = data

	if data != nil {
		o.packfileReader = bytes.NewReader(data)
	} else {
		o.packfileReader = nil
	}

	return nil
}

// GetPackfileReader returns an io.ReaderAt for the stored packfile data.
// This allows seeking to specific offsets to decompress individual objects.
//
// Returns nil if no packfile data has been set or if the packfile has been
// evicted after all lazy objects were cached.
//
// Thread safety note: bytes.Reader.ReadAt is safe for concurrent use.
// However, the packfile may be evicted (set to nil) when all lazy objects
// have been cached. LazyMemoryObjects hold a reference to the reader obtained
// at creation time, so they remain valid even after eviction. New calls to
// this method after eviction will return nil.
func (o *ObjectStorage) GetPackfileReader() io.ReaderAt {
	return o.packfileReader
}

// pooledZlibReaderFunc returns a ZlibReaderFunc that uses a sync.Pool for zlib readers.
// This reduces allocations when decompressing lazy objects.
func pooledZlibReaderFunc(r io.Reader) (io.ReadCloser, func(), error) {
	zr, err := gogitsync.GetZlibReader(r)
	if err != nil {
		return nil, func() {}, err
	}
	return zr, func() { gogitsync.PutZlibReader(zr) }, nil
}

// StoreLazyObject creates and stores a LazyMemoryObject for the given object metadata.
// This is used by the scanner when lazy loading is enabled to store blob metadata
// without decompressing the content.
//
// The object will be created as a LazyMemoryObject that decompresses on-demand
// from the packfile using the contentOffset.
//
// objectOffset is the offset to the object header in the packfile (for OFS_DELTA tracking).
// contentOffset is the offset to the compressed content (for decompression).
//
// Returns an error if the packfile reader is not available or if storing fails.
func (o *ObjectStorage) StoreLazyObject(typ plumbing.ObjectType, hash plumbing.Hash, size int64, objectOffset int64, contentOffset int64) error {
	if o.packfileReader == nil {
		return fmt.Errorf("packfile reader not available for lazy loading")
	}

	// Create LazyMemoryObject and set all fields
	lazyObj := plumbing.NewLazyMemoryObject(o.oh)
	lazyObj.SetType(typ)
	lazyObj.SetSize(size)

	// Set lazy-specific fields
	// cacheOnRead is always true: once a lazy blob is read, we cache it in memory
	// The threshold controls which blobs are lazy in the first place, not caching behavior
	lazyObj.SetLazyFields(o.packfileReader, contentOffset, true)

	// Use pooled zlib reader for better performance
	lazyObj.SetZlibReaderFunc(pooledZlibReaderFunc)

	// Set up cache notification for automatic packfile eviction
	lazyObj.SetCacheNotifier(func() {
		o.notifyObjectCached()
	})

	// Set the pre-calculated hash from the scanner
	lazyObj.SetHash(hash)

	// Track object offset-to-hash mapping for OFS_DELTA resolution
	// This uses the object header offset, not the content offset
	o.trackObjectOffset(objectOffset, hash)

	// Increment lazy object count
	o.packfileMu.Lock()
	o.lazyObjectCount++
	o.packfileMu.Unlock()

	// Store the object
	_, err := o.SetEncodedObject(lazyObj)
	return err
}

// StoreLazyDeltaObject creates and stores a LazyDeltaObject for the given delta object metadata.
// This is used by the parser when lazy loading is enabled to store delta object metadata
// without eagerly resolving the delta.
//
// The object will be created as a LazyDeltaObject that resolves deltas on-demand.
//
// Parameters:
//   - typ: The resolved object type (after delta application)
//   - hash: The object's hash
//   - size: The resolved object size
//   - deltaOffset: Offset to compressed delta instructions in packfile
//   - deltaType: OFSDelta or REFDelta
//   - baseHash: Hash of base object (for REFDelta)
//   - baseOffset: Offset of base object in packfile (for OFSDelta)
//   - objectOffset: Offset of this delta object in packfile (for tracking)
//
// Returns an error if the packfile reader is not available or if storing fails.
func (o *ObjectStorage) StoreLazyDeltaObject(
	typ plumbing.ObjectType,
	hash plumbing.Hash,
	size int64,
	deltaOffset int64,
	deltaType plumbing.DeltaType,
	baseHash plumbing.Hash,
	baseOffset int64,
	objectOffset int64,
) error {
	if o.packfileReader == nil {
		return fmt.Errorf("packfile reader not available for lazy delta loading")
	}

	if o.deltaPatcher == nil {
		return fmt.Errorf("delta patcher not set for lazy delta loading")
	}

	// Create LazyDeltaObject and set all fields
	lazyObj := plumbing.NewLazyDeltaObject(o.oh)
	lazyObj.SetType(typ)
	lazyObj.SetSize(size)
	lazyObj.SetHash(hash)

	// Set lazy delta-specific fields
	lazyObj.SetLazyDeltaFields(
		o.packfileReader,
		deltaOffset,
		deltaType,
		baseHash,
		baseOffset,
		o,             // ObjectStorage implements LazyDeltaResolver
		o.deltaPatcher,
		true, // cacheOnRead
	)

	// Use pooled zlib reader for better performance
	lazyObj.SetZlibReaderFunc(pooledZlibReaderFunc)

	// Set up cache notification for automatic packfile eviction
	lazyObj.SetCacheNotifier(func() {
		o.notifyObjectCached()
	})

	// Track offset-to-hash mapping for OFS_DELTA resolution
	o.trackObjectOffset(objectOffset, hash)

	// Increment lazy object count
	o.packfileMu.Lock()
	o.lazyObjectCount++
	o.packfileMu.Unlock()

	// Store the object
	_, err := o.SetEncodedObject(lazyObj)
	return err
}

// TrackObjectOffset records the mapping from packfile offset to object hash.
// This is needed for OFS_DELTA resolution when the base object is stored eagerly.
// Implements part of packfile.LazyStorageCapable interface.
func (o *ObjectStorage) TrackObjectOffset(offset int64, hash plumbing.Hash) {
	o.trackObjectOffset(offset, hash)
}

// trackObjectOffset records the mapping from packfile offset to object hash.
// This is needed for OFS_DELTA resolution.
func (o *ObjectStorage) trackObjectOffset(offset int64, hash plumbing.Hash) {
	o.packfileMu.Lock()
	defer o.packfileMu.Unlock()

	if o.objectsByOffset == nil {
		o.objectsByOffset = make(map[int64]plumbing.Hash)
	}
	o.objectsByOffset[offset] = hash
}

// EncodedObjectByOffset retrieves an object by its offset in the packfile.
// This is used for OFS_DELTA resolution where we only have an offset, not a hash.
// Implements plumbing.OffsetResolver interface.
func (o *ObjectStorage) EncodedObjectByOffset(offset int64) (plumbing.EncodedObject, error) {
	o.packfileMu.Lock()
	hash, ok := o.objectsByOffset[offset]
	o.packfileMu.Unlock()

	if !ok {
		return nil, plumbing.ErrObjectNotFound
	}

	return o.EncodedObject(plumbing.AnyObject, hash)
}

// SetDeltaPatcher sets the delta patcher function used for lazy delta resolution.
// This must be called before storing any lazy delta objects.
func (o *ObjectStorage) SetDeltaPatcher(patcher plumbing.DeltaPatcher) {
	o.deltaPatcher = patcher
}

// notifyObjectCached is called by LazyMemoryObject when it caches its content.
// This tracks the number of cached objects and automatically evicts the packfile
// from memory once all lazy objects have been cached.
func (o *ObjectStorage) notifyObjectCached() {
	o.packfileMu.Lock()
	defer o.packfileMu.Unlock()

	o.cachedObjectCount++

	// Don't evict if we're still parsing the packfile
	if o.parsingInProgress {
		return
	}

	// If all lazy objects are now cached, we can evict the packfile from memory
	if o.cachedObjectCount >= o.lazyObjectCount && o.lazyObjectCount > 0 {
		// All lazy objects have been read and cached, so we no longer need
		// the packfile in memory. Set it to nil so GC can reclaim the memory.
		o.packfileData = nil
		o.packfileReader = nil
	}
}

// SetParsingInProgress indicates whether packfile parsing is in progress.
// When true, packfile eviction is deferred until parsing completes.
func (o *ObjectStorage) SetParsingInProgress(inProgress bool) {
	o.packfileMu.Lock()
	defer o.packfileMu.Unlock()
	o.parsingInProgress = inProgress
}

// IsLazyLoadingEnabled returns true if lazy loading is enabled for this storage.
func (o *ObjectStorage) IsLazyLoadingEnabled() bool {
	return o.lazyLoadBlobs
}

// IsPackfileEvicted returns true if the packfile has been evicted from memory.
// This happens automatically when all lazy objects have been cached.
func (o *ObjectStorage) IsPackfileEvicted() bool {
	o.packfileMu.Lock()
	defer o.packfileMu.Unlock()
	return o.packfileReader == nil && o.lazyObjectCount > 0
}

// GetLazyObjectStats returns statistics about lazy objects for debugging and testing.
func (o *ObjectStorage) GetLazyObjectStats() (total, cached int) {
	o.packfileMu.Lock()
	defer o.packfileMu.Unlock()
	return o.lazyObjectCount, o.cachedObjectCount
}

// TxObjectStorage implements storer.Transaction for in-memory storage.
type TxObjectStorage struct {
	Storage *ObjectStorage
	Objects map[plumbing.Hash]plumbing.EncodedObject
}

// SetEncodedObject stores the given EncodedObject in the transaction.
func (tx *TxObjectStorage) SetEncodedObject(obj plumbing.EncodedObject) (plumbing.Hash, error) {
	h := obj.Hash()
	tx.Objects[h] = obj

	return h, nil
}

// EncodedObject returns the object with the given type and hash from the transaction.
func (tx *TxObjectStorage) EncodedObject(t plumbing.ObjectType, h plumbing.Hash) (plumbing.EncodedObject, error) {
	obj, ok := tx.Objects[h]
	if !ok || (plumbing.AnyObject != t && obj.Type() != t) {
		return nil, plumbing.ErrObjectNotFound
	}

	return obj, nil
}

// Commit commits all objects in the transaction to the storage.
func (tx *TxObjectStorage) Commit() error {
	for h, obj := range tx.Objects {
		delete(tx.Objects, h)
		if _, err := tx.Storage.SetEncodedObject(obj); err != nil {
			return err
		}
	}

	return nil
}

// Rollback discards all objects in the transaction.
func (tx *TxObjectStorage) Rollback() error {
	clear(tx.Objects)
	return nil
}

// ReferenceStorage implements storer.ReferenceStorer for in-memory storage.
type ReferenceStorage map[plumbing.ReferenceName]*plumbing.Reference

// SetReference stores the given reference.
func (r ReferenceStorage) SetReference(ref *plumbing.Reference) error {
	if ref != nil {
		r[ref.Name()] = ref
	}

	return nil
}

// CheckAndSetReference stores the reference if the old reference matches.
func (r ReferenceStorage) CheckAndSetReference(ref, old *plumbing.Reference) error {
	if ref == nil {
		return nil
	}

	if old != nil {
		tmp := r[ref.Name()]
		if tmp != nil && tmp.Hash() != old.Hash() {
			return storage.ErrReferenceHasChanged
		}
	}
	r[ref.Name()] = ref
	return nil
}

// Reference returns the reference with the given name.
func (r ReferenceStorage) Reference(n plumbing.ReferenceName) (*plumbing.Reference, error) {
	ref, ok := r[n]
	if !ok {
		return nil, plumbing.ErrReferenceNotFound
	}

	return ref, nil
}

// IterReferences returns an iterator for all references.
func (r ReferenceStorage) IterReferences() (storer.ReferenceIter, error) {
	refs := make([]*plumbing.Reference, 0, len(r))
	for _, ref := range r {
		refs = append(refs, ref)
	}

	return storer.NewReferenceSliceIter(refs), nil
}

// CountLooseRefs returns the number of references.
func (r ReferenceStorage) CountLooseRefs() (int, error) {
	return len(r), nil
}

// PackRefs is a no-op.
func (r ReferenceStorage) PackRefs() error {
	return nil
}

// RemoveReference removes the reference with the given name.
func (r ReferenceStorage) RemoveReference(n plumbing.ReferenceName) error {
	delete(r, n)
	return nil
}

// ShallowStorage implements storer.ShallowStorer for in-memory storage.
type ShallowStorage []plumbing.Hash

// SetShallow stores the shallow commits.
func (s *ShallowStorage) SetShallow(commits []plumbing.Hash) error {
	*s = commits
	return nil
}

// Shallow returns the shallow commits.
func (s ShallowStorage) Shallow() ([]plumbing.Hash, error) {
	return s, nil
}

// ModuleStorage implements storer.ModuleStorer for in-memory storage.
type ModuleStorage map[string]*Storage

// Module returns the storage for the given submodule.
func (s ModuleStorage) Module(name string) (storage.Storer, error) {
	if m, ok := s[name]; ok {
		return m, nil
	}

	m := NewStorage()
	s[name] = m

	return m, nil
}
