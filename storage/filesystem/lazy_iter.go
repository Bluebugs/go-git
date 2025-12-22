package filesystem

import (
	"io"

	billy "github.com/go-git/go-billy/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/cache"
	"github.com/go-git/go-git/v6/plumbing/format/idxfile"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	"github.com/go-git/go-git/v6/plumbing/storer"
)

// lazyFSObjectIter iterates over pack entries returning LazyFSObject instances.
// It trusts the index for hashes and defers header parsing until objects are accessed.
type lazyFSObjectIter struct {
	fs           billy.Filesystem
	packPath     string
	index        idxfile.Index
	cache        cache.Object
	iter         idxfile.EntryIter
	seen         map[plumbing.Hash]struct{}
	typ          plumbing.ObjectType
	objectIDSize int
}

// newLazyFSObjectIter creates a new iterator that returns LazyFSObject instances.
// This is more efficient than the standard iterator because it doesn't parse
// object headers until they are actually accessed.
func newLazyFSObjectIter(
	fs billy.Filesystem,
	packPath string,
	index idxfile.Index,
	cache cache.Object,
	seen map[plumbing.Hash]struct{},
	typ plumbing.ObjectType,
	objectIDSize int,
) (storer.EncodedObjectIter, error) {
	entries, err := index.EntriesByOffset()
	if err != nil {
		return nil, err
	}

	return &lazyFSObjectIter{
		fs:           fs,
		packPath:     packPath,
		index:        index,
		cache:        cache,
		iter:         entries,
		seen:         seen,
		typ:          typ,
		objectIDSize: objectIDSize,
	}, nil
}

func (i *lazyFSObjectIter) Next() (plumbing.EncodedObject, error) {
	for {
		e, err := i.iter.Next()
		if err != nil {
			return nil, err
		}

		// Skip if we've already seen this hash
		if _, ok := i.seen[e.Hash]; ok {
			continue
		}

		// Check cache first
		if obj, ok := i.cache.Get(e.Hash); ok {
			// If filtering by type, check if it matches
			if i.typ != plumbing.AnyObject && obj.Type() != i.typ {
				continue
			}
			return obj, nil
		}

		// Create a LazyFSObject - this doesn't read any data yet
		lazyObj := packfile.NewLazyFSObject(
			e.Hash,
			int64(e.Offset),
			i.index,
			i.fs,
			i.packPath,
			i.cache,
			i.objectIDSize,
		)

		// If we're filtering by type, we need to check the type
		// This will trigger lazy header loading
		if i.typ != plumbing.AnyObject {
			objType := lazyObj.Type()
			// Skip delta objects when filtering - they will resolve to base type
			if objType.IsDelta() {
				// For now, skip deltas when filtering by type
				// A more sophisticated approach would resolve the delta
				continue
			}
			if objType != i.typ {
				continue
			}
		}

		return lazyObj, nil
	}
}

func (i *lazyFSObjectIter) ForEach(f func(plumbing.EncodedObject) error) error {
	for {
		o, err := i.Next()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		if err := f(o); err != nil {
			return err
		}
	}
}

func (i *lazyFSObjectIter) Close() {
	i.iter.Close()
}

// lazyPackfileIter wraps multiple pack files and returns lazy objects.
type lazyPackfileIter struct {
	hashes []plumbing.Hash
	open   func(h plumbing.Hash) (storer.EncodedObjectIter, error)
	cur    storer.EncodedObjectIter
}

func (it *lazyPackfileIter) Next() (plumbing.EncodedObject, error) {
	for {
		if it.cur == nil {
			if len(it.hashes) == 0 {
				return nil, io.EOF
			}
			h := it.hashes[0]
			it.hashes = it.hashes[1:]

			sub, err := it.open(h)
			if err == io.EOF {
				continue
			} else if err != nil {
				return nil, err
			}
			it.cur = sub
		}
		ob, err := it.cur.Next()
		if err == io.EOF {
			it.cur.Close()
			it.cur = nil
			continue
		} else if err != nil {
			return nil, err
		}
		return ob, nil
	}
}

func (it *lazyPackfileIter) ForEach(cb func(plumbing.EncodedObject) error) error {
	return storer.ForEachIterator(it, cb)
}

func (it *lazyPackfileIter) Close() {
	if it.cur != nil {
		it.cur.Close()
		it.cur = nil
	}
	it.hashes = nil
}
