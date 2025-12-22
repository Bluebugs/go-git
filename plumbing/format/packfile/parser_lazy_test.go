package packfile

import (
	"io"
	"testing"

	fixtures "github.com/go-git/go-git-fixtures/v5"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParser_WithLazyStorage tests parser with lazy-loading capable storage.
func TestParser_WithLazyStorage(t *testing.T) {
	// Create storage with lazy loading enabled
	storage := memory.NewStorage(memory.WithLazyLoading(0))

	// Read packfile
	packfile := fixtures.Basic().One().Packfile()
	
	// Store packfile data for lazy objects
	packfileData, err := io.ReadAll(packfile)
	require.NoError(t, err)
	storage.ObjectStorage.SetPackfileData(packfileData)
	
	// Parse with storage
	packfile.Seek(0, io.SeekStart)
	parser := NewParser(packfile, WithStorage(&storage.ObjectStorage))
	
	hash, err := parser.Parse()
	require.NoError(t, err)
	assert.NotEqual(t, plumbing.ZeroHash, hash)

	// Verify objects are stored
	// For now, just check that we can retrieve them
	// Later we'll verify they're LazyMemoryObjects
	iter, err := storage.ObjectStorage.IterEncodedObjects(plumbing.BlobObject)
	require.NoError(t, err)

	blobCount := 0
	lazyBlobCount := 0
	err = iter.ForEach(func(obj plumbing.EncodedObject) error {
		assert.Equal(t, plumbing.BlobObject, obj.Type())

		// Check if blob is a LazyMemoryObject
		if _, isLazy := obj.(*plumbing.LazyMemoryObject); isLazy {
			lazyBlobCount++
		}

		blobCount++
		return nil
	})
	require.NoError(t, err)
	assert.Greater(t, blobCount, 0, "should have stored some blobs")
	assert.Greater(t, lazyBlobCount, 0, "blobs should be lazy when lazy loading is enabled")
}

// TestParser_LazyObjects_AreReadable tests that lazy objects can be read.
func TestParser_LazyObjects_AreReadable(t *testing.T) {
	storage := memory.NewStorage(memory.WithLazyLoading(0))

	packfile := fixtures.Basic().One().Packfile()
	packfileData, err := io.ReadAll(packfile)
	require.NoError(t, err)
	
	storage.ObjectStorage.SetPackfileData(packfileData)
	
	packfile.Seek(0, io.SeekStart)
	parser := NewParser(packfile, WithStorage(&storage.ObjectStorage))
	_, err = parser.Parse()
	require.NoError(t, err)

	// Get a blob object and try to read it
	iter, err := storage.ObjectStorage.IterEncodedObjects(plumbing.BlobObject)
	require.NoError(t, err)

	// Read first blob
	var firstBlob plumbing.EncodedObject
	iter.ForEach(func(obj plumbing.EncodedObject) error {
		if firstBlob == nil {
			firstBlob = obj
		}
		return nil
	})
	require.NotNil(t, firstBlob)

	// Verify it's a LazyMemoryObject
	_, isLazy := firstBlob.(*plumbing.LazyMemoryObject)
	assert.True(t, isLazy, "blob should be a LazyMemoryObject when lazy loading is enabled")

	// Try to read the blob content
	reader, err := firstBlob.Reader()
	require.NoError(t, err)
	defer reader.Close()

	content, err := io.ReadAll(reader)
	require.NoError(t, err)
	
	// Content should match the blob size
	assert.Equal(t, int(firstBlob.Size()), len(content))
}

// TestParser_NonBlobsStayEager tests that commits, trees, tags are not lazy.
func TestParser_NonBlobsStayEager(t *testing.T) {
	storage := memory.NewStorage(memory.WithLazyLoading(0))

	packfile := fixtures.Basic().One().Packfile()
	packfileData, err := io.ReadAll(packfile)
	require.NoError(t, err)
	
	storage.ObjectStorage.SetPackfileData(packfileData)
	
	packfile.Seek(0, io.SeekStart)
	parser := NewParser(packfile, WithStorage(&storage.ObjectStorage))
	_, err = parser.Parse()
	require.NoError(t, err)

	// Check commits
	commitIter, _ := storage.ObjectStorage.IterEncodedObjects(plumbing.CommitObject)
	commitCount := 0
	commitIter.ForEach(func(obj plumbing.EncodedObject) error {
		// Commits should be regular MemoryObjects (not lazy)
		_, isLazy := obj.(*plumbing.LazyMemoryObject)
		assert.False(t, isLazy, "commits should not be lazy")
		commitCount++
		return nil
	})
	t.Logf("Found %d commits", commitCount)

	// Check trees
	treeIter, _ := storage.ObjectStorage.IterEncodedObjects(plumbing.TreeObject)
	treeCount := 0
	treeIter.ForEach(func(obj plumbing.EncodedObject) error {
		_, isLazy := obj.(*plumbing.LazyMemoryObject)
		assert.False(t, isLazy, "trees should not be lazy")
		treeCount++
		return nil
	})
	t.Logf("Found %d trees", treeCount)
}
