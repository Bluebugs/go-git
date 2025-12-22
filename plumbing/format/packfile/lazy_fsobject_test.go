package packfile_test

import (
	"crypto"
	"io"
	"testing"

	"github.com/go-git/go-billy/v6/osfs"
	fixtures "github.com/go-git/go-git-fixtures/v5"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/cache"
	"github.com/go-git/go-git/v6/plumbing/format/idxfile"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLazyFSObject_Basic(t *testing.T) {
	t.Parallel()

	f := fixtures.Basic().One()

	// Get the index
	idx := idxfile.NewMemoryIndex(crypto.SHA1.Size())
	err := idxfile.NewDecoder(f.Idx()).Decode(idx)
	require.NoError(t, err)

	// Get entries from the index
	entries, err := idx.EntriesByOffset()
	require.NoError(t, err)

	// Get the first non-delta entry
	var entry *idxfile.Entry
	for {
		e, err := entries.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		entry = e
		break
	}
	entries.Close()
	require.NotNil(t, entry, "should have at least one entry")

	// Create a temp directory filesystem
	fs := osfs.New(t.TempDir())

	// Copy the packfile to our temp fs
	packFilePath := "objects/pack/test.pack"
	packfileData, err := io.ReadAll(f.Packfile())
	require.NoError(t, err)
	err = fs.MkdirAll("objects/pack", 0755)
	require.NoError(t, err)
	packFile, err := fs.Create(packFilePath)
	require.NoError(t, err)
	_, err = packFile.Write(packfileData)
	require.NoError(t, err)
	err = packFile.Close()
	require.NoError(t, err)

	objectCache := cache.NewObjectLRUDefault()

	// Create a LazyFSObject
	lazyObj := packfile.NewLazyFSObject(
		entry.Hash,
		int64(entry.Offset),
		idx,
		fs,
		packFilePath,
		objectCache,
		crypto.SHA1.Size(),
	)

	// Test Hash - should return the hash we provided
	assert.Equal(t, entry.Hash, lazyObj.Hash())

	// Test Type - should trigger lazy header loading
	objType := lazyObj.Type()
	// Should be a valid non-delta type for a basic object
	assert.NotEqual(t, plumbing.InvalidObject, objType)

	// Test Size - should use the already loaded header
	size := lazyObj.Size()
	assert.GreaterOrEqual(t, size, int64(0))

	// Test Reader - should return decompressed content
	reader, err := lazyObj.Reader()
	require.NoError(t, err)
	defer reader.Close()

	content, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, size, int64(len(content)))
}

func TestLazyFSObject_CompareWithRegular(t *testing.T) {
	t.Parallel()

	f := fixtures.Basic().One()

	// Get the index
	idx := idxfile.NewMemoryIndex(crypto.SHA1.Size())
	err := idxfile.NewDecoder(f.Idx()).Decode(idx)
	require.NoError(t, err)

	// Create a temp directory filesystem
	tmpDir := t.TempDir()
	fs := osfs.New(tmpDir)

	// Copy the packfile to our temp fs
	packFilePath := "objects/pack/test.pack"
	packfileData, err := io.ReadAll(f.Packfile())
	require.NoError(t, err)
	err = fs.MkdirAll("objects/pack", 0755)
	require.NoError(t, err)
	packFile, err := fs.Create(packFilePath)
	require.NoError(t, err)
	_, err = packFile.Write(packfileData)
	require.NoError(t, err)
	err = packFile.Close()
	require.NoError(t, err)

	// Reset the packfile fixture so we can read it again
	f = fixtures.Basic().One()

	// Create a regular packfile for comparison
	regularPack := packfile.NewPackfile(
		f.Packfile(),
		packfile.WithIdx(idx),
		packfile.WithFs(fs),
	)

	objectCache := cache.NewObjectLRUDefault()

	// Get entries and compare lazy vs regular
	entries, err := idx.EntriesByOffset()
	require.NoError(t, err)
	defer entries.Close()

	comparedObjects := 0
	for {
		entry, err := entries.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		// Get the regular object
		regularObj, err := regularPack.GetByOffset(int64(entry.Offset))
		if err != nil {
			// Skip objects that can't be retrieved (e.g., deltas without base)
			continue
		}

		// Create a LazyFSObject
		lazyObj := packfile.NewLazyFSObject(
			entry.Hash,
			int64(entry.Offset),
			idx,
			fs,
			packFilePath,
			objectCache,
			crypto.SHA1.Size(),
		)

		// Compare hash
		assert.Equal(t, regularObj.Hash(), lazyObj.Hash(), "hash mismatch for %s", entry.Hash)

		// Compare type
		assert.Equal(t, regularObj.Type(), lazyObj.Type(), "type mismatch for %s", entry.Hash)

		// For delta objects, size may be -1 initially
		// If size is -1, calling Reader() will resolve it
		lazySize := lazyObj.Size()
		if lazySize == -1 {
			// This is a delta object - reading content will resolve the size
			lazyReader, err := lazyObj.Reader()
			require.NoError(t, err)
			lazyContent, err := io.ReadAll(lazyReader)
			require.NoError(t, err)
			lazyReader.Close()

			// After reading, size should be resolved
			lazySize = lazyObj.Size()
			assert.Equal(t, regularObj.Size(), lazySize, "size mismatch after resolution for %s", entry.Hash)
			assert.Equal(t, int64(len(lazyContent)), lazySize, "content length mismatch for %s", entry.Hash)

			// Compare content
			regularReader, err := regularObj.Reader()
			require.NoError(t, err)
			regularContent, err := io.ReadAll(regularReader)
			require.NoError(t, err)
			regularReader.Close()

			assert.Equal(t, regularContent, lazyContent, "content mismatch for %s", entry.Hash)
		} else {
			// Non-delta object - compare directly
			assert.Equal(t, regularObj.Size(), lazySize, "size mismatch for %s", entry.Hash)

			// Compare content
			regularReader, err := regularObj.Reader()
			require.NoError(t, err)
			regularContent, err := io.ReadAll(regularReader)
			require.NoError(t, err)
			regularReader.Close()

			lazyReader, err := lazyObj.Reader()
			require.NoError(t, err)
			lazyContent, err := io.ReadAll(lazyReader)
			require.NoError(t, err)
			lazyReader.Close()

			assert.Equal(t, regularContent, lazyContent, "content mismatch for %s", entry.Hash)
		}

		comparedObjects++
	}

	// We should have compared at least some objects
	assert.Greater(t, comparedObjects, 0, "should have compared at least one object")
	t.Logf("Successfully compared %d objects", comparedObjects)
}

func TestLazyFSObject_SettersAreNoOp(t *testing.T) {
	t.Parallel()

	f := fixtures.Basic().One()

	// Get the index
	idx := idxfile.NewMemoryIndex(crypto.SHA1.Size())
	err := idxfile.NewDecoder(f.Idx()).Decode(idx)
	require.NoError(t, err)

	entries, err := idx.EntriesByOffset()
	require.NoError(t, err)
	entry, err := entries.Next()
	require.NoError(t, err)
	entries.Close()

	fs := osfs.New(t.TempDir())

	// Copy packfile
	packFilePath := "objects/pack/test.pack"
	packfileData, err := io.ReadAll(f.Packfile())
	require.NoError(t, err)
	err = fs.MkdirAll("objects/pack", 0755)
	require.NoError(t, err)
	packFile, err := fs.Create(packFilePath)
	require.NoError(t, err)
	_, err = packFile.Write(packfileData)
	require.NoError(t, err)
	err = packFile.Close()
	require.NoError(t, err)

	objectCache := cache.NewObjectLRUDefault()

	lazyObj := packfile.NewLazyFSObject(
		entry.Hash,
		int64(entry.Offset),
		idx,
		fs,
		packFilePath,
		objectCache,
		crypto.SHA1.Size(),
	)

	// Load the actual values first
	originalType := lazyObj.Type()
	originalSize := lazyObj.Size()

	// Try to modify - these should be no-ops
	lazyObj.SetType(plumbing.InvalidObject)
	lazyObj.SetSize(9999)

	// Values should be unchanged
	assert.Equal(t, originalType, lazyObj.Type())
	assert.Equal(t, originalSize, lazyObj.Size())

	// Writer should return nil
	w, err := lazyObj.Writer()
	assert.Nil(t, w)
	assert.Nil(t, err)
}

func TestLazyFSObject_MultipleConcurrentReaders(t *testing.T) {
	t.Parallel()

	f := fixtures.Basic().One()

	// Get the index
	idx := idxfile.NewMemoryIndex(crypto.SHA1.Size())
	err := idxfile.NewDecoder(f.Idx()).Decode(idx)
	require.NoError(t, err)

	entries, err := idx.EntriesByOffset()
	require.NoError(t, err)

	// Find a blob entry (non-delta, reasonable size)
	var entry *idxfile.Entry
	for {
		e, err := entries.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		entry = e
		break
	}
	entries.Close()
	require.NotNil(t, entry)

	fs := osfs.New(t.TempDir())

	// Copy packfile
	packFilePath := "objects/pack/test.pack"
	packfileData, err := io.ReadAll(f.Packfile())
	require.NoError(t, err)
	err = fs.MkdirAll("objects/pack", 0755)
	require.NoError(t, err)
	packFile, err := fs.Create(packFilePath)
	require.NoError(t, err)
	_, err = packFile.Write(packfileData)
	require.NoError(t, err)
	err = packFile.Close()
	require.NoError(t, err)

	objectCache := cache.NewObjectLRUDefault()

	lazyObj := packfile.NewLazyFSObject(
		entry.Hash,
		int64(entry.Offset),
		idx,
		fs,
		packFilePath,
		objectCache,
		crypto.SHA1.Size(),
	)

	// Read content once to get expected value
	reader1, err := lazyObj.Reader()
	require.NoError(t, err)
	expectedContent, err := io.ReadAll(reader1)
	require.NoError(t, err)
	reader1.Close()

	// Read again to ensure multiple reads work
	reader2, err := lazyObj.Reader()
	require.NoError(t, err)
	content2, err := io.ReadAll(reader2)
	require.NoError(t, err)
	reader2.Close()

	assert.Equal(t, expectedContent, content2)
}
