// Package shared provides shared storage facilities for memory-based git repositories.
//
// This package enables multiple memory storage instances to share the same underlying
// packfile and index data when they represent clones of the same repository. This is
// particularly useful for reducing memory usage when creating multiple clones from
// the same shared local repository.
//
// # Packfile Deduplication
//
// The package uses Go 1.23's unique.Handle to canonicalize packfile data based on
// filesystem metadata (inode, device, size, mtime). When multiple storage instances
// clone from the same shared repository, they all reference the same packfile files
// on disk. The shared package recognizes this and ensures only one copy of the
// packfile data exists in memory.
//
// # Example
//
//	// Clone the same repository multiple times
//	for i := 0; i < 10; i++ {
//	    storage := memory.NewStorage(memory.WithLazyLoading(0))
//	    repo, _ := git.Clone(storage, nil, &git.CloneOptions{
//	        URL: "/path/to/shared/repo",
//	        Shared: true,
//	    })
//	    // All 10 clones share the same packfile in memory
//	    // Memory usage: ~1x packfile size instead of 10x
//	}
//
// # Thread Safety
//
// All public functions in this package are thread-safe and can be called
// concurrently from multiple goroutines.
//
// # Automatic Cleanup
//
// The package uses weak references internally (via unique.Handle), which means
// packfile data is automatically garbage collected when no storage instances
// reference it anymore. No manual cleanup is required.
package shared
