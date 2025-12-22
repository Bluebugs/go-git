package memory

import formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"

type options struct {
	objectFormat formatcfg.ObjectFormat

	// Lazy loading options
	lazyLoadBlobs     bool  // Enable lazy loading for blob objects
	lazyThreshold     int64 // Objects above this size are lazy loaded (0 = all blobs)
	packfileRetention bool  // Keep packfile in memory for lazy access
}

func newOptions() options {
	return options{
		objectFormat:      formatcfg.SHA1,
		lazyLoadBlobs:     false,
		lazyThreshold:     0,
		packfileRetention: false,
	}
}

// StorageOption is a function that configures storage options.
type StorageOption func(*options)

// WithObjectFormat sets the storage's object format.
func WithObjectFormat(of formatcfg.ObjectFormat) StorageOption {
	return func(o *options) {
		o.objectFormat = of
	}
}

// WithLazyLoading enables lazy loading for blob objects.
//
// When lazy loading is enabled, blob objects are not decompressed during
// clone or fetch operations. Instead, they remain compressed in memory
// and are only decompressed when their content is accessed via Reader().
//
// This significantly reduces memory usage for workflows that don't need
// to read all files (e.g., clone → modify → commit → push).
//
// The threshold parameter controls which blobs are lazy loaded:
//   - 0: All blobs are lazy loaded (maximum memory savings)
//   - >0: Only blobs larger than threshold bytes are lazy loaded
//
// Examples:
//   - WithLazyLoading(0)        // All blobs lazy (recommended for most cases)
//   - WithLazyLoading(1024)     // Only blobs > 1KB are lazy
//   - WithLazyLoading(1<<20)    // Only blobs > 1MB are lazy
//
// Note: Lazy loading requires packfile retention in memory. The packfile
// data remains in compressed form, which is still more memory-efficient
// than decompressing all objects.
func WithLazyLoading(threshold int64) StorageOption {
	return func(o *options) {
		o.lazyLoadBlobs = true
		o.lazyThreshold = threshold
		o.packfileRetention = true
	}
}
