package memory

import (
	"testing"

	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/stretchr/testify/assert"
)

// TestWithObjectFormat tests the existing WithObjectFormat option.
func TestWithObjectFormat(t *testing.T) {
	opts := newOptions()

	// Test SHA1 (default)
	assert.Equal(t, formatcfg.SHA1, opts.objectFormat)

	// Test SHA256
	WithObjectFormat(formatcfg.SHA256)(&opts)
	assert.Equal(t, formatcfg.SHA256, opts.objectFormat)
}

// TestWithLazyLoading_DefaultSettings tests that WithLazyLoading
// sets all the necessary options for lazy loading.
func TestWithLazyLoading_DefaultSettings(t *testing.T) {
	opts := newOptions()

	// Apply lazy loading with threshold 0 (all blobs lazy)
	WithLazyLoading(0)(&opts)

	assert.True(t, opts.lazyLoadBlobs, "lazyLoadBlobs should be enabled")
	assert.Equal(t, int64(0), opts.lazyThreshold, "lazyThreshold should be 0")
	assert.True(t, opts.packfileRetention, "packfileRetention should be enabled")
}

// TestWithLazyLoading_CustomThreshold tests lazy loading with a custom threshold.
func TestWithLazyLoading_CustomThreshold(t *testing.T) {
	opts := newOptions()

	// Apply lazy loading with 1KB threshold
	threshold := int64(1024)
	WithLazyLoading(threshold)(&opts)

	assert.True(t, opts.lazyLoadBlobs)
	assert.Equal(t, threshold, opts.lazyThreshold)
	assert.True(t, opts.packfileRetention)
}

// TestWithLazyLoading_LargeThreshold tests lazy loading with large threshold
// (e.g., only very large blobs are lazy loaded).
func TestWithLazyLoading_LargeThreshold(t *testing.T) {
	opts := newOptions()

	// Apply lazy loading with 10MB threshold
	threshold := int64(10 * 1024 * 1024)
	WithLazyLoading(threshold)(&opts)

	assert.True(t, opts.lazyLoadBlobs)
	assert.Equal(t, threshold, opts.lazyThreshold)
	assert.True(t, opts.packfileRetention)
}

// TestNewOptions_Defaults tests that newOptions returns expected defaults.
func TestNewOptions_Defaults(t *testing.T) {
	opts := newOptions()

	assert.Equal(t, formatcfg.SHA1, opts.objectFormat)
	assert.False(t, opts.lazyLoadBlobs, "lazyLoadBlobs should be false by default")
	assert.Equal(t, int64(0), opts.lazyThreshold, "lazyThreshold should be 0 by default")
	assert.False(t, opts.packfileRetention, "packfileRetention should be false by default")
}

// TestMultipleOptions tests applying multiple options together.
func TestMultipleOptions(t *testing.T) {
	opts := newOptions()

	// Apply both object format and lazy loading
	WithObjectFormat(formatcfg.SHA256)(&opts)
	WithLazyLoading(2048)(&opts)

	assert.Equal(t, formatcfg.SHA256, opts.objectFormat)
	assert.True(t, opts.lazyLoadBlobs)
	assert.Equal(t, int64(2048), opts.lazyThreshold)
	assert.True(t, opts.packfileRetention)
}

// TestNewStorage_WithLazyLoading tests creating storage with lazy loading option.
func TestNewStorage_WithLazyLoading(t *testing.T) {
	// Create storage with lazy loading enabled
	storage := NewStorage(WithLazyLoading(0))

	assert.NotNil(t, storage)
	assert.True(t, storage.options.lazyLoadBlobs)
	assert.Equal(t, int64(0), storage.options.lazyThreshold)
	assert.True(t, storage.options.packfileRetention)
}

// TestNewStorage_WithMultipleOptions tests creating storage with multiple options.
func TestNewStorage_WithMultipleOptions(t *testing.T) {
	storage := NewStorage(
		WithObjectFormat(formatcfg.SHA256),
		WithLazyLoading(1024),
	)

	assert.NotNil(t, storage)
	assert.Equal(t, formatcfg.SHA256, storage.options.objectFormat)
	assert.True(t, storage.options.lazyLoadBlobs)
	assert.Equal(t, int64(1024), storage.options.lazyThreshold)
}

// TestNewStorage_DefaultNoLazyLoading tests that storage defaults to non-lazy.
func TestNewStorage_DefaultNoLazyLoading(t *testing.T) {
	storage := NewStorage()

	assert.NotNil(t, storage)
	assert.False(t, storage.options.lazyLoadBlobs)
	assert.Equal(t, int64(0), storage.options.lazyThreshold)
	assert.False(t, storage.options.packfileRetention)
}
