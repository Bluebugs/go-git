package packfile

import (
	"bufio"

	"github.com/go-git/go-git/v6/plumbing"
	format "github.com/go-git/go-git/v6/plumbing/format/config"
)

// ScannerOption configures a Scanner.
type ScannerOption func(*Scanner)

// WithSHA256 enables the SHA256 hashing while scanning a pack file.
func WithSHA256() ScannerOption {
	return func(s *Scanner) {
		h := plumbing.NewHasher(format.SHA256, plumbing.AnyObject, 0)
		s.objectIDSize = format.SHA256Size
		s.hasher256 = &h
	}
}

// WithBufioReader passes a bufio.Reader for scanner to use.
// It is used for reusing the buffer across multiple scanner instances.
func WithBufioReader(buf *bufio.Reader) ScannerOption {
	return func(s *Scanner) {
		s.rbuf = buf
	}
}

// WithLazyMode enables lazy mode for the scanner.
//
// When lazy mode is enabled, the scanner skips decompressing object content
// and only records the offset where compressed data begins (ContentOffset).
// This is significantly more memory-efficient for scenarios where objects
// may not need to be read immediately.
//
// In lazy mode:
//   - ObjectHeader.content will be nil (not decompressed)
//   - ObjectHeader.ContentOffset will be set (points to compressed data)
//   - All other metadata (Type, Size, Hash, CRC, etc.) is still calculated
//
// This is used in conjunction with LazyMemoryObject for memory-efficient
// git operations like clone → modify → commit → push without reading all files.
//
// Example:
//
//	scanner := packfile.NewScanner(reader, packfile.WithLazyMode(true))
func WithLazyMode(enabled bool) ScannerOption {
	return func(s *Scanner) {
		s.lazyMode = enabled
	}
}
