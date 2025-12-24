package memory

import (
	"runtime"
	"testing"
	"time"

	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage/memory/shared"
)

// BenchmarkPackfileStorage_NonShared measures memory and performance without sharing
func BenchmarkPackfileStorage_NonShared(b *testing.B) {
	packfileData := makeTestPackfile(10 * 1024 * 1024) // 10MB packfile

	b.ReportAllocs()

	for b.Loop() {
		storage := NewStorage(WithLazyLoading(0))
		_ = storage.ObjectStorage.SetPackfileData(packfileData)
	}
}

// BenchmarkPackfileStorage_Shared measures memory and performance with sharing
func BenchmarkPackfileStorage_Shared(b *testing.B) {
	shared.ClearStore()
	defer shared.ClearStore()

	packfileData := makeTestPackfile(10 * 1024 * 1024) // 10MB packfile

	identity := storer.PackfileIdentity{
		Inode:  12345,
		Device: 67890,
		Size:   int64(len(packfileData)),
		Mtime:  time.Now().UnixNano(),
	}

	b.ReportAllocs()

	for b.Loop() {
		storage := NewStorage(WithLazyLoading(0))
		_ = storage.ObjectStorage.SetPackfileDataShared(packfileData, identity)
	}
}

// BenchmarkMultipleClones_NonShared benchmarks memory usage for multiple non-shared clones
func BenchmarkMultipleClones_NonShared(b *testing.B) {
	packfileData := makeTestPackfile(10 * 1024 * 1024) // 10MB packfile
	const numClones = 10

	b.Run("10_clones_10MB_each", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			storages := make([]*Storage, numClones)
			for j := 0; j < numClones; j++ {
				storages[j] = NewStorage(WithLazyLoading(0))
				_ = storages[j].ObjectStorage.SetPackfileData(packfileData)
			}

			// Force to keep storages alive
			runtime.KeepAlive(storages)
		}
	})
}

// BenchmarkMultipleClones_Shared benchmarks memory usage for multiple shared clones
func BenchmarkMultipleClones_Shared(b *testing.B) {
	packfileData := makeTestPackfile(10 * 1024 * 1024) // 10MB packfile
	const numClones = 10

	b.Run("10_clones_shared_10MB", func(b *testing.B) {
		shared.ClearStore()
		defer shared.ClearStore()

		identity := storer.PackfileIdentity{
			Inode:  12345,
			Device: 67890,
			Size:   int64(len(packfileData)),
			Mtime:  time.Now().UnixNano(),
		}

		b.ReportAllocs()

		for b.Loop() {
			storages := make([]*Storage, numClones)
			for j := 0; j < numClones; j++ {
				storages[j] = NewStorage(WithLazyLoading(0))
				_ = storages[j].ObjectStorage.SetPackfileDataShared(packfileData, identity)
			}

			// Force to keep storages alive
			runtime.KeepAlive(storages)

			// Verify sharing
			stats := shared.GetStats()
			if stats.UniquePackfiles != 1 {
				b.Fatalf("Expected 1 unique packfile, got %d", stats.UniquePackfiles)
			}

			shared.ClearStore()
		}
	})
}

// BenchmarkGetPackfileReader_Legacy benchmarks reader access without sharing
func BenchmarkGetPackfileReader_Legacy(b *testing.B) {
	storage := NewStorage(WithLazyLoading(0))
	packfileData := makeTestPackfile(1024 * 1024) // 1MB
	_ = storage.ObjectStorage.SetPackfileData(packfileData)

	b.ReportAllocs()

	for b.Loop() {
		reader := storage.ObjectStorage.GetPackfileReader()
		if reader == nil {
			b.Fatal("reader is nil")
		}
	}
}

// BenchmarkGetPackfileReader_Shared benchmarks reader access with sharing
func BenchmarkGetPackfileReader_Shared(b *testing.B) {
	shared.ClearStore()
	defer shared.ClearStore()

	storage := NewStorage(WithLazyLoading(0))
	packfileData := makeTestPackfile(1024 * 1024) // 1MB

	identity := storer.PackfileIdentity{
		Inode:  12345,
		Device: 67890,
		Size:   int64(len(packfileData)),
		Mtime:  time.Now().UnixNano(),
	}

	_ = storage.ObjectStorage.SetPackfileDataShared(packfileData, identity)

	b.ReportAllocs()

	for b.Loop() {
		reader := storage.ObjectStorage.GetPackfileReader()
		if reader == nil {
			b.Fatal("reader is nil")
		}
	}
}

// BenchmarkConcurrentAccess_Shared benchmarks concurrent access to shared packfiles
func BenchmarkConcurrentAccess_Shared(b *testing.B) {
	shared.ClearStore()
	defer shared.ClearStore()

	packfileData := makeTestPackfile(10 * 1024 * 1024) // 10MB
	identity := storer.PackfileIdentity{
		Inode:  12345,
		Device: 67890,
		Size:   int64(len(packfileData)),
		Mtime:  time.Now().UnixNano(),
	}

	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			storage := NewStorage(WithLazyLoading(0))
			_ = storage.ObjectStorage.SetPackfileDataShared(packfileData, identity)
			reader := storage.ObjectStorage.GetPackfileReader()
			if reader == nil {
				b.Fatal("reader is nil")
			}
		}
	})
}

// BenchmarkMemoryFootprint shows actual memory usage comparison
func BenchmarkMemoryFootprint(b *testing.B) {
	packfileSize := 50 * 1024 * 1024 // 50MB packfile
	numClones := 10

	b.Run("NonShared_10x50MB", func(b *testing.B) {
		for b.Loop() {
			var m1, m2 runtime.MemStats

			// Measure before
			runtime.GC()
			runtime.ReadMemStats(&m1)

			// Create non-shared storages
			packfileData := makeTestPackfile(packfileSize)
			storages := make([]*Storage, numClones)
			for j := 0; j < numClones; j++ {
				storages[j] = NewStorage(WithLazyLoading(0))
				_ = storages[j].ObjectStorage.SetPackfileData(packfileData)
			}

			// Measure after
			runtime.GC()
			runtime.ReadMemStats(&m2)

			allocated := m2.Alloc - m1.Alloc
			b.ReportMetric(float64(allocated)/1024/1024, "MB")
			b.ReportMetric(float64(allocated)/(float64(packfileSize)*float64(numClones)), "overhead")

			runtime.KeepAlive(storages)
		}
	})

	b.Run("Shared_10x50MB", func(b *testing.B) {
		for b.Loop() {
			shared.ClearStore()

			var m1, m2 runtime.MemStats

			// Measure before
			runtime.GC()
			runtime.ReadMemStats(&m1)

			// Create shared storages
			packfileData := makeTestPackfile(packfileSize)
			identity := storer.PackfileIdentity{
				Inode:  12345,
				Device: 67890,
				Size:   int64(len(packfileData)),
				Mtime:  time.Now().UnixNano(),
			}

			storages := make([]*Storage, numClones)
			for j := 0; j < numClones; j++ {
				storages[j] = NewStorage(WithLazyLoading(0))
				_ = storages[j].ObjectStorage.SetPackfileDataShared(packfileData, identity)
			}

			// Measure after
			runtime.GC()
			runtime.ReadMemStats(&m2)

			allocated := m2.Alloc - m1.Alloc
			b.ReportMetric(float64(allocated)/1024/1024, "MB")
			b.ReportMetric(float64(allocated)/float64(packfileSize), "overhead")

			stats := shared.GetStats()
			b.ReportMetric(float64(stats.UniquePackfiles), "unique_packfiles")

			runtime.KeepAlive(storages)
			shared.ClearStore()
		}
	})
}

// makeTestPackfile creates a test packfile of the specified size
func makeTestPackfile(size int) []byte {
	data := make([]byte, size)

	// Fill with semi-realistic packfile structure
	// PACK header
	copy(data[0:4], []byte("PACK"))
	data[4] = 0 // Version
	data[5] = 0
	data[6] = 0
	data[7] = 2 // Version 2

	// Fill rest with pseudo-random but compressible data
	// (to simulate real packfile characteristics)
	for i := 12; i < size; i++ {
		data[i] = byte((i * 31) % 256)
	}

	return data
}
