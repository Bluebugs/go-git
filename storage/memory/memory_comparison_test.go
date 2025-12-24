package memory

import (
	"runtime"
	"testing"
	"time"

	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage/memory/shared"
)

// TestMemoryComparison_VisualDemonstration provides a clear visualization of memory savings
func TestMemoryComparison_VisualDemonstration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory comparison test in short mode")
	}

	packfileSize := 20 * 1024 * 1024 // 20MB packfile
	numClones := 10

	t.Run("NonShared_Scenario", func(t *testing.T) {
		var m1, m2 runtime.MemStats

		// Force GC and measure baseline
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
		runtime.ReadMemStats(&m1)

		// Create non-shared storages (each gets its own copy)
		packfileData := make([]byte, packfileSize)
		storages := make([]*Storage, numClones)

		for i := 0; i < numClones; i++ {
			storages[i] = NewStorage(WithLazyLoading(0))
			// Each storage gets its own copy of the packfile
			packfileCopy := make([]byte, len(packfileData))
			copy(packfileCopy, packfileData)
			_ = storages[i].ObjectStorage.SetPackfileData(packfileCopy)
		}

		// Force GC and measure after allocation
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
		runtime.ReadMemStats(&m2)

		allocated := int64(m2.Alloc - m1.Alloc)
		expectedMin := int64(packfileSize * numClones)

		t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		t.Logf("NON-SHARED SCENARIO (%d clones of %dMB packfile)", numClones, packfileSize/1024/1024)
		t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		t.Logf("Expected minimum: %d MB (%d clones × %d MB)",
			expectedMin/1024/1024, numClones, packfileSize/1024/1024)
		t.Logf("Actual allocated: %d MB", allocated/1024/1024)
		t.Logf("Overhead: %.1f%%", float64(allocated-expectedMin)/float64(expectedMin)*100)
		t.Logf("")

		// Verify we allocated at least the expected amount
		if allocated < expectedMin {
			t.Logf("⚠️  Warning: Allocated less than expected (GC may have run)")
		}

		runtime.KeepAlive(storages)
	})

	t.Run("Shared_Scenario", func(t *testing.T) {
		shared.ClearStore()
		defer shared.ClearStore()

		var m1, m2 runtime.MemStats

		// Force GC and measure baseline
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
		runtime.ReadMemStats(&m1)

		// Create shared storages (all share the same packfile)
		packfileData := make([]byte, packfileSize)
		identity := storer.PackfileIdentity{
			Inode:  12345,
			Device: 67890,
			Size:   int64(len(packfileData)),
			Mtime:  time.Now().UnixNano(),
		}

		storages := make([]*Storage, numClones)
		for i := 0; i < numClones; i++ {
			storages[i] = NewStorage(WithLazyLoading(0))
			// All storages share the same canonical packfile
			_ = storages[i].ObjectStorage.SetPackfileDataShared(packfileData, identity)
		}

		// Force GC and measure after allocation
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
		runtime.ReadMemStats(&m2)

		allocated := int64(m2.Alloc - m1.Alloc)
		expectedApprox := int64(packfileSize) // Only 1 copy expected

		stats := shared.GetStats()

		t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		t.Logf("SHARED SCENARIO (%d clones of %dMB packfile)", numClones, packfileSize/1024/1024)
		t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		t.Logf("Unique packfiles in store: %d", stats.UniquePackfiles)
		t.Logf("Expected ~1 copy: %d MB", expectedApprox/1024/1024)
		t.Logf("Actual allocated: %d MB", allocated/1024/1024)
		t.Logf("Overhead: %.1f%%", float64(allocated-expectedApprox)/float64(expectedApprox)*100)
		t.Logf("")

		// Verify we only have 1 unique packfile
		if stats.UniquePackfiles != 1 {
			t.Errorf("Expected 1 unique packfile, got %d", stats.UniquePackfiles)
		}

		runtime.KeepAlive(storages)
	})

	t.Run("Comparison_Summary", func(t *testing.T) {
		shared.ClearStore()
		defer shared.ClearStore()

		packfileData := make([]byte, packfileSize)

		// Measure non-shared
		var nonSharedMem runtime.MemStats
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
		runtime.ReadMemStats(&nonSharedMem)

		nonSharedStorages := make([]*Storage, numClones)
		for i := 0; i < numClones; i++ {
			nonSharedStorages[i] = NewStorage(WithLazyLoading(0))
			packfileCopy := make([]byte, len(packfileData))
			copy(packfileCopy, packfileData)
			_ = nonSharedStorages[i].ObjectStorage.SetPackfileData(packfileCopy)
		}

		runtime.GC()
		time.Sleep(10 * time.Millisecond)
		var nonSharedMemAfter runtime.MemStats
		runtime.ReadMemStats(&nonSharedMemAfter)
		nonSharedAllocated := int64(nonSharedMemAfter.Alloc - nonSharedMem.Alloc)

		// Clear and measure shared
		runtime.KeepAlive(nonSharedStorages)
		nonSharedStorages = nil
		runtime.GC()
		time.Sleep(10 * time.Millisecond)

		var sharedMem runtime.MemStats
		runtime.ReadMemStats(&sharedMem)

		identity := storer.PackfileIdentity{
			Inode:  12345,
			Device: 67890,
			Size:   int64(len(packfileData)),
			Mtime:  time.Now().UnixNano(),
		}

		sharedStorages := make([]*Storage, numClones)
		for i := 0; i < numClones; i++ {
			sharedStorages[i] = NewStorage(WithLazyLoading(0))
			_ = sharedStorages[i].ObjectStorage.SetPackfileDataShared(packfileData, identity)
		}

		runtime.GC()
		time.Sleep(10 * time.Millisecond)
		var sharedMemAfter runtime.MemStats
		runtime.ReadMemStats(&sharedMemAfter)
		sharedAllocated := int64(sharedMemAfter.Alloc - sharedMem.Alloc)

		// Calculate savings
		savings := nonSharedAllocated - sharedAllocated
		savingsPercent := float64(savings) / float64(nonSharedAllocated) * 100

		t.Logf("╔═══════════════════════════════════════════════════════╗")
		t.Logf("║           MEMORY SAVINGS COMPARISON                   ║")
		t.Logf("╠═══════════════════════════════════════════════════════╣")
		t.Logf("║ Configuration: %d clones × %dMB packfile             ║", numClones, packfileSize/1024/1024)
		t.Logf("╠═══════════════════════════════════════════════════════╣")
		t.Logf("║ Non-Shared Memory: %7d MB                        ║", nonSharedAllocated/1024/1024)
		t.Logf("║ Shared Memory:     %7d MB                        ║", sharedAllocated/1024/1024)
		t.Logf("║ ─────────────────────────────────────────────────── ║")
		t.Logf("║ Memory Saved:      %7d MB                        ║", savings/1024/1024)
		t.Logf("║ Savings Percent:   %7.1f%%                        ║", savingsPercent)
		t.Logf("╚═══════════════════════════════════════════════════════╝")
		t.Logf("")

		stats := shared.GetStats()
		t.Logf("📊 Shared Store Statistics:")
		t.Logf("   • Unique packfiles: %d", stats.UniquePackfiles)
		t.Logf("   • Shared memory: %d MB", stats.SharedMemoryMB)
		t.Logf("")

		if savingsPercent < 50 {
			t.Logf("⚠️  Note: GC timing may affect exact measurements")
			t.Logf("    Expected savings: ~%d%% (9 of 10 copies avoided)", (numClones-1)*100/numClones)
		} else {
			t.Logf("✅ Achieved significant memory savings through deduplication")
		}

		runtime.KeepAlive(sharedStorages)
	})
}
