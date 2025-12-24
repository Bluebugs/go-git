package shared

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGetPackfileIdentity(t *testing.T) {
	// Create a temporary file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.pack")

	// Write some test data
	testData := []byte("test packfile data")
	if err := os.WriteFile(testFile, testData, 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Get identity
	identity, err := GetPackfileIdentity(testFile)
	if err != nil {
		t.Fatalf("GetPackfileIdentity failed: %v", err)
	}

	// Verify identity fields are populated
	if identity.Inode == 0 {
		t.Error("expected non-zero inode")
	}
	if identity.Device == 0 {
		t.Error("expected non-zero device")
	}
	if identity.Size != int64(len(testData)) {
		t.Errorf("expected size %d, got %d", len(testData), identity.Size)
	}
	if identity.Mtime == 0 {
		t.Error("expected non-zero mtime")
	}
}

func TestGetPackfileIdentity_SameFile(t *testing.T) {
	// Create a temporary file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.pack")

	testData := []byte("test packfile data")
	if err := os.WriteFile(testFile, testData, 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Get identity twice
	identity1, err := GetPackfileIdentity(testFile)
	if err != nil {
		t.Fatalf("GetPackfileIdentity failed: %v", err)
	}

	identity2, err := GetPackfileIdentity(testFile)
	if err != nil {
		t.Fatalf("GetPackfileIdentity failed: %v", err)
	}

	// They should be identical
	if identity1 != identity2 {
		t.Errorf("expected identical identities for same file, got %+v and %+v", identity1, identity2)
	}
}

func TestGetPackfileIdentity_DifferentFiles(t *testing.T) {
	tmpDir := t.TempDir()

	// Create two different files
	testFile1 := filepath.Join(tmpDir, "test1.pack")
	testFile2 := filepath.Join(tmpDir, "test2.pack")

	testData := []byte("test packfile data")
	if err := os.WriteFile(testFile1, testData, 0644); err != nil {
		t.Fatalf("failed to create test file 1: %v", err)
	}
	if err := os.WriteFile(testFile2, testData, 0644); err != nil {
		t.Fatalf("failed to create test file 2: %v", err)
	}

	// Get identities
	identity1, err := GetPackfileIdentity(testFile1)
	if err != nil {
		t.Fatalf("GetPackfileIdentity failed for file 1: %v", err)
	}

	identity2, err := GetPackfileIdentity(testFile2)
	if err != nil {
		t.Fatalf("GetPackfileIdentity failed for file 2: %v", err)
	}

	// They should be different (at least inode should differ)
	if identity1.Inode == identity2.Inode && identity1.Device == identity2.Device {
		t.Errorf("expected different identities for different files, got same inode/device")
	}
}

func TestGetPackfileIdentity_ModifiedFile(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.pack")

	// Create file
	testData := []byte("test packfile data")
	if err := os.WriteFile(testFile, testData, 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Get initial identity
	identity1, err := GetPackfileIdentity(testFile)
	if err != nil {
		t.Fatalf("GetPackfileIdentity failed: %v", err)
	}

	// Wait a bit to ensure different mtime
	time.Sleep(10 * time.Millisecond)

	// Touch the file (update mtime)
	now := time.Now()
	if err := os.Chtimes(testFile, now, now); err != nil {
		t.Fatalf("failed to touch file: %v", err)
	}

	// Get new identity
	identity2, err := GetPackfileIdentity(testFile)
	if err != nil {
		t.Fatalf("GetPackfileIdentity failed: %v", err)
	}

	// Inode and device should be same, but mtime should differ
	if identity1.Inode != identity2.Inode {
		t.Errorf("expected same inode after touch, got %d and %d", identity1.Inode, identity2.Inode)
	}
	if identity1.Device != identity2.Device {
		t.Errorf("expected same device after touch, got %d and %d", identity1.Device, identity2.Device)
	}
	if identity1.Mtime == identity2.Mtime {
		t.Errorf("expected different mtime after touch, got same: %d", identity1.Mtime)
	}
}

func TestGetPackfileIdentity_NonExistent(t *testing.T) {
	tmpDir := t.TempDir()
	nonExistent := filepath.Join(tmpDir, "nonexistent.pack")

	_, err := GetPackfileIdentity(nonExistent)
	if err == nil {
		t.Error("expected error for non-existent file, got nil")
	}
	if !os.IsNotExist(err) {
		t.Errorf("expected IsNotExist error, got: %v", err)
	}
}
