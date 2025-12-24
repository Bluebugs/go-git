//go:build windows

package shared

import (
	"os"
	"syscall"
)

// getPackfileIdentity extracts filesystem identity on Windows systems.
// Uses volume serial number and file index to create a unique identity.
func getPackfileIdentity(filePath string) (PackfileIdentity, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		return PackfileIdentity{}, err
	}

	// Open the file to get handle for Windows API call
	pathPtr, err := syscall.UTF16PtrFromString(filePath)
	if err != nil {
		return PackfileIdentity{}, err
	}

	handle, err := syscall.CreateFile(
		pathPtr,
		0,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE,
		nil,
		syscall.OPEN_EXISTING,
		syscall.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		return PackfileIdentity{}, err
	}
	defer syscall.CloseHandle(handle)

	// Get file information by handle
	var fileInfo syscall.ByHandleFileInformation
	err = syscall.GetFileInformationByHandle(handle, &fileInfo)
	if err != nil {
		return PackfileIdentity{}, err
	}

	// Combine FileIndexHigh and FileIndexLow to create a unique inode-like identifier
	inode := (uint64(fileInfo.FileIndexHigh) << 32) | uint64(fileInfo.FileIndexLow)

	// Use volume serial number as device ID
	device := uint64(fileInfo.VolumeSerialNumber)

	return PackfileIdentity{
		Inode:  inode,
		Device: device,
		Size:   fi.Size(),
		Mtime:  fi.ModTime().UnixNano(),
	}, nil
}

// Ensure ByHandleFileInformation is available in syscall package
// If not, we need to define it
var _ = (*syscall.ByHandleFileInformation)(nil)

// If the above line causes a compile error, uncomment the following:
/*
type byHandleFileInformation struct {
	FileAttributes     uint32
	CreationTime       syscall.Filetime
	LastAccessTime     syscall.Filetime
	LastWriteTime      syscall.Filetime
	VolumeSerialNumber uint32
	FileSizeHigh       uint32
	FileSizeLow        uint32
	NumberOfLinks      uint32
	FileIndexHigh      uint32
	FileIndexLow       uint32
}

var (
	modkernel32 = syscall.NewLazyDLL("kernel32.dll")
	procGetFileInformationByHandle = modkernel32.NewProc("GetFileInformationByHandle")
)

func getFileInformationByHandle(handle syscall.Handle, fileInfo *byHandleFileInformation) error {
	r1, _, e1 := syscall.Syscall(procGetFileInformationByHandle.Addr(), 2,
		uintptr(handle), uintptr(unsafe.Pointer(fileInfo)), 0)
	if r1 == 0 {
		return e1
	}
	return nil
}
*/
