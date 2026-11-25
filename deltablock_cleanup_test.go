package backupstore

import (
	"io"
	"strings"
	"testing"
	"time"
)

// fakeDriver is a minimal BackupStoreDriver used to check that GC batches
// deletions when the driver supports it and falls back to one Remove per block
// when it does not.
type fakeDriver struct {
	removed     []string
	batchCalls  int
	batchKeys   int
	batchMaxLen int
}

func (f *fakeDriver) Kind() string                       { return "fake" }
func (f *fakeDriver) GetURL() string                     { return "fake://bucket/" }
func (f *fakeDriver) FileExists(filePath string) bool    { return true }
func (f *fakeDriver) FileSize(filePath string) int64     { return 1 }
func (f *fakeDriver) FileTime(filePath string) time.Time { return time.Now() }
func (f *fakeDriver) Remove(path string) error {
	f.removed = append(f.removed, path)
	return nil
}
func (f *fakeDriver) Read(src string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(`{"Name":"vol-1"}`)), nil
}
func (f *fakeDriver) Write(dst string, rs io.ReadSeeker) error { return nil }
func (f *fakeDriver) List(path string) ([]string, error)       { return nil, nil }
func (f *fakeDriver) Upload(src, dst string) error             { return nil }
func (f *fakeDriver) Download(src, dst string) error           { return nil }

// batchDriver is the same driver with the optional BatchRemover interface, as
// the s3 driver has it.
type batchDriver struct {
	fakeDriver
}

func (b *batchDriver) RemoveBatch(paths []string) error {
	b.batchCalls++
	b.batchKeys += len(paths)
	if len(paths) > b.batchMaxLen {
		b.batchMaxLen = len(paths)
	}
	b.removed = append(b.removed, paths...)
	return nil
}

func blockInfos(count int) map[string]*BlockInfo {
	infos := make(map[string]*BlockInfo, count)
	for i := 0; i < count; i++ {
		checksum := strings.Repeat("a", 40)[:40-i%2] + "x"
		infos[checksum] = &BlockInfo{
			checksum: checksum,
			path:     getBlockFilePath("vol-1", checksum),
			refcount: 0,
		}
	}
	return infos
}

// TestCleanupBlocksUsesBatchRemover asserts GC hands the whole list of blocks
// it already has to a driver that can batch, instead of calling Remove (and on
// S3 an extra ListObjects) once per block.
func TestCleanupBlocksUsesBatchRemover(t *testing.T) {
	driver := &batchDriver{}
	infos := blockInfos(1500)

	if err := cleanupBlocks(driver, infos, "vol-1"); err != nil {
		t.Fatalf("cleanupBlocks: %v", err)
	}

	if driver.batchCalls == 0 {
		t.Fatal("GC did not use BatchRemover on a driver that supports it")
	}
	if driver.batchKeys != len(infos) {
		t.Fatalf("GC passed %d blocks to RemoveBatch, expected %d", driver.batchKeys, len(infos))
	}
	if len(driver.removed) != len(infos) {
		t.Fatalf("GC removed %d blocks, expected %d", len(driver.removed), len(infos))
	}

	// The batching itself happens in the driver (chunks of MaxBatchDeleteKeys),
	// GC hands over the full list once.
	if driver.batchCalls != 1 {
		t.Fatalf("GC called RemoveBatch %d times, expected 1", driver.batchCalls)
	}
}

// TestCleanupBlocksFallsBackToRemoveForDriversWithoutBatchRemover makes sure
// NFS/CIFS/local filesystem drivers, which do not implement BatchRemover, keep
// working exactly as before.
func TestCleanupBlocksFallsBackToRemoveForDriversWithoutBatchRemover(t *testing.T) {
	driver := &fakeDriver{}
	infos := blockInfos(3)

	if err := cleanupBlocks(driver, infos, "vol-1"); err != nil {
		t.Fatalf("cleanupBlocks: %v", err)
	}

	if _, ok := interface{}(driver).(BatchRemover); ok {
		t.Fatal("the plain fake driver must not implement BatchRemover")
	}
	if len(driver.removed) != len(infos) {
		t.Fatalf("GC removed %d blocks, expected %d", len(driver.removed), len(infos))
	}
}

// TestBlockFilePathIsNotAPrefix locks in the assumption the s3 Remove() relies
// on: block paths are single keys, not prefixes, so they can be deleted without
// listing first.
func TestBlockFilePathIsNotAPrefix(t *testing.T) {
	path := getBlockFilePath("vol-1", strings.Repeat("a", 40))
	if strings.HasSuffix(path, "/") {
		t.Fatalf("block path %q looks like a prefix, Remove() would list before deleting", path)
	}
	if !strings.HasSuffix(path, BLK_SUFFIX) {
		t.Fatalf("unexpected block path %q", path)
	}
}
