package backupstore

import (
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

type cleanupTestDriver struct {
	mockStoreDriver

	mu           sync.Mutex
	removeCalls  []string
	removeErrors map[string]error
	active       int
	maxActive    int
	writeCalls   int
}

func newCleanupTestDriver(t *testing.T, volume *Volume) *cleanupTestDriver {
	t.Helper()

	driver := &cleanupTestDriver{
		mockStoreDriver: mockStoreDriver{
			fs:      afero.NewMemMapFs(),
			destURL: "cleanup-test://localhost",
		},
		removeErrors: map[string]error{},
	}

	data, err := json.Marshal(volume)
	if err != nil {
		t.Fatal(err)
	}
	if err := driver.fs.MkdirAll(filepath.Dir(getVolumeFilePath(volume.Name)), 0755); err != nil {
		t.Fatal(err)
	}
	if err := afero.WriteFile(driver.fs, getVolumeFilePath(volume.Name), data, 0644); err != nil {
		t.Fatal(err)
	}

	return driver
}

func (d *cleanupTestDriver) Remove(path string) error {
	d.mu.Lock()
	d.removeCalls = append(d.removeCalls, path)
	d.active++
	if d.active > d.maxActive {
		d.maxActive = d.active
	}
	err := d.removeErrors[path]
	d.mu.Unlock()

	// Make an accidental concurrent fallback observable while keeping this
	// test fast. Drivers implementing only Remove historically run serially.
	time.Sleep(5 * time.Millisecond)

	d.mu.Lock()
	d.active--
	d.mu.Unlock()
	return err
}

func (d *cleanupTestDriver) Write(dst string, rs io.ReadSeeker) error {
	data, err := io.ReadAll(rs)
	if err != nil {
		return err
	}

	d.mu.Lock()
	d.writeCalls++
	d.mu.Unlock()

	if err := d.fs.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	return afero.WriteFile(d.fs, dst, data, 0644)
}

func (d *cleanupTestDriver) calls() (removeCalls []string, maxActive, writeCalls int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.removeCalls...), d.maxActive, d.writeCalls
}

type exactCleanupTestDriver struct {
	*cleanupTestDriver

	exactCalls int
	exactPaths []string
	exactErr   error
}

func (d *exactCleanupTestDriver) RemoveFiles(paths []string) error {
	d.exactCalls++
	d.exactPaths = append(d.exactPaths, paths...)
	return d.exactErr
}

func TestCleanupBlocksUsesExactFileRemoverForSafeBlocks(t *testing.T) {
	volumeName := "cleanup-volume"
	driver := &exactCleanupTestDriver{
		cleanupTestDriver: newCleanupTestDriver(t, &Volume{Name: volumeName, BlockCount: 99}),
	}
	blocks := map[string]*BlockInfo{
		"delete-1":           {checksum: "delete-1", path: "blocks/delete-1.blk"},
		"delete-2":           {checksum: "delete-2", path: "blocks/delete-2.blk"},
		"referenced":         {checksum: "referenced", path: "blocks/referenced.blk", refcount: 2},
		"missing":            {checksum: "missing"},
		"referenced-missing": {checksum: "referenced-missing", refcount: 1},
	}

	if err := cleanupBlocks(driver, blocks, volumeName); err != nil {
		t.Fatal(err)
	}

	sort.Strings(driver.exactPaths)
	assert.Equal(t, []string{"blocks/delete-1.blk", "blocks/delete-2.blk"}, driver.exactPaths)
	assert.Equal(t, 1, driver.exactCalls)
	removeCalls, _, writeCalls := driver.calls()
	assert.Empty(t, removeCalls, "the prefix-removing fallback must not be used")
	assert.Equal(t, 1, writeCalls)

	volume, err := loadVolume(driver, volumeName)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, int64(1), volume.BlockCount)
}

func TestCleanupBlocksExactRemovalFailurePreservesBlockCount(t *testing.T) {
	volumeName := "cleanup-volume"
	removeErr := errors.New("exact removal failed")
	driver := &exactCleanupTestDriver{
		cleanupTestDriver: newCleanupTestDriver(t, &Volume{Name: volumeName, BlockCount: 99}),
		exactErr:          removeErr,
	}
	blocks := map[string]*BlockInfo{
		"delete":     {checksum: "delete", path: "blocks/delete.blk"},
		"referenced": {checksum: "referenced", path: "blocks/referenced.blk", refcount: 1},
	}

	err := cleanupBlocks(driver, blocks, volumeName)
	if !errors.Is(err, removeErr) {
		t.Fatalf("expected error %v, got %v", removeErr, err)
	}
	assert.Contains(t, err.Error(), "failed to delete backup blocks")
	assert.Equal(t, []string{"blocks/delete.blk"}, driver.exactPaths)
	removeCalls, _, writeCalls := driver.calls()
	assert.Empty(t, removeCalls)
	assert.Zero(t, writeCalls, "incomplete GC must not update volume metadata")

	volume, loadErr := loadVolume(driver, volumeName)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	assert.Equal(t, int64(99), volume.BlockCount)
}

func TestCleanupBlocksKeepsLegacyRemovalSerialAndAttemptsEveryBlock(t *testing.T) {
	volumeName := "cleanup-volume"
	driver := newCleanupTestDriver(t, &Volume{Name: volumeName, BlockCount: 99})
	driver.removeErrors["blocks/delete-2.blk"] = errors.New("remove failed")
	blocks := map[string]*BlockInfo{
		"delete-1":   {checksum: "delete-1", path: "blocks/delete-1.blk"},
		"delete-2":   {checksum: "delete-2", path: "blocks/delete-2.blk"},
		"delete-3":   {checksum: "delete-3", path: "blocks/delete-3.blk"},
		"referenced": {checksum: "referenced", path: "blocks/referenced.blk", refcount: 1},
	}

	err := cleanupBlocks(driver, blocks, volumeName)
	if err == nil {
		t.Fatal("expected block removal error")
	}
	assert.Contains(t, err.Error(), "delete-2")

	removeCalls, maxActive, writeCalls := driver.calls()
	sort.Strings(removeCalls)
	assert.Equal(t, []string{"blocks/delete-1.blk", "blocks/delete-2.blk", "blocks/delete-3.blk"}, removeCalls)
	assert.Equal(t, 1, maxActive)
	assert.Zero(t, writeCalls, "incomplete GC must not update volume metadata")
}
