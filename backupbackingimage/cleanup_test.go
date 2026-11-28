package backupbackingimage

import (
	"errors"
	"io"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/longhorn/backupstore/common"
)

type cleanupTestDriver struct {
	mu           sync.Mutex
	removeCalls  []string
	removeErrors map[string]error
	active       int
	maxActive    int
}

func (d *cleanupTestDriver) Kind() string              { return "cleanup-test" }
func (d *cleanupTestDriver) GetURL() string            { return "cleanup-test://localhost" }
func (d *cleanupTestDriver) FileExists(string) bool    { return false }
func (d *cleanupTestDriver) FileSize(string) int64     { return -1 }
func (d *cleanupTestDriver) FileTime(string) time.Time { return time.Time{} }
func (d *cleanupTestDriver) Read(string) (io.ReadCloser, error) {
	return nil, errors.New("not implemented")
}
func (d *cleanupTestDriver) Write(string, io.ReadSeeker) error { return nil }
func (d *cleanupTestDriver) List(string) ([]string, error)     { return nil, nil }
func (d *cleanupTestDriver) Upload(string, string) error       { return nil }
func (d *cleanupTestDriver) Download(string, string) error     { return nil }

func (d *cleanupTestDriver) Remove(path string) error {
	d.mu.Lock()
	d.removeCalls = append(d.removeCalls, path)
	d.active++
	if d.active > d.maxActive {
		d.maxActive = d.active
	}
	err := d.removeErrors[path]
	d.mu.Unlock()

	time.Sleep(5 * time.Millisecond)

	d.mu.Lock()
	d.active--
	d.mu.Unlock()
	return err
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
	driver := &exactCleanupTestDriver{cleanupTestDriver: &cleanupTestDriver{removeErrors: map[string]error{}}}
	blocks := map[string]*common.BlockInfo{
		"delete-1":           {Checksum: "delete-1", Path: "blocks/delete-1.blk"},
		"delete-2":           {Checksum: "delete-2", Path: "blocks/delete-2.blk"},
		"referenced":         {Checksum: "referenced", Path: "blocks/referenced.blk", Refcount: 1},
		"missing":            {Checksum: "missing"},
		"referenced-missing": {Checksum: "referenced-missing", Refcount: 1},
	}

	if err := cleanupBlocks(logrus.New().WithField("test", true), driver, blocks); err != nil {
		t.Fatal(err)
	}

	sort.Strings(driver.exactPaths)
	assert.Equal(t, []string{"blocks/delete-1.blk", "blocks/delete-2.blk"}, driver.exactPaths)
	assert.Equal(t, 1, driver.exactCalls)
	assert.Empty(t, driver.removeCalls, "the prefix-removing fallback must not be used")
}

func TestCleanupBlocksExactRemovalFailureIsReturned(t *testing.T) {
	removeErr := errors.New("exact removal failed")
	driver := &exactCleanupTestDriver{
		cleanupTestDriver: &cleanupTestDriver{removeErrors: map[string]error{}},
		exactErr:          removeErr,
	}
	blocks := map[string]*common.BlockInfo{
		"delete":     {Checksum: "delete", Path: "blocks/delete.blk"},
		"referenced": {Checksum: "referenced", Path: "blocks/referenced.blk", Refcount: 1},
	}

	err := cleanupBlocks(logrus.New().WithField("test", true), driver, blocks)
	if !errors.Is(err, removeErr) {
		t.Fatalf("expected error %v, got %v", removeErr, err)
	}
	assert.Contains(t, err.Error(), "failed to delete backing image blocks")
	assert.Equal(t, []string{"blocks/delete.blk"}, driver.exactPaths)
	assert.Empty(t, driver.removeCalls)
}

func TestCleanupBlocksKeepsLegacyRemovalSerial(t *testing.T) {
	driver := &cleanupTestDriver{removeErrors: map[string]error{}}
	blocks := map[string]*common.BlockInfo{
		"delete-1":   {Checksum: "delete-1", Path: "blocks/delete-1.blk"},
		"delete-2":   {Checksum: "delete-2", Path: "blocks/delete-2.blk"},
		"referenced": {Checksum: "referenced", Path: "blocks/referenced.blk", Refcount: 1},
	}

	if err := cleanupBlocks(logrus.New().WithField("test", true), driver, blocks); err != nil {
		t.Fatal(err)
	}

	sort.Strings(driver.removeCalls)
	assert.Equal(t, []string{"blocks/delete-1.blk", "blocks/delete-2.blk"}, driver.removeCalls)
	assert.Equal(t, 1, driver.maxActive)
}
