package backupstore

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

const (
	mockRecursiveDriverName = "mockrecursive"
	mockRecursiveDriverURL  = "mockrecursive://localhost"
)

// mockRecursiveStoreDriver reuses mockStoreDriver's List() semantics but
// additionally implements RecursiveLister, so getBlockNamesForVolume can
// exercise the new "single call" code path against an in-memory filesystem.
// This mirrors how the s3 driver's ListRecursive walks every key under a
// prefix in one paginated request sequence instead of directory-by-directory.
type mockRecursiveStoreDriver struct {
	mockStoreDriver
}

func (m *mockRecursiveStoreDriver) Init() {
	m.fs = afero.NewMemMapFs()
	m.destURL = mockRecursiveDriverURL

	RegisterDriver(mockRecursiveDriverName, func(destURL string) (BackupStoreDriver, error) { // nolint:errcheck
		m.fs.MkdirAll(filepath.Join(backupstoreBase, VOLUME_DIRECTORY), 0755) // nolint:errcheck
		return m, nil
	})
}

func (m *mockRecursiveStoreDriver) uninstall() {
	m.fs.RemoveAll("/")                       // nolint:errcheck
	unregisterDriver(mockRecursiveDriverName) // nolint:errcheck
}

func (m *mockRecursiveStoreDriver) Kind() string {
	return mockRecursiveDriverName
}

// ListRecursive walks the whole in-memory tree under path and returns every
// file found, relative to path.
func (m *mockRecursiveStoreDriver) ListRecursive(path string) ([]string, error) {
	defer time.Sleep(m.delay)

	var result []string
	err := afero.Walk(m.fs, path, func(p string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			return nil
		}
		rel, relErr := filepath.Rel(path, p)
		if relErr != nil {
			return relErr
		}
		result = append(result, rel)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func TestGetBlockNamesForVolumeUsesRecursiveListing(t *testing.T) {
	assert := assert.New(t)

	m := &mockRecursiveStoreDriver{}
	m.Init()
	defer m.uninstall()

	volumeName := "test-vol"
	blockPathBase := getBlockPath(volumeName)

	blocks := []string{
		filepath.Join(blockPathBase, "aa", "bb", "block1.blk"),
		filepath.Join(blockPathBase, "aa", "cc", "block2.blk"),
		filepath.Join(blockPathBase, "dd", "ee", "block3.blk"),
	}
	for _, b := range blocks {
		assert.NoError(afero.WriteFile(m.fs, b, []byte("data"), 0644))
	}

	names, err := getBlockNamesForVolume(m, volumeName)
	assert.NoError(err)

	expected := []string{"block1", "block2", "block3"}
	sort.Strings(names)
	sort.Strings(expected)
	assert.Equal(expected, names)
}

func TestGetBlockNamesForVolumeEmptyDirectory(t *testing.T) {
	assert := assert.New(t)

	m := &mockRecursiveStoreDriver{}
	m.Init()
	defer m.uninstall()

	names, err := getBlockNamesForVolume(m, "nonexistent-vol")
	assert.NoError(err)
	assert.Empty(names)
}
