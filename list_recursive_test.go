package backupstore

import (
	"errors"
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
// file found, relative to path. A missing path is not an error - it mirrors
// the real s3 and fsops ListRecursive implementations, which both return
// (nil, nil) for a prefix/directory that doesn't exist (see List()'s
// existing "No such file or directory" tolerance in fsops, and S3's
// ListObjectsV2 simply returning zero objects for a non-matching prefix).
// Any other walk error is a genuine failure and must propagate.
func (m *mockRecursiveStoreDriver) ListRecursive(path string) ([]string, error) {
	defer time.Sleep(m.delay)

	if exists, _ := afero.DirExists(m.fs, path); !exists {
		return nil, nil
	}

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

// mockFailingRecursiveStoreDriver always returns a genuine error from
// ListRecursive, simulating an auth/network/pagination failure rather than
// a missing directory.
type mockFailingRecursiveStoreDriver struct {
	mockRecursiveStoreDriver
}

var errSimulatedListFailure = errors.New("simulated recursive listing failure")

func (m *mockFailingRecursiveStoreDriver) ListRecursive(path string) ([]string, error) {
	return nil, errSimulatedListFailure
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
		// Explicitly create parent directories rather than relying on
		// afero.MemMapFs auto-vivifying them on WriteFile - that's an
		// implementation detail of this particular in-memory fake, not
		// something afero.WriteFile itself guarantees.
		assert.NoError(m.fs.MkdirAll(filepath.Dir(b), 0755))
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

// TestGetBlockNamesForVolumePropagatesRecursiveListError verifies that a
// genuine ListRecursive failure (auth, network, pagination, etc.) is
// propagated to the caller rather than silently treated as "no blocks".
// Swallowing such errors would let GC continue with an empty block map and
// persist BlockCount = 0 for a volume whose blocks simply failed to list.
func TestGetBlockNamesForVolumePropagatesRecursiveListError(t *testing.T) {
	assert := assert.New(t)

	m := &mockFailingRecursiveStoreDriver{}
	m.Init()
	defer m.uninstall()

	names, err := getBlockNamesForVolume(m, "test-vol")
	assert.ErrorIs(err, errSimulatedListFailure)
	assert.Nil(names)
}
