package backupstore

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

type recursiveMockStoreDriver struct {
	*mockStoreDriver
	listCalls          int
	listRecursiveCalls int
}

func (m *recursiveMockStoreDriver) List(listPath string) ([]string, error) {
	m.listCalls++
	return m.mockStoreDriver.List(listPath)
}

// ListRecursive returns no error for a missing path, like an S3 prefix with no objects.
func (m *recursiveMockStoreDriver) ListRecursive(listPath string) ([]string, error) {
	m.listRecursiveCalls++
	exists, err := afero.DirExists(m.fs, listPath)
	if err != nil || !exists {
		return nil, err
	}

	var files []string
	err = afero.Walk(m.fs, listPath, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		rel, err := filepath.Rel(listPath, path)
		if err != nil {
			return err
		}
		files = append(files, filepath.ToSlash(rel))
		return nil
	})
	return files, err
}

func TestGetBlockNamesForVolumeRecursiveMatchesWalk(t *testing.T) {
	assert := assert.New(t)

	walker := &mockStoreDriver{fs: afero.NewMemMapFs()}
	lister := &recursiveMockStoreDriver{mockStoreDriver: walker}

	checksums := []string{
		"0a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f9",
		"0a1bffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
		"9f8e7d6c5b4a39281706f5e4d3c2b1a09f8e7d6c5b4a39281706f5e4d3c2b1a0",
	}
	for _, c := range checksums {
		assert.NoError(afero.WriteFile(walker.fs, getBlockFilePath("pvc-1", c), []byte("x"), 0644))
	}
	blockPath := getBlockPath("pvc-1")
	assert.NoError(afero.WriteFile(walker.fs, filepath.Join(blockPath, "0a", "1b", "not-a-block.txt"), []byte("x"), 0644))
	assert.NoError(afero.WriteFile(walker.fs, filepath.Join(blockPath, "0a", "1b", "cc", "too-deep.blk"), []byte("x"), 0644))

	walked, err := getBlockNamesForVolume(walker, "pvc-1")
	assert.NoError(err)
	assert.ElementsMatch(checksums, walked)

	listed, err := getBlockNamesForVolume(lister, "pvc-1")
	assert.NoError(err)
	assert.ElementsMatch(walked, listed)
	assert.Equal(1, lister.listRecursiveCalls)
	assert.Equal(0, lister.listCalls)

	// The walk of the afero mock fails on a file at the first level, so only the recursive path checks it.
	assert.NoError(afero.WriteFile(walker.fs, filepath.Join(blockPath, "top-level.blk"), []byte("x"), 0644))
	listed, err = getBlockNamesForVolume(lister, "pvc-1")
	assert.NoError(err)
	assert.ElementsMatch(checksums, listed)
}

func TestGetBlockNamesForVolumeRecursiveWithoutBlocks(t *testing.T) {
	assert := assert.New(t)

	lister := &recursiveMockStoreDriver{mockStoreDriver: &mockStoreDriver{fs: afero.NewMemMapFs()}}

	names, err := getBlockNamesForVolume(lister, "pvc-1")
	assert.NoError(err)
	assert.Empty(names)
}
