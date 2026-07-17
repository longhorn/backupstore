package fsops

import (
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/backupstore/util"
	"github.com/sirupsen/logrus"
)

const (
	MaxCleanupLevel = 10
)

type FileSystemOps interface {
	LocalPath(path string) string
}

type FileSystemOperator struct {
	FileSystemOps
}

func NewFileSystemOperator(ops FileSystemOps) *FileSystemOperator {
	return &FileSystemOperator{ops}
}

func (f *FileSystemOperator) preparePath(file string) error {
	return os.MkdirAll(filepath.Dir(f.LocalPath(file)), os.ModeDir|0700)
}

func (f *FileSystemOperator) FileSize(filePath string) int64 {
	file := f.LocalPath(filePath)
	st, err := os.Stat(file)
	if err != nil || st.IsDir() {
		return -1
	}
	return st.Size()
}

func (f *FileSystemOperator) FileTime(filePath string) time.Time {
	file := f.LocalPath(filePath)
	st, err := os.Stat(file)
	if err != nil || st.IsDir() {
		return time.Time{}
	}

	return st.ModTime().UTC()
}

func (f *FileSystemOperator) FileExists(filePath string) bool {
	return f.FileSize(filePath) >= 0
}

func (f *FileSystemOperator) Remove(path string) error {
	if err := os.RemoveAll(f.LocalPath(path)); err != nil {
		return err
	}
	//Also automatically cleanup upper level directories
	dir := f.LocalPath(path)
	for i := 0; i < MaxCleanupLevel; i++ {
		dir = filepath.Dir(dir)
		// Don't clean above backupstore base
		if strings.HasSuffix(dir, backupstore.GetBackupstoreBase()) {
			break
		}
		// If directory is not empty, then we don't need to continue
		if err := os.Remove(dir); err != nil {
			break
		}
	}
	return nil
}

func (f *FileSystemOperator) Read(src string) (io.ReadCloser, error) {
	file, err := os.Open(f.LocalPath(src))
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (f *FileSystemOperator) Write(dst string, rs io.ReadSeeker) error {
	// we append the timestamp to the tmp files so that we should never have 2 backups using the same tmp file
	tmpFile := dst + ".tmp" + "." + strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	if err := f.preparePath(dst); err != nil {
		return err
	}
	file, err := os.Create(f.LocalPath(tmpFile))
	if err != nil {
		return err
	}

	_, err = io.Copy(file, rs)
	if err != nil {
		_ = file.Close()
		return err
	}

	// we close the file here to force nfs to sync the data to stable storage
	err = file.Close()
	if err != nil {
		return err
	}

	return os.Rename(f.LocalPath(tmpFile), f.LocalPath(dst))
}

func (f *FileSystemOperator) List(path string) ([]string, error) {
	out, err := util.Execute("ls", []string{"-1", f.LocalPath(path)})
	if err != nil &&
		!strings.Contains(err.Error(), "No such file or directory") &&
		!strings.Contains(err.Error(), "cannot open directory") {
		return nil, err
	}
	var result []string
	if len(out) == 0 {
		return result, nil
	}
	result = strings.Split(strings.TrimSpace(string(out)), "\n")
	return result, nil
}

// ListRecursive implements backupstore.RecursiveLister for filesystem-backed
// drivers (nfs, cifs, vfs). It walks the local mount in a single process
// instead of issuing a separate remote List() round-trip per directory
// level, which matters for network filesystems where each List() is a
// remote call.
func (f *FileSystemOperator) ListRecursive(path string) ([]string, error) {
	base := f.LocalPath(path)
	var result []string

	err := filepath.Walk(base, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			// Missing directory is not an error for callers, mirroring List()'s
			// "No such file or directory" tolerance above.
			if os.IsNotExist(err) {
				return filepath.SkipDir
			}
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, relErr := filepath.Rel(base, p)
		if relErr != nil {
			return relErr
		}
		result = append(result, rel)
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	return result, nil
}

func (f *FileSystemOperator) Upload(src, dst string) error {
	tmpDst := dst + ".tmp" + "." + strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	if f.FileExists(tmpDst) {
		if err := f.Remove(tmpDst); err != nil {
			logrus.WithError(err).Warnf("Failed to remove tmp file %s", tmpDst)
		}
	}
	if err := f.preparePath(dst); err != nil {
		return err
	}
	_, err := util.Execute("cp", []string{src, f.LocalPath(tmpDst)})
	if err != nil {
		return err
	}
	_, err = util.Execute("mv", []string{f.LocalPath(tmpDst), f.LocalPath(dst)})
	return err
}

func (f *FileSystemOperator) Download(src, dst string) error {
	_, err := util.Execute("cp", []string{f.LocalPath(src), dst})
	return err
}
