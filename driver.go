package backupstore

import (
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/sirupsen/logrus"

	. "github.com/longhorn/backupstore/logging" // nolint: staticcheck
)

type InitFunc func(destURL string) (BackupStoreDriver, error)

type BackupStoreDriver interface {
	Kind() string
	GetURL() string
	FileExists(filePath string) bool
	FileSize(filePath string) int64
	FileTime(filePath string) time.Time     // Needs to be returned in UTC
	Remove(path string) error               // Behavior like "rm -rf"
	Read(src string) (io.ReadCloser, error) // Caller needs to close
	Write(dst string, rs io.ReadSeeker) error
	List(path string) ([]string, error) // Behavior like "ls", not like "find"
	Upload(src, dst string) error
	Download(src, dst string) error
}

// BatchRemover is an optional extension of BackupStoreDriver for backends that
// can delete many already-known keys in one request. S3's DeleteObjects accepts
// up to 1000 keys per call, so a caller that already holds the complete key
// list (block GC after a flat listing, for example) can replace one request per
// block with one request per 1000 blocks.
//
// This matters on backends that bill per API request (Backblaze B2 Class C
// transactions, AWS S3 Tier 1 requests). See
// https://github.com/longhorn/longhorn/issues/14510 and the GC term described
// in https://github.com/longhorn/longhorn/issues/1547: before this, removing a
// single known block cost a ListObjects of its prefix followed by a
// DeleteObject, i.e. two billable requests per block.
//
// Drivers are not required to implement it; callers must fall back to Remove()
// per path when the driver does not.
type BatchRemover interface {
	// RemoveBatch deletes the given paths. A path ending in "/" is treated as a
	// prefix like Remove() does; any other path is treated as a known key and
	// deleted without listing first. Keys are batched, up to
	// MaxBatchDeleteKeys per request.
	RemoveBatch(paths []string) error
}

// MaxBatchDeleteKeys is the number of keys a single S3 DeleteObjects request
// accepts.
const MaxBatchDeleteKeys = 1000

var (
	initializers map[string]InitFunc
)

var (
	log = logrus.WithFields(logrus.Fields{"pkg": "backupstore"})
)

func GetLog() logrus.FieldLogger {
	return log
}

func generateError(fields logrus.Fields, format string, v ...interface{}) error {
	return ErrorWithFields("backupstore", fields, format, v...)
}

func init() {
	initializers = make(map[string]InitFunc)
}

func RegisterDriver(kind string, initFunc InitFunc) error {
	if _, exists := initializers[kind]; exists {
		return fmt.Errorf("%s has already been registered", kind)
	}
	initializers[kind] = initFunc
	return nil
}

func unregisterDriver(kind string) error {
	if _, exists := initializers[kind]; !exists {
		return fmt.Errorf("%s has not been registered", kind)
	}
	delete(initializers, kind)
	return nil
}

func GetBackupStoreDriver(destURL string) (BackupStoreDriver, error) {
	if destURL == "" {
		return nil, fmt.Errorf("destination URL hasn't been specified")
	}
	u, err := url.Parse(destURL)
	if err != nil {
		return nil, err
	}
	if _, exists := initializers[u.Scheme]; !exists {
		return nil, fmt.Errorf("driver %v is not supported", u.Scheme)
	}
	return initializers[u.Scheme](destURL)
}
