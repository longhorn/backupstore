package nfs

import (
	"testing"

	"github.com/longhorn/backupstore"
)

// TestBackupStoreDriverDoesNotImplementRecursiveLister locks in that nfs
// does NOT satisfy backupstore.RecursiveLister. filepath.Walk over a
// network mount still issues one stat-equivalent remote call per directory
// entry, so promoting fsops.FileSystemOperator's recursive walk here could
// turn one remote round-trip per directory level into one remote
// round-trip per block - worse than the List()-per-directory traversal it
// would replace on backends with very large block counts.
func TestBackupStoreDriverDoesNotImplementRecursiveLister(t *testing.T) {
	var driver interface{} = &BackupStoreDriver{}
	if _, ok := driver.(backupstore.RecursiveLister); ok {
		t.Fatal("nfs.BackupStoreDriver must not implement backupstore.RecursiveLister " +
			"(filepath.Walk over a network mount is not a cheap native recursive listing)")
	}
}
