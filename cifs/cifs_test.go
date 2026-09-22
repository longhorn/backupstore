package cifs

import (
	"testing"

	"github.com/longhorn/backupstore"
)

// TestBackupStoreDriverDoesNotImplementRecursiveLister locks in that cifs
// does NOT satisfy backupstore.RecursiveLister, for the same reason as nfs:
// filepath.Walk over a network mount is not a cheap native recursive
// listing (one remote round-trip per directory entry, not per directory).
func TestBackupStoreDriverDoesNotImplementRecursiveLister(t *testing.T) {
	var driver interface{} = &BackupStoreDriver{}
	if _, ok := driver.(backupstore.RecursiveLister); ok {
		t.Fatal("cifs.BackupStoreDriver must not implement backupstore.RecursiveLister " +
			"(filepath.Walk over a network mount is not a cheap native recursive listing)")
	}
}
