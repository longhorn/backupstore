package vfs

import (
	"testing"

	"github.com/longhorn/backupstore"
)

// TestBackupStoreDriverImplementsRecursiveLister locks in that vfs opts
// into backupstore.RecursiveLister explicitly (a plain local directory
// makes the single filepath.Walk pass strictly cheaper than the nested
// List() walk it replaces). See fsops.FileSystemOperator.ListRecursiveLocal
// for why nfs/cifs deliberately do NOT get this via embedding.
func TestBackupStoreDriverImplementsRecursiveLister(t *testing.T) {
	var driver interface{} = &BackupStoreDriver{}
	if _, ok := driver.(backupstore.RecursiveLister); !ok {
		t.Fatal("vfs.BackupStoreDriver is expected to implement backupstore.RecursiveLister")
	}
}
