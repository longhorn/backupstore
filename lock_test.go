package backupstore

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

func TestIsLockConflictError(t *testing.T) {
	lockConflictErr := fmt.Errorf("%v %v when performing backup %v, %v",
		ErrorMessageFailedToAcquireLock,
		"backupstore/volumes/aa/bb/vol-1/locks/lock-1234.lck",
		BackupOperationCreateRestore,
		ErrorMessagePleaseTryAgainLater)

	testCases := []struct {
		name   string
		err    error
		expect bool
	}{
		{
			name:   "nil error",
			err:    nil,
			expect: false,
		},
		{
			name:   "lock conflict",
			err:    lockConflictErr,
			expect: true,
		},
		{
			// The consumers only see the error after the engine and the instance manager
			// have wrapped it, so only the embedded fragments are left to match on.
			name:   "wrapped lock conflict",
			err:    errors.Wrap(lockConflictErr, "error initiating incremental backup restore"),
			expect: true,
		},
		{
			name:   "unrelated error",
			err:    errors.New("cannot find volume config in backupstore"),
			expect: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.expect, IsLockConflictError(testCase.err))
		})
	}
}

// TestLockConflictIsRetryable pins the error that Lock actually produces to
// IsLockConflictError. Rewording that error without updating the shared fragments made the
// consumers treat a transient conflict as a terminal failure before.
func TestLockConflictIsRetryable(t *testing.T) {
	assert := assert.New(t)

	m := &mockStoreDriver{delay: time.Millisecond}
	m.Init()
	defer m.uninstall()

	const volumeName = "vol-1"

	// A held deletion lock has a different type and a higher priority than a restore lock,
	// so the restore lock cannot be acquired.
	heldLock := &FileLock{Name: "lock-0000", Type: DELETION_LOCK, Acquired: true}
	heldLockFile := getLockFilePath(volumeName, heldLock.Name)
	assert.NoError(m.fs.MkdirAll(filepath.Dir(heldLockFile), 0755))
	heldLockData, err := json.Marshal(heldLock)
	assert.NoError(err)
	assert.NoError(afero.WriteFile(m.fs, heldLockFile, heldLockData, 0644))

	lock, err := New(m, volumeName, RESTORE_LOCK)
	assert.NoError(err)

	err = lock.Lock()
	assert.Error(err)
	assert.True(IsLockConflictError(err))
	assert.False(lock.Acquired)
}
