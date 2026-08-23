package backupstore

import (
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/spf13/afero"

	"github.com/stretchr/testify/assert"

	lhbackup "github.com/longhorn/go-common-libs/backup"

	"github.com/longhorn/backupstore/types"
	"github.com/longhorn/backupstore/util"
)

const (
	deltaDriverName = "deltamock"
	deltaDriverURL  = "deltamock://localhost"

	deltaVolumeName   = "pvc-delta-1"
	deltaSnapshotName = "snap-2"
	deltaBlockSize    = int64(4096)
)

// deltaMockStoreDriver is an in-memory BackupStoreDriver. It differs from mockStoreDriver in
// list_inspect_test.go in that Write actually persists: CreateDeltaBlockBackup writes the volume
// config, the lock file and the block files and then reads them back within the same call, so a
// no-op Write would not exercise the code under test.
type deltaMockStoreDriver struct {
	fs afero.Fs
}

func newDeltaMockStoreDriver(t *testing.T) *deltaMockStoreDriver {
	t.Helper()

	m := &deltaMockStoreDriver{fs: afero.NewMemMapFs()}
	if err := RegisterDriver(deltaDriverName, func(destURL string) (BackupStoreDriver, error) {
		return m, nil
	}); err != nil {
		t.Fatalf("failed to register the mock driver: %v", err)
	}
	t.Cleanup(func() {
		_ = unregisterDriver(deltaDriverName)
	})
	return m
}

func (m *deltaMockStoreDriver) Kind() string {
	return deltaDriverName
}

func (m *deltaMockStoreDriver) GetURL() string {
	return deltaDriverURL
}

func (m *deltaMockStoreDriver) FileExists(filePath string) bool {
	exist, err := afero.Exists(m.fs, filePath)
	if err != nil {
		return false
	}
	return exist
}

func (m *deltaMockStoreDriver) FileSize(filePath string) int64 {
	fi, err := m.fs.Stat(filePath)
	if err != nil {
		return -1
	}
	return fi.Size()
}

func (m *deltaMockStoreDriver) FileTime(filePath string) time.Time {
	fi, err := m.fs.Stat(filePath)
	if err != nil {
		return time.Time{}
	}
	return fi.ModTime().UTC()
}

func (m *deltaMockStoreDriver) Remove(path string) error {
	return m.fs.RemoveAll(path)
}

func (m *deltaMockStoreDriver) Read(src string) (io.ReadCloser, error) {
	return m.fs.Open(src)
}

func (m *deltaMockStoreDriver) Write(dst string, rs io.ReadSeeker) error {
	if err := m.fs.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	if _, err := rs.Seek(0, io.SeekStart); err != nil {
		return err
	}
	f, err := m.fs.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()
	_, err = io.Copy(f, rs)
	return err
}

func (m *deltaMockStoreDriver) List(listPath string) ([]string, error) {
	fis, err := afero.ReadDir(m.fs, listPath)
	if err != nil {
		return nil, err
	}

	ret := []string{}
	for _, fi := range fis {
		ret = append(ret, fi.Name())
	}
	return ret, nil
}

func (m *deltaMockStoreDriver) Upload(src, dst string) error {
	return fmt.Errorf("upload is not supported by the mock driver")
}

func (m *deltaMockStoreDriver) Download(src, dst string) error {
	return fmt.Errorf("download is not supported by the mock driver")
}

func (m *deltaMockStoreDriver) seedVolume(t *testing.T, volume *Volume) {
	t.Helper()

	if err := saveVolume(m, volume); err != nil {
		t.Fatalf("failed to seed volume %v: %v", volume.Name, err)
	}
}

func (m *deltaMockStoreDriver) seedBackup(t *testing.T, backup *Backup) {
	t.Helper()

	if err := saveBackup(m, backup); err != nil {
		t.Fatalf("failed to seed backup %v: %v", backup.Name, err)
	}
}

type backupStatus struct {
	snapshotName string
	volumeName   string
	state        string
	progress     int
	url          string
	errMessage   string
}

// mockDeltaOps records what CreateDeltaBlockBackup asks of the volume engine and lets a test
// inject failures at each of the three call sites the function has to recover from.
type mockDeltaOps struct {
	mutex sync.Mutex

	// injected inputs
	localSnapshots map[string]bool
	mappings       *types.Mappings
	openErr        error
	compareErr     error

	// recorded calls
	openCount  int
	closeCount int
	compareIDs []string
	statuses   []backupStatus

	closed     chan struct{}
	closedOnce sync.Once
}

func newMockDeltaOps() *mockDeltaOps {
	return &mockDeltaOps{
		localSnapshots: map[string]bool{deltaSnapshotName: true},
		mappings: &types.Mappings{
			BlockSize: deltaBlockSize,
			Mappings: []types.Mapping{
				{Offset: 0, Size: deltaBlockSize},
				{Offset: 2 * deltaBlockSize, Size: deltaBlockSize},
			},
		},
		compareIDs: []string{},
		statuses:   []backupStatus{},
		closed:     make(chan struct{}),
	}
}

func (ops *mockDeltaOps) HasSnapshot(id, volumeID string) bool {
	ops.mutex.Lock()
	defer ops.mutex.Unlock()
	return ops.localSnapshots[id]
}

func (ops *mockDeltaOps) CompareSnapshot(id, compareID, volumeID string, blockSize int64) (*types.Mappings, error) {
	ops.mutex.Lock()
	ops.compareIDs = append(ops.compareIDs, compareID)
	ops.mutex.Unlock()

	if ops.compareErr != nil {
		return nil, ops.compareErr
	}
	return ops.mappings, nil
}

func (ops *mockDeltaOps) OpenSnapshot(id, volumeID string) error {
	if ops.openErr != nil {
		return ops.openErr
	}

	ops.mutex.Lock()
	defer ops.mutex.Unlock()
	ops.openCount++
	return nil
}

func (ops *mockDeltaOps) ReadSnapshot(id, volumeID string, start int64, data []byte) error {
	// Give every offset distinct content so that each block gets its own checksum, otherwise
	// the backup would deduplicate them into a single block file.
	for i := range data {
		data[i] = byte(start/deltaBlockSize) + 1
	}
	return nil
}

func (ops *mockDeltaOps) CloseSnapshot(id, volumeID string) error {
	ops.mutex.Lock()
	ops.closeCount++
	ops.mutex.Unlock()

	ops.closedOnce.Do(func() { close(ops.closed) })
	return nil
}

func (ops *mockDeltaOps) UpdateBackupStatus(id, volumeID string, backupState string, backupProgress int, backupURL string, errMessage string) error {
	ops.mutex.Lock()
	defer ops.mutex.Unlock()

	ops.statuses = append(ops.statuses, backupStatus{
		snapshotName: id,
		volumeName:   volumeID,
		state:        backupState,
		progress:     backupProgress,
		url:          backupURL,
		errMessage:   errMessage,
	})
	return nil
}

// waitForSnapshotClosed blocks until the snapshot is closed. Closing the snapshot is the very
// last thing the asynchronous backup goroutine does, so this also means every status update has
// already been recorded.
func (ops *mockDeltaOps) waitForSnapshotClosed(t *testing.T) {
	t.Helper()

	select {
	case <-ops.closed:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for the backup goroutine to close the snapshot")
	}
}

func (ops *mockDeltaOps) getCloseCount() int {
	ops.mutex.Lock()
	defer ops.mutex.Unlock()
	return ops.closeCount
}

func (ops *mockDeltaOps) getOpenCount() int {
	ops.mutex.Lock()
	defer ops.mutex.Unlock()
	return ops.openCount
}

func (ops *mockDeltaOps) getCompareIDs() []string {
	ops.mutex.Lock()
	defer ops.mutex.Unlock()
	return append([]string{}, ops.compareIDs...)
}

func (ops *mockDeltaOps) getStatuses() []backupStatus {
	ops.mutex.Lock()
	defer ops.mutex.Unlock()
	return append([]backupStatus{}, ops.statuses...)
}

func (ops *mockDeltaOps) getLastStatus(t *testing.T) backupStatus {
	t.Helper()

	statuses := ops.getStatuses()
	if len(statuses) == 0 {
		t.Fatal("expected at least one backup status update, got none")
	}
	return statuses[len(statuses)-1]
}

func newDeltaBackupConfig(ops *mockDeltaOps) *DeltaBackupConfig {
	return &DeltaBackupConfig{
		Volume: &Volume{
			Name:              deltaVolumeName,
			Size:              4 * deltaBlockSize,
			CompressionMethod: LEGACY_COMPRESSION_METHOD,
			DataEngine:        string(DataEngineV1),
		},
		Snapshot: &Snapshot{
			Name:        deltaSnapshotName,
			CreatedTime: "2026-08-20T00:00:00Z",
		},
		DestURL:         deltaDriverURL,
		DeltaOps:        ops,
		ConcurrentLimit: 1,
		Parameters: map[string]string{
			lhbackup.LonghornBackupParameterBackupBlockSize: fmt.Sprint(deltaBlockSize),
		},
	}
}

func TestCreateDeltaBlockBackupRejectsInvalidConfig(t *testing.T) {
	testCases := map[string]struct {
		newConfig func(ops *mockDeltaOps) *DeltaBackupConfig

		expectErrorContains string
		// A caller that never gets as far as knowing the block size cannot have its failure
		// reported through the volume engine, so the backup status must stay untouched. Only
		// once the config is usable does a failure have to be reported back as an error state.
		expectStatusUpdate bool
	}{
		"nil config": {
			newConfig: func(ops *mockDeltaOps) *DeltaBackupConfig {
				return nil
			},
			expectErrorContains: "invalid empty config for backup",
		},
		"missing delta block backup operations": {
			newConfig: func(ops *mockDeltaOps) *DeltaBackupConfig {
				config := newDeltaBackupConfig(ops)
				config.DeltaOps = nil
				return config
			},
			expectErrorContains: "missing DeltaBlockBackupOperations",
		},
		"unparsable backup block size": {
			newConfig: func(ops *mockDeltaOps) *DeltaBackupConfig {
				config := newDeltaBackupConfig(ops)
				config.Parameters[lhbackup.LonghornBackupParameterBackupBlockSize] = "not-a-size"
				return config
			},
			expectErrorContains: "invalid block size not-a-size",
		},
		"unsupported destination URL": {
			newConfig: func(ops *mockDeltaOps) *DeltaBackupConfig {
				config := newDeltaBackupConfig(ops)
				config.DestURL = "nosuchdriver://localhost"
				return config
			},
			expectErrorContains: "driver nosuchdriver is not supported",
			expectStatusUpdate:  true,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			assert := assert.New(t)

			ops := newMockDeltaOps()

			isIncremental, err := CreateDeltaBlockBackup("backup-1", testCase.newConfig(ops))
			assert.Error(err)
			assert.Contains(err.Error(), testCase.expectErrorContains)
			assert.False(isIncremental)

			// The snapshot must never be opened when the config is rejected, otherwise it
			// would stay open forever.
			assert.Equal(0, ops.getOpenCount())

			statuses := ops.getStatuses()
			if !testCase.expectStatusUpdate {
				assert.Empty(statuses)
				return
			}
			assert.Len(statuses, 1)
			assert.Equal(string(types.ProgressStateError), statuses[0].state)
			assert.Equal(deltaVolumeName, statuses[0].volumeName)
			assert.Equal(deltaSnapshotName, statuses[0].snapshotName)
			assert.Contains(statuses[0].errMessage, testCase.expectErrorContains)
		})
	}
}

func TestCreateDeltaBlockBackupCreatesFullBackup(t *testing.T) {
	assert := assert.New(t)

	m := newDeltaMockStoreDriver(t)
	ops := newMockDeltaOps()

	isIncremental, err := CreateDeltaBlockBackup("backup-1", newDeltaBackupConfig(ops))
	assert.NoError(err)
	// A volume that the backupstore has never seen has no last backup to diff against.
	assert.False(isIncremental)

	ops.waitForSnapshotClosed(t)

	// An empty compare ID is what makes the volume engine hand over every allocated block
	// instead of only the changed ones.
	assert.Equal([]string{""}, ops.getCompareIDs())
	// The snapshot has to be released once the backup is done, otherwise the next recurring
	// job cannot purge it.
	assert.Equal(1, ops.getOpenCount())
	assert.Equal(1, ops.getCloseCount())

	// The caller learns where the backup landed only through the final status update.
	lastStatus := ops.getLastStatus(t)
	assert.Equal(EncodeBackupURL("backup-1", deltaVolumeName, deltaDriverURL), lastStatus.url)
	assert.Empty(lastStatus.errMessage)

	backup, err := loadBackup(m, "backup-1", deltaVolumeName)
	assert.NoError(err)
	assert.False(backup.IsIncremental)
	assert.Equal(deltaSnapshotName, backup.SnapshotName)
	assert.Len(backup.Blocks, 2)
	assert.Equal(2*deltaBlockSize, backup.Size)

	// The volume config is what a later backup reads to decide whether it can be incremental.
	volume, err := loadVolume(m, deltaVolumeName)
	assert.NoError(err)
	assert.Equal("backup-1", volume.LastBackupName)
	assert.Equal(int64(2), volume.BlockCount)
}

func TestCreateDeltaBlockBackupCreatesIncrementalBackup(t *testing.T) {
	assert := assert.New(t)

	m := newDeltaMockStoreDriver(t)
	ops := newMockDeltaOps()
	ops.localSnapshots["snap-1"] = true
	// Only the third block changed since snap-1.
	ops.mappings = &types.Mappings{
		BlockSize: deltaBlockSize,
		Mappings:  []types.Mapping{{Offset: 2 * deltaBlockSize, Size: deltaBlockSize}},
	}

	lastBlockChecksum := util.GetChecksum([]byte("block at offset 0"))
	m.seedVolume(t, &Volume{
		Name:              deltaVolumeName,
		Size:              4 * deltaBlockSize,
		CompressionMethod: LEGACY_COMPRESSION_METHOD,
		LastBackupName:    "backup-1",
		BlockCount:        1,
	})
	m.seedBackup(t, &Backup{
		Name:              "backup-1",
		VolumeName:        deltaVolumeName,
		SnapshotName:      "snap-1",
		CreatedTime:       "2026-08-19T00:00:00Z",
		CompressionMethod: LEGACY_COMPRESSION_METHOD,
		Blocks:            []BlockMapping{{Offset: 0, BlockChecksum: lastBlockChecksum}},
	})

	isIncremental, err := CreateDeltaBlockBackup("backup-2", newDeltaBackupConfig(ops))
	assert.NoError(err)
	assert.True(isIncremental)

	ops.waitForSnapshotClosed(t)

	// The last backup's snapshot is the baseline the volume engine diffs against.
	assert.Equal([]string{"snap-1"}, ops.getCompareIDs())

	backup, err := loadBackup(m, "backup-2", deltaVolumeName)
	assert.NoError(err)
	assert.True(backup.IsIncremental)
	// The unchanged block is not re-uploaded but it still has to appear in the new backup,
	// otherwise restoring backup-2 would produce a hole at offset 0.
	assert.Equal([]BlockMapping{
		{Offset: 0, BlockChecksum: lastBlockChecksum},
		{Offset: 2 * deltaBlockSize, BlockChecksum: backup.Blocks[1].BlockChecksum},
	}, backup.Blocks)

	volume, err := loadVolume(m, deltaVolumeName)
	assert.NoError(err)
	assert.Equal("backup-2", volume.LastBackupName)
	// Only the changed block was newly uploaded.
	assert.Equal(int64(2), volume.BlockCount)
}

func TestCreateDeltaBlockBackupFallsBackToFullBackup(t *testing.T) {
	testCases := map[string]struct {
		lastBackup *Backup
		// snapshots the volume engine still holds locally
		localSnapshots []string
		fullBackupMode bool
	}{
		// The metadata is gone from the backupstore, so there is nothing to diff against.
		"last backup config is missing": {
			lastBackup:     nil,
			localSnapshots: []string{deltaSnapshotName, "snap-1"},
		},
		// Diffing a snapshot against itself would produce an empty backup.
		"last backup was taken from the same snapshot": {
			lastBackup: &Backup{
				Name: "backup-1", VolumeName: deltaVolumeName, SnapshotName: deltaSnapshotName,
				CreatedTime: "2026-08-19T00:00:00Z", CompressionMethod: LEGACY_COMPRESSION_METHOD,
			},
			localSnapshots: []string{deltaSnapshotName},
		},
		// Without the baseline snapshot on the node the engine cannot compute a delta.
		"last snapshot no longer exists locally": {
			lastBackup: &Backup{
				Name: "backup-1", VolumeName: deltaVolumeName, SnapshotName: "snap-1",
				CreatedTime: "2026-08-19T00:00:00Z", CompressionMethod: LEGACY_COMPRESSION_METHOD,
			},
			localSnapshots: []string{deltaSnapshotName},
		},
		// The user explicitly asked for a full backup.
		"full backup mode is requested": {
			lastBackup: &Backup{
				Name: "backup-1", VolumeName: deltaVolumeName, SnapshotName: "snap-1",
				CreatedTime: "2026-08-19T00:00:00Z", CompressionMethod: LEGACY_COMPRESSION_METHOD,
			},
			localSnapshots: []string{deltaSnapshotName, "snap-1"},
			fullBackupMode: true,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			assert := assert.New(t)

			m := newDeltaMockStoreDriver(t)
			ops := newMockDeltaOps()
			ops.localSnapshots = map[string]bool{}
			for _, snapshot := range testCase.localSnapshots {
				ops.localSnapshots[snapshot] = true
			}

			m.seedVolume(t, &Volume{
				Name:              deltaVolumeName,
				Size:              4 * deltaBlockSize,
				CompressionMethod: LEGACY_COMPRESSION_METHOD,
				LastBackupName:    "backup-1",
			})
			if testCase.lastBackup != nil {
				m.seedBackup(t, testCase.lastBackup)
			}

			config := newDeltaBackupConfig(ops)
			if testCase.fullBackupMode {
				config.Parameters[lhbackup.LonghornBackupParameterBackupMode] = string(lhbackup.LonghornBackupModeFull)
			}

			isIncremental, err := CreateDeltaBlockBackup("backup-2", config)
			assert.NoError(err)
			assert.False(isIncremental)

			ops.waitForSnapshotClosed(t)

			// Falling back means diffing against nothing, not failing the backup.
			assert.Equal([]string{""}, ops.getCompareIDs())

			backup, err := loadBackup(m, "backup-2", deltaVolumeName)
			assert.NoError(err)
			assert.False(backup.IsIncremental)
			assert.Len(backup.Blocks, 2)
		})
	}
}

func TestCreateDeltaBlockBackupReportsOpenSnapshotFailure(t *testing.T) {
	assert := assert.New(t)

	newDeltaMockStoreDriver(t)
	ops := newMockDeltaOps()
	ops.openErr = fmt.Errorf("engine is not running")

	isIncremental, err := CreateDeltaBlockBackup("backup-1", newDeltaBackupConfig(ops))
	assert.Error(err)
	assert.Contains(err.Error(), "engine is not running")
	assert.False(isIncremental)

	// Nothing was opened, so nothing may be closed.
	assert.Equal(0, ops.getCloseCount())
	assert.Empty(ops.getCompareIDs())

	lastStatus := ops.getLastStatus(t)
	assert.Equal(string(types.ProgressStateError), lastStatus.state)
	assert.Contains(lastStatus.errMessage, "engine is not running")
}

func TestCreateDeltaBlockBackupClosesSnapshotOnCompareFailure(t *testing.T) {
	assert := assert.New(t)

	newDeltaMockStoreDriver(t)
	ops := newMockDeltaOps()
	ops.compareErr = fmt.Errorf("failed to compare snapshots")

	isIncremental, err := CreateDeltaBlockBackup("backup-1", newDeltaBackupConfig(ops))
	assert.Error(err)
	assert.Contains(err.Error(), "failed to compare snapshots")
	assert.False(isIncremental)

	// The snapshot was opened before the comparison, so a failed comparison must release it
	// instead of leaving it open for the lifetime of the process.
	assert.Equal(1, ops.getOpenCount())
	assert.Equal(1, ops.getCloseCount())

	lastStatus := ops.getLastStatus(t)
	assert.Equal(string(types.ProgressStateError), lastStatus.state)
	assert.Contains(lastStatus.errMessage, "failed to compare snapshots")
}

func TestCreateDeltaBlockBackupReportsLoadVolumeFailure(t *testing.T) {
	assert := assert.New(t)

	m := newDeltaMockStoreDriver(t)
	ops := newMockDeltaOps()

	// A corrupt volume config makes addVolume a no-op (the file exists) and loadVolume fail.
	if err := afero.WriteFile(m.fs, getVolumeFilePath(deltaVolumeName), []byte("{not json"), 0644); err != nil {
		t.Fatalf("failed to seed a corrupt volume config: %v", err)
	}

	isIncremental, err := CreateDeltaBlockBackup("backup-1", newDeltaBackupConfig(ops))
	assert.Error(err)
	assert.False(isIncremental)
	assert.Equal(0, ops.getOpenCount())

	// The deferred status update dereferences the volume, so a failed load must not replace
	// the volume of the config with the nil that loadVolume returns alongside the error.
	lastStatus := ops.getLastStatus(t)
	assert.Equal(deltaVolumeName, lastStatus.volumeName)
	assert.Equal(deltaSnapshotName, lastStatus.snapshotName)
	assert.Equal(string(types.ProgressStateError), lastStatus.state)
}
