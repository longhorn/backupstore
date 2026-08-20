package backupstore

import (
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"

	"github.com/longhorn/backupstore/types"
)

const (
	panicDriverName = "missingcfg"
	panicDriverURL  = "missingcfg://localhost"
)

// missingCfgDriver simulates an S3-compatible backend (e.g. Backblaze B2) that
// reports a volume's volume.cfg as existing but fails to read it with an error
// such as "AWS Error: InvalidRequest not found".
type missingCfgDriver struct {
	fs afero.Fs
}

func (m *missingCfgDriver) Init() {
	m.fs = afero.NewMemMapFs()
	RegisterDriver(panicDriverName, func(destURL string) (BackupStoreDriver, error) { // nolint:errcheck
		return m, nil
	})
}

func (m *missingCfgDriver) uninstall() {
	m.fs.RemoveAll("/")             // nolint:errcheck
	unregisterDriver(panicDriverName) // nolint:errcheck
}

func (m *missingCfgDriver) Kind() string   { return panicDriverName }
func (m *missingCfgDriver) GetURL() string { return panicDriverURL }

func (m *missingCfgDriver) List(listPath string) ([]string, error) {
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

// FileExists reports the volume.cfg as existing so that addVolume treats the
// volume as already present and does not recreate it.
func (m *missingCfgDriver) FileExists(filePath string) bool {
	if strings.HasSuffix(filePath, VOLUME_CONFIG_FILE) {
		return true
	}
	exist, err := afero.Exists(m.fs, filePath)
	if err != nil {
		return false
	}
	return exist
}

func (m *missingCfgDriver) FileSize(filePath string) int64 {
	fi, err := m.fs.Stat(filePath)
	if err != nil {
		return -1
	}
	return fi.Size()
}

func (m *missingCfgDriver) FileTime(filePath string) time.Time {
	fi, err := m.fs.Stat(filePath)
	if err != nil {
		return time.Now()
	}
	return fi.ModTime()
}

func (m *missingCfgDriver) Remove(path string) error { return m.fs.Remove(path) }

// Read fails for the volume.cfg, mimicking "InvalidRequest not found".
func (m *missingCfgDriver) Read(src string) (io.ReadCloser, error) {
	if strings.HasSuffix(src, VOLUME_CONFIG_FILE) {
		return nil, fmt.Errorf("failed to get object: %s: AWS Error: InvalidRequest not found", src)
	}
	return m.fs.Open(src)
}

func (m *missingCfgDriver) Write(dst string, rs io.ReadSeeker) error { return nil }
func (m *missingCfgDriver) Upload(src, dst string) error            { return nil }
func (m *missingCfgDriver) Download(src, dst string) error          { return nil }

// recordingDeltaOps records the last backup status update.
type recordingDeltaOps struct {
	lastState string
	lastError string
}

func (r *recordingDeltaOps) HasSnapshot(id, volumeID string) bool { return false }
func (r *recordingDeltaOps) CompareSnapshot(id, compareID, volumeID string, blockSize int64) (*types.Mappings, error) {
	return nil, nil
}
func (r *recordingDeltaOps) OpenSnapshot(id, volumeID string) error  { return nil }
func (r *recordingDeltaOps) ReadSnapshot(id, volumeID string, start int64, data []byte) error {
	return nil
}
func (r *recordingDeltaOps) CloseSnapshot(id, volumeID string) error { return nil }
func (r *recordingDeltaOps) UpdateBackupStatus(id, volumeID string, backupState string, backupProgress int, backupURL string, err string) error {
	r.lastState = backupState
	r.lastError = err
	return nil
}

// TestCreateDeltaBlockBackupMissingVolumeCfg verifies that a backup against a
// volume whose volume.cfg cannot be read returns a normal error and updates the
// backup status to error, instead of panicking in the deferred status-update
// path. See longhorn/longhorn#13790.
func TestCreateDeltaBlockBackupMissingVolumeCfg(t *testing.T) {
	assert := assert.New(t)

	m := &missingCfgDriver{}
	m.Init()
	defer m.uninstall()

	deltaOps := &recordingDeltaOps{}
	config := &DeltaBackupConfig{
		Volume:   &Volume{Name: "pvc-1"},
		Snapshot: &Snapshot{Name: "snap-1"},
		DestURL:  panicDriverURL,
		DeltaOps: deltaOps,
	}

	assert.NotPanics(func() {
		_, err := CreateDeltaBlockBackup("backup-1", config)
		assert.Error(err)
		assert.Contains(err.Error(), "InvalidRequest not found")
	})

	assert.Equal(string(types.ProgressStateError), deltaOps.lastState)
	assert.Contains(deltaOps.lastError, "InvalidRequest not found")
}
