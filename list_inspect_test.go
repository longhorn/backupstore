package backupstore

import (
	"fmt"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/afero"

	"github.com/stretchr/testify/assert"
)

const (
	mockDriverName = "mock"
	mockDriverURL  = "mock://localhost"
)

type mockStoreDriver struct {
	fs      afero.Fs
	delay   time.Duration
	destURL string
}

func (m *mockStoreDriver) Init() {
	m.fs = afero.NewMemMapFs()
	m.destURL = mockDriverURL

	RegisterDriver(mockDriverName, func(destURL string) (BackupStoreDriver, error) {
		m.fs.MkdirAll(filepath.Join(backupstoreBase, VOLUME_DIRECTORY), 0755)
		return m, nil
	})
}

func (m *mockStoreDriver) uninstall() {
	m.fs.RemoveAll("/")
	unregisterDriver(mockDriverName)
}

func (m *mockStoreDriver) Kind() string {
	return mockDriverName
}

func (m *mockStoreDriver) GetURL() string {
	return m.destURL
}

func (m *mockStoreDriver) updatePath(path string) string {
	return ""
}

func (m *mockStoreDriver) List(listPath string) ([]string, error) {
	defer time.Sleep(m.delay)

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

func (m *mockStoreDriver) FileExists(filePath string) bool {
	exist, err := afero.Exists(m.fs, filePath)
	if err != nil {
		return false
	}
	return exist
}

func (m *mockStoreDriver) FileSize(filePath string) int64 {
	fi, err := m.fs.Stat(filePath)
	if err != nil {
		return -1
	}
	return fi.Size()
}

func (m *mockStoreDriver) FileTime(filePath string) time.Time {
	fi, err := m.fs.Stat(filePath)
	if err != nil {
		return time.Now()
	}
	return fi.ModTime()
}

func (m *mockStoreDriver) Remove(path string) error {
	return m.fs.Remove(path)
}

func (m *mockStoreDriver) Read(src string) (io.ReadCloser, error) {
	defer time.Sleep(m.delay)

	file, err := m.fs.Open(src)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (m *mockStoreDriver) Write(dst string, rs io.ReadSeeker) error {
	return nil
}

func (m *mockStoreDriver) Upload(src, dst string) error {
	return nil
}

func (m *mockStoreDriver) Download(src, dst string) error {
	return nil
}

func TestListBackupVolumeNames(t *testing.T) {
	assert := assert.New(t)

	m := &mockStoreDriver{delay: time.Millisecond}
	m.Init()
	defer m.uninstall()

	// list folder backupstore/volumes/
	volumeInfo, err := List("", mockDriverURL, true)
	assert.NoError(err)
	assert.Equal(0, len(volumeInfo))

	// create pvc-1 folder and metadata
	m.fs.MkdirAll(getVolumePath("pvc-1"), 0755)
	afero.WriteFile(m.fs, getVolumeFilePath("pvc-1"), []byte(`{"Name":"pvc-1"}`), 0644)

	// create pvc-2 folder without metadata
	m.fs.MkdirAll(getVolumePath("pvc-2"), 0755)

	// list backup volume names
	volumeInfo, err = List("", mockDriverURL, true)
	assert.NoError(err)
	assert.Equal(2, len(volumeInfo))
	assert.Equal(0, len(volumeInfo["pvc-1"].Messages))
	assert.Equal(1, len(volumeInfo["pvc-2"].Messages))
}

func TestListBackupVolumeBackups(t *testing.T) {
	assert := assert.New(t)

	m := &mockStoreDriver{delay: time.Millisecond}
	m.Init()
	defer m.uninstall()

	// create pvc-1 folder
	m.fs.MkdirAll(getVolumePath("pvc-1"), 0755)

	// list pvc-1 without metadata
	volumeInfo, err := List("pvc-1", mockDriverURL, false)
	assert.NoError(err)
	assert.Equal(1, len(volumeInfo["pvc-1"].Messages))

	// create pvc-1 metadata
	afero.WriteFile(m.fs, getVolumeFilePath("pvc-1"), []byte(`{"Name":"pvc-1"}`), 0644)

	// create backups folder
	m.fs.MkdirAll(getBackupPath("pvc-1"), 0755)

	// create 100 backups metadata
	for i := 1; i <= 100; i++ {
		backup := fmt.Sprintf("backup-%d", i)
		afero.WriteFile(m.fs, getBackupConfigPath(backup, "pvc-1"),
			[]byte(fmt.Sprintf(`{"Name":"%s","CreatedTime":"%s"}`, backup, time.Now().String())), 0644)
	}

	volumeInfo, err = List("pvc-1", mockDriverURL, false)
	assert.NoError(err)
	assert.Equal(1, len(volumeInfo))
	assert.Equal(0, len(volumeInfo["pvc-1"].Messages))
	assert.Equal(100, len(volumeInfo["pvc-1"].Backups))
}

func TestInspectVolume(t *testing.T) {
	assert := assert.New(t)

	m := &mockStoreDriver{delay: time.Millisecond}
	m.Init()
	defer m.uninstall()

	// create pvc-1 folder and metadata
	m.fs.MkdirAll(getVolumePath("pvc-1"), 0755)

	volumeURL := EncodeMetadataURL("", "pvc-1", mockDriverURL)
	volumeInfo, err := InspectVolume(volumeURL)
	assert.Error(err)
	assert.Nil(volumeInfo)

	// create pvc-1 metadata
	afero.WriteFile(m.fs, getVolumeFilePath("pvc-1"),
		[]byte(`{"Name":"pvc-1","Size":"2147483648","CreatedTime":"2021-05-12T00:52:01Z","LastBackupName":"backup-3","LastBackupAt":"2021-05-17T05:31:01Z"}`), 0644)

	// inspect backup volume metadata
	volumeURL = EncodeMetadataURL("", "pvc-1", mockDriverURL)
	volumeInfo, err = InspectVolume(volumeURL)
	assert.NoError(err)
	assert.Equal(volumeInfo.Name, "pvc-1")
	assert.Equal(volumeInfo.Size, int64(2147483648))
	assert.Equal(volumeInfo.Created, "2021-05-12T00:52:01Z")
	assert.Equal(volumeInfo.LastBackupName, "backup-3")
	assert.Equal(volumeInfo.LastBackupAt, "2021-05-17T05:31:01Z")
}

func TestInspectBackup(t *testing.T) {
	assert := assert.New(t)

	m := &mockStoreDriver{delay: time.Millisecond}
	m.Init()
	defer m.uninstall()

	// create pvc-1 folder and metadata
	m.fs.MkdirAll(getVolumePath("pvc-1"), 0755)

	backupURL := EncodeMetadataURL("backup-1", "pvc-1", mockDriverURL)
	backupInfo, err := InspectBackup(backupURL)
	assert.Error(err)
	assert.Nil(backupInfo)

	// create pvc-1 metadata
	afero.WriteFile(m.fs, getVolumeFilePath("pvc-1"),
		[]byte(`{"Name":"pvc-1","Size":"2147483648","CreatedTime":"2021-05-12T00:52:01Z","LastBackupName":"backup-3","LastBackupAt":"2021-05-17T05:31:01Z"}`), 0644)

	// create backups folder
	m.fs.MkdirAll(getBackupPath("pvc-1"), 0755)

	// inspect an invalid backup-1 metadata
	afero.WriteFile(m.fs, getBackupConfigPath("backup-1", "pvc-1"), []byte(""), 0644)
	backupInfo, err = InspectBackup(backupURL)
	assert.Error(err)
	assert.Nil(backupInfo)

	// create a in progress backup-1 metadata
	afero.WriteFile(m.fs, getBackupConfigPath("backup-1", "pvc-1"),
		[]byte(`{"Name":"backup-1"}`), 0644)
	backupInfo, err = InspectBackup(backupURL)
	assert.Error(err)
	assert.Nil(backupInfo)

	// create a valid backup-1 metadata
	afero.WriteFile(m.fs, getBackupConfigPath("backup-1", "pvc-1"),
		[]byte(`{"Name":"backup-1","VolumeName":"pvc-1","Size":"115343360","SnapshotName":"1eb35e75-73d8-4e8c-9761-3df6ec35ba9a","SnapshotCreatedAt":"2021-06-07T08:57:23Z","CreatedTime":"2021-06-07T08:57:25Z","Size":"115343360"}`), 0644)

	// inspect backup-1 metadata
	backupInfo, err = InspectBackup(backupURL)
	assert.NoError(err)
	assert.Equal(backupInfo.Name, "backup-1")
	assert.Equal(backupInfo.URL, backupURL)
	assert.Equal(backupInfo.SnapshotName, "1eb35e75-73d8-4e8c-9761-3df6ec35ba9a")
	assert.Equal(backupInfo.SnapshotCreated, "2021-06-07T08:57:23Z")
	assert.Equal(backupInfo.Created, "2021-06-07T08:57:25Z")
	assert.Equal(backupInfo.Size, int64(115343360))
}

func BenchmarkBackupVolumeNames10ms32volumes(b *testing.B) {
	m := &mockStoreDriver{delay: 10 * time.Millisecond}
	m.Init()
	defer m.uninstall()

	// create 32 backup volumes
	for i := 1; i <= 32; i++ {
		pvc := fmt.Sprintf("pvc-%d", i)
		m.fs.MkdirAll(getVolumePath(pvc), 0755)
		afero.WriteFile(m.fs, getVolumeFilePath(pvc), []byte(fmt.Sprintf(`{"Name":%s}`, pvc)), 0644)
	}

	for i := 0; i < b.N; i++ {
		List("", mockDriverURL, true)
	}
}

func BenchmarkBackupVolumeNames100ms32volumes(b *testing.B) {
	m := &mockStoreDriver{delay: 100 * time.Millisecond}
	m.Init()
	defer m.uninstall()

	// create 32 backup volumes
	for i := 1; i <= 32; i++ {
		pvc := fmt.Sprintf("pvc-%d", i)
		m.fs.MkdirAll(getVolumePath(pvc), 0755)
		afero.WriteFile(m.fs, getVolumeFilePath(pvc), []byte(fmt.Sprintf(`{"Name":%s}`, pvc)), 0644)
	}

	for i := 0; i < b.N; i++ {
		List("", mockDriverURL, true)
	}
}

func BenchmarkBackupVolumeNames250ms32volumes(b *testing.B) {
	m := &mockStoreDriver{delay: 250 * time.Millisecond}
	m.Init()
	defer m.uninstall()

	// create 32 backup volumes
	for i := 1; i <= 32; i++ {
		pvc := fmt.Sprintf("pvc-%d", i)
		m.fs.MkdirAll(getVolumePath(pvc), 0755)
		afero.WriteFile(m.fs, getVolumeFilePath(pvc), []byte(fmt.Sprintf(`{"Name":%s}`, pvc)), 0644)
	}

	for i := 0; i < b.N; i++ {
		List("", mockDriverURL, true)
	}
}

func BenchmarkBackupVolumeNames500ms32volumes(b *testing.B) {
	m := &mockStoreDriver{delay: 500 * time.Millisecond}
	m.Init()
	defer m.uninstall()

	// create 32 backup volumes
	for i := 1; i <= 32; i++ {
		pvc := fmt.Sprintf("pvc-%d", i)
		m.fs.MkdirAll(getVolumePath(pvc), 0755)
		afero.WriteFile(m.fs, getVolumeFilePath(pvc), []byte(fmt.Sprintf(`{"Name":%s}`, pvc)), 0644)
	}

	for i := 0; i < b.N; i++ {
		List("", mockDriverURL, true)
	}
}

func BenchmarkListBackupVolumeBackups10ms(b *testing.B) {
	m := &mockStoreDriver{delay: 10 * time.Millisecond}
	m.Init()
	defer m.uninstall()

	// create pvc-1 metadata
	m.fs.MkdirAll(getBackupPath("pvc-1"), 0755)
	afero.WriteFile(m.fs, getVolumeFilePath("pvc-1"), []byte(`{"Name":"pvc-1"}`), 0644)

	// create 100 backups
	for i := 1; i <= 100; i++ {
		backup := fmt.Sprintf("backup-%d", i)
		afero.WriteFile(m.fs, getBackupConfigPath(backup, "pvc-1"),
			[]byte(fmt.Sprintf(`{"Name":"%s","CreatedTime":"%s"}`, backup, time.Now().String())), 0644)
	}

	for i := 0; i < b.N; i++ {
		List("pvc-1", mockDriverURL, false)
	}
}

func BenchmarkListBackupVolumeBackups100ms(b *testing.B) {
	m := &mockStoreDriver{delay: 100 * time.Millisecond}
	m.Init()
	defer m.uninstall()

	// create pvc-1 metadata
	m.fs.MkdirAll(getBackupPath("pvc-1"), 0755)
	afero.WriteFile(m.fs, getVolumeFilePath("pvc-1"), []byte(`{"Name":"pvc-1"}`), 0644)

	// create 100 backups
	for i := 1; i <= 100; i++ {
		backup := fmt.Sprintf("backup-%d", i)
		afero.WriteFile(m.fs, getBackupConfigPath(backup, "pvc-1"),
			[]byte(fmt.Sprintf(`{"Name":"%s","CreatedTime":"%s"}`, backup, time.Now().String())), 0644)
	}

	for i := 0; i < b.N; i++ {
		List("pvc-1", mockDriverURL, false)
	}
}

func BenchmarkListBackupVolumeBackups250ms(b *testing.B) {
	m := &mockStoreDriver{delay: 250 * time.Millisecond}
	m.Init()
	defer m.uninstall()

	// create pvc-1 metadata
	m.fs.MkdirAll(getBackupPath("pvc-1"), 0755)
	afero.WriteFile(m.fs, getVolumeFilePath("pvc-1"), []byte(`{"Name":"pvc-1"}`), 0644)

	// create 100 backups
	for i := 1; i <= 100; i++ {
		backup := fmt.Sprintf("backup-%d", i)
		afero.WriteFile(m.fs, getBackupConfigPath(backup, "pvc-1"),
			[]byte(fmt.Sprintf(`{"Name":"%s","CreatedTime":"%s"}`, backup, time.Now().String())), 0644)
	}

	for i := 0; i < b.N; i++ {
		List("pvc-1", mockDriverURL, false)
	}
}

func BenchmarkListBackupVolumeBackups500ms(b *testing.B) {
	m := &mockStoreDriver{delay: 500 * time.Millisecond}
	m.Init()
	defer m.uninstall()

	// create pvc-1 metadata
	m.fs.MkdirAll(getBackupPath("pvc-1"), 0755)
	afero.WriteFile(m.fs, getVolumeFilePath("pvc-1"), []byte(`{"Name":"pvc-1"}`), 0644)

	// create 100 backups
	for i := 1; i <= 100; i++ {
		backup := fmt.Sprintf("backup-%d", i)
		afero.WriteFile(m.fs, getBackupConfigPath(backup, "pvc-1"),
			[]byte(fmt.Sprintf(`{"Name":"%s","CreatedTime":"%s"}`, backup, time.Now().String())), 0644)
	}

	for i := 0; i < b.N; i++ {
		List("pvc-1", mockDriverURL, false)
	}
}
