package backupstore

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	mockDriverName = "mock"
	mockDriverURL  = "mock://localhost"
)

type mockStoreDriver struct {
	destURL      string
	delay        time.Duration
	numOfVolumes int
	numOfBackups int
	volumeInfos  map[string]*VolumeInfo
}

func (m *mockStoreDriver) Init() {
	if len(m.volumeInfos) > 0 {
		return
	}
	m.volumeInfos = make(map[string]*VolumeInfo, m.numOfVolumes)

	rand.Seed(time.Now().UnixNano())
	letterRunes := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	for i := 0; i < m.numOfVolumes; i++ {
		// generate random string with length 5
		pvc := make([]rune, 5)
		for i := range pvc {
			pvc[i] = letterRunes[rand.Intn(len(letterRunes))]
		}
		volumeName := fmt.Sprintf("pvc-%s", string(pvc))

		backups := make(map[string]*BackupInfo, m.numOfBackups)
		for j := 0; j < m.numOfBackups; j++ {
			// generate random string with length 5
			backup := make([]rune, 5)
			for i := range backup {
				backup[i] = letterRunes[rand.Intn(len(letterRunes))]
			}
			backupName := fmt.Sprintf("backup_backup-%s.cfg", string(backup))
			backups[backupName] = &BackupInfo{Name: backupName}
		}

		m.volumeInfos[volumeName] = &VolumeInfo{Name: volumeName, Backups: backups}
	}
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

	count := strings.Count(listPath, "/")
	switch count {
	case 1:
		// for listPath equal to "backupstore/volumes"
		lv1Map := make(map[string]struct{})
		for volumeName := range m.volumeInfos {
			path := getVolumePath(volumeName)
			if strings.HasPrefix(path, listPath) {
				ss := strings.Split(path, "/")
				lv1Map[ss[2]] = struct{}{}
			}
		}
		lv1s := []string{}
		for lv1 := range lv1Map {
			lv1s = append(lv1s, lv1)
		}
		return lv1s, nil
	case 2:
		// for listPath equal to "backupstore/volumes/xx"
		lv2Map := make(map[string]struct{})
		for volumeName := range m.volumeInfos {
			path := getVolumePath(volumeName)
			if strings.HasPrefix(path, listPath) {
				ss := strings.Split(path, "/")
				lv2Map[ss[3]] = struct{}{}
			}
		}
		lv2s := []string{}
		for lv2 := range lv2Map {
			lv2s = append(lv2s, lv2)
		}
		return lv2s, nil
	case 3:
		// for listPath equal to "backupstore/volumes/xx/yy"
		volumePaths := []string{}
		for volumeName := range m.volumeInfos {
			path := getVolumePath(volumeName)
			if strings.HasPrefix(path, listPath) {
				ss := strings.Split(path, "/")
				volumePaths = append(volumePaths, ss[4])
			}
		}
		return volumePaths, nil
	case 6:
		// for listPath equal to "backupstore/volumes/xx/yy/pvc-zzz/backups/"
		backups := []string{}
		for volumeName, volumeInfo := range m.volumeInfos {
			path := getVolumePath(volumeName)
			if !strings.HasPrefix(listPath, path) {
				continue
			}

			for backup := range volumeInfo.Backups {
				backups = append(backups, backup)
			}
			return backups, nil
		}
	}
	return nil, nil
}

func (m *mockStoreDriver) FileExists(filePath string) bool {
	return true
}

func (m *mockStoreDriver) FileSize(filePath string) int64 {
	return rand.Int63()
}

func (m *mockStoreDriver) FileTime(filePath string) time.Time {
	return time.Now()
}

func (m *mockStoreDriver) Remove(path string) error {
	return nil
}

func (m *mockStoreDriver) Read(src string) (io.ReadCloser, error) {
	defer time.Sleep(m.delay)

	for volumeName, volumeInfo := range m.volumeInfos {
		path := getVolumePath(volumeName)
		if !strings.HasPrefix(src, path) {
			continue
		}
		if !strings.HasSuffix(src, CFG_SUFFIX) {
			continue
		}

		if strings.Contains(src, VOLUME_CONFIG_FILE) {
			// read volume.cfg
			return ioutil.NopCloser(
				bytes.NewReader([]byte(fmt.Sprintf(`{"Name":"%s"}`, volumeName)))), nil
		} else if strings.Contains(src, BACKUP_CONFIG_PREFIX) {
			// read backup_backup-xxx.cfg
			for backupName := range volumeInfo.Backups {
				if strings.Contains(src, backupName) {
					return ioutil.NopCloser(
						bytes.NewReader([]byte(fmt.Sprintf(`{"Name":"%s","CreatedTime":"%s"}`, backupName, time.Now().String())))), nil
				}
			}
		}
	}

	return ioutil.NopCloser(bytes.NewReader([]byte(""))), nil
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

func TestListAllVolumeOnly(t *testing.T) {
	assert := assert.New(t)
	m := &mockStoreDriver{
		numOfVolumes: 4,
		delay:        time.Millisecond,
	}
	m.Init()
	err := RegisterDriver(mockDriverName, func(destURL string) (BackupStoreDriver, error) {
		m.destURL = destURL
		return m, nil
	})
	assert.NoError(err)
	defer unregisterDriver(mockDriverName)

	volumeInfo, err := List("", mockDriverURL, true)
	assert.NoError(err)
	assert.Equal(m.numOfVolumes, len(volumeInfo))
}

func TestListSingleVolumeBackups(t *testing.T) {
	assert := assert.New(t)
	m := &mockStoreDriver{
		numOfVolumes: 1,
		numOfBackups: 100,
		delay:        time.Millisecond,
	}
	m.Init()
	err := RegisterDriver(mockDriverName, func(destURL string) (BackupStoreDriver, error) {
		m.destURL = destURL
		return m, nil
	})
	assert.NoError(err)
	defer unregisterDriver(mockDriverName)

	for volumeName := range m.volumeInfos {
		volumeInfo, err := List(volumeName, mockDriverURL, false)
		assert.NoError(err)
		assert.Equal(m.numOfBackups, len(volumeInfo[volumeName].Backups))
	}
}

func BenchmarkListAllVolumeOnly10ms32volumes(b *testing.B) {
	m := &mockStoreDriver{
		numOfVolumes: 32,
		delay:        10 * time.Millisecond,
	}
	m.Init()
	RegisterDriver(mockDriverName, func(destURL string) (BackupStoreDriver, error) {
		m.destURL = destURL
		return m, nil
	})
	defer unregisterDriver(mockDriverName)

	for i := 0; i < b.N; i++ {
		List("", mockDriverURL, true)
	}
}

func BenchmarkListAllVolumeOnly100ms32volumes(b *testing.B) {
	m := &mockStoreDriver{
		numOfVolumes: 32,
		delay:        100 * time.Millisecond,
	}
	m.Init()
	RegisterDriver(mockDriverName, func(destURL string) (BackupStoreDriver, error) {
		m.destURL = destURL
		return m, nil
	})
	defer unregisterDriver(mockDriverName)

	for i := 0; i < b.N; i++ {
		List("", mockDriverURL, true)
	}
}

func BenchmarkListAllVolumeOnly250ms32volumes(b *testing.B) {
	m := &mockStoreDriver{
		numOfVolumes: 32,
		delay:        250 * time.Millisecond,
	}
	m.Init()
	RegisterDriver(mockDriverName, func(destURL string) (BackupStoreDriver, error) {
		m.destURL = destURL
		return m, nil
	})
	defer unregisterDriver(mockDriverName)

	for i := 0; i < b.N; i++ {
		List("", mockDriverURL, true)
	}
}

func BenchmarkListAllVolumeOnly500ms32volumes(b *testing.B) {
	m := &mockStoreDriver{
		numOfVolumes: 32,
		delay:        500 * time.Millisecond,
	}
	m.Init()
	RegisterDriver(mockDriverName, func(destURL string) (BackupStoreDriver, error) {
		m.destURL = destURL
		return m, nil
	})
	defer unregisterDriver(mockDriverName)

	for i := 0; i < b.N; i++ {
		List("", mockDriverURL, true)
	}
}

func BenchmarkListSingleVolumeBackups10ms(b *testing.B) {
	m := &mockStoreDriver{
		numOfVolumes: 1,
		numOfBackups: 50,
		delay:        10 * time.Millisecond,
	}
	m.Init()
	RegisterDriver(mockDriverName, func(destURL string) (BackupStoreDriver, error) {
		m.destURL = destURL
		return m, nil
	})
	defer unregisterDriver(mockDriverName)

	for i := 0; i < b.N; i++ {
		for volumeName := range m.volumeInfos {
			List(volumeName, mockDriverURL, false)
		}
	}
}

func BenchmarkListSingleVolumeBackups100ms(b *testing.B) {
	m := &mockStoreDriver{
		numOfVolumes: 1,
		numOfBackups: 50,
		delay:        100 * time.Millisecond,
	}
	m.Init()
	RegisterDriver(mockDriverName, func(destURL string) (BackupStoreDriver, error) {
		m.destURL = destURL
		return m, nil
	})
	defer unregisterDriver(mockDriverName)

	for i := 0; i < b.N; i++ {
		for volumeName := range m.volumeInfos {
			List(volumeName, mockDriverURL, false)
		}
	}
}

func BenchmarkListSingleVolumeBackups250ms(b *testing.B) {
	m := &mockStoreDriver{
		numOfVolumes: 1,
		numOfBackups: 50,
		delay:        250 * time.Millisecond,
	}
	m.Init()
	RegisterDriver(mockDriverName, func(destURL string) (BackupStoreDriver, error) {
		m.destURL = destURL
		return m, nil
	})
	defer unregisterDriver(mockDriverName)

	for i := 0; i < b.N; i++ {
		for volumeName := range m.volumeInfos {
			List(volumeName, mockDriverURL, false)
		}
	}
}

func BenchmarkListSingleVolumeBackups500ms(b *testing.B) {
	m := &mockStoreDriver{
		numOfVolumes: 1,
		numOfBackups: 50,
		delay:        500 * time.Millisecond,
	}
	m.Init()
	RegisterDriver(mockDriverName, func(destURL string) (BackupStoreDriver, error) {
		m.destURL = destURL
		return m, nil
	})
	defer unregisterDriver(mockDriverName)

	for i := 0; i < b.N; i++ {
		for volumeName := range m.volumeInfos {
			List(volumeName, mockDriverURL, false)
		}
	}
}
