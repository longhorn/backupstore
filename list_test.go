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

type mockStoreDriver struct {
	destURL      string
	delay        time.Duration
	numOfVolumes int
	volumePaths  []string
}

func (m *mockStoreDriver) Init() {
	if len(m.volumePaths) > 0 {
		return
	}

	rand.Seed(time.Now().UnixNano())
	letterRunes := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	for i := 0; i < m.numOfVolumes; i++ {
		// generate random string with length 5
		b := make([]rune, 5)
		for i := range b {
			b[i] = letterRunes[rand.Intn(len(letterRunes))]
		}
		volumeName := fmt.Sprintf("pvc-%s", string(b))
		m.volumePaths = append(m.volumePaths, getVolumePath((volumeName)))
	}
}

func (m *mockStoreDriver) Kind() string {
	return "mock"
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
		for _, path := range m.volumePaths {
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
		for _, path := range m.volumePaths {
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
		for _, path := range m.volumePaths {
			if strings.HasPrefix(path, listPath) {
				ss := strings.Split(path, "/")
				volumePaths = append(volumePaths, ss[4])
			}
		}
		return volumePaths, nil
	}
	return nil, nil
}

func (m *mockStoreDriver) FileExists(filePath string) bool {
	for _, path := range m.volumePaths {
		// the filePath format is "backupstore/volumes/xx/yy/pvc-zzz/volume.cfg"
		if strings.HasPrefix(filePath, path) {
			return true
		}
	}
	return false
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

	// parse the src that the format is "backupstore/volumes/xx/yy/pvc-zzz/volume.cfg"
	ss := strings.Split(strings.TrimSuffix(src, VOLUME_CONFIG_FILE), "/")
	name := ss[len(ss)-2]
	return ioutil.NopCloser(
		bytes.NewReader([]byte(fmt.Sprintf(`{"Name":"%s"}`, name)))), nil
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

func TestList(t *testing.T) {
	assert := assert.New(t)
	m := &mockStoreDriver{
		numOfVolumes: 4,
		delay:        time.Millisecond,
	}
	m.Init()
	initFunc := func(destURL string) (BackupStoreDriver, error) {
		m.destURL = destURL
		return m, nil
	}
	_ = RegisterDriver("mock", initFunc)

	volumeInfo, err := List("", "mock://localhost", true)
	assert.NoError(err)
	assert.Equal(m.numOfVolumes, len(volumeInfo))
}
