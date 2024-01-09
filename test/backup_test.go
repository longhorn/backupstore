package test

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	. "gopkg.in/check.v1"

	"github.com/longhorn/backupstore"
	_ "github.com/longhorn/backupstore/nfs"
	"github.com/longhorn/backupstore/types"
	"github.com/longhorn/backupstore/util"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	volumeName        = "BackupStoreTestVolume"
	volumeName2       = "BackupStoreExtraTestVolume"
	volumeContentSize = int64(5 * 2 * 1024 * 1024)       // snapshotCounts number of blocks
	volumeSize        = int64((5 + 4) * 2 * 1024 * 1024) // snapshotCounts number of blocks + intended empty block
	snapPrefix        = "volume_snap"
	snapshotCounts    = 5
	snapIncrePreifix  = "restore-snap-"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
	BasePath        string
	BackupStorePath string
}

var _ = Suite(&TestSuite{})

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

/*
type DeltaBlockBackupOperations interface {
	HasSnapshot(id, volumeID string) bool
	CompareSnapshot(id, compareID, volumeID string) (*Mappings, error)
	OpenSnapshot(id, volumeID string) error
	ReadSnapshot(id, volumeID string, start int64, data []byte) error
	CloseSnapshot(id, volumeID string) error
}
*/

type RawFileVolume struct {
	lock            sync.Mutex
	v               backupstore.Volume
	Snapshots       []backupstore.Snapshot
	BackupState     string
	BackupProgress  int
	BackupError     string
	BackupURL       string
	RestoreProgress int
	RestoreError    error
	stopOnce        sync.Once
	stopChan        chan struct{}
}

func (r *RawFileVolume) OpenVolumeDev(volDevName string) (*os.File, string, error) {
	if _, err := os.Stat(volDevName); err == nil {
		logrus.WithError(err).Warnf("File %s for the restore exists, will remove and re-create it", volDevName)
		if err := os.RemoveAll(volDevName); err != nil {
			return nil, "", errors.Wrapf(err, "failed to clean up the existing file %v before restore", volDevName)
		}
	}

	fh, err := os.Create(volDevName)
	return fh, volDevName, err
}

func (r *RawFileVolume) CloseVolumeDev(volDev *os.File) error {
	if volDev == nil {
		return nil
	}
	return volDev.Close()
}

func (r *RawFileVolume) UpdateBackupStatus(id, volumeID string, backupState string, backupProgress int, backupURL string, backupError string) error {
	r.lock.Lock()
	r.BackupState = backupState
	r.BackupProgress = backupProgress
	r.BackupURL = backupURL
	r.BackupError = backupError
	r.lock.Unlock()
	return nil
}

func (r *RawFileVolume) GetBackupStatus() (string, string) {
	r.lock.Lock()
	bURL := r.BackupURL
	bErr := r.BackupError
	r.lock.Unlock()
	return bURL, bErr
}

func (r *RawFileVolume) ResetBackupStatus() {
	r.lock.Lock()
	r.BackupURL = ""
	r.BackupError = ""
	r.lock.Unlock()
}

func (r *RawFileVolume) UpdateRestoreStatus(snapshot string, restoreProgress int, err error) {
	r.lock.Lock()
	r.RestoreProgress = restoreProgress
	r.RestoreError = err
	r.lock.Unlock()
}

func (r *RawFileVolume) GetRestoreStatus() (int, error) {
	r.lock.Lock()
	rp := r.RestoreProgress
	re := r.RestoreError
	r.lock.Unlock()
	return rp, re
}

func (r *RawFileVolume) ResetRestoreStatus() {
	r.lock.Lock()
	r.RestoreProgress = 0
	r.RestoreError = nil
	r.lock.Unlock()
}

func (r *RawFileVolume) Stop() {
	r.stopOnce.Do(func() {
		close(r.stopChan)
	})
}

func (r *RawFileVolume) GetStopChan() chan struct{} {
	return r.stopChan
}

func (r *RawFileVolume) HasSnapshot(id, volumeID string) bool {
	_, ok := os.Stat(id)
	return ok == nil
}

func (r *RawFileVolume) CompareSnapshot(id, compareID, volumeID string) (*types.Mappings, error) {
	mappings := types.Mappings{
		Mappings:  []types.Mapping{},
		BlockSize: backupstore.DEFAULT_BLOCK_SIZE,
	}

	snap1, err := os.Open(id)
	if err != nil {
		fmt.Println("Failed to open", id)
		return nil, err
	}
	defer snap1.Close()

	blockSize := int64(backupstore.DEFAULT_BLOCK_SIZE)

	if compareID == "" {
		emptyData := make([]byte, blockSize)
		for i := int64(0); i < volumeContentSize/blockSize; i++ {
			offset := i * blockSize
			data := make([]byte, blockSize)
			if _, err := snap1.ReadAt(data, offset); err != nil {
				return nil, err
			}
			if !reflect.DeepEqual(data, emptyData) {
				mappings.Mappings = append(mappings.Mappings, types.Mapping{
					Offset: offset,
					Size:   blockSize,
				})
			}
		}
		return &mappings, nil
	}

	snap2, err := os.Open(compareID)
	if err != nil {
		fmt.Println("Failed to open", compareID)
		return nil, err
	}
	defer snap2.Close()

	for i := int64(0); i < volumeContentSize/blockSize; i++ {
		offset := i * blockSize
		data1 := make([]byte, blockSize)
		data2 := make([]byte, blockSize)
		if _, err := snap1.ReadAt(data1, offset); err != nil {
			return nil, err
		}
		if _, err := snap2.ReadAt(data2, offset); err != nil {
			return nil, err
		}
		if !reflect.DeepEqual(data1, data2) {
			mappings.Mappings = append(mappings.Mappings, types.Mapping{
				Offset: offset,
				Size:   blockSize,
			})
		}
	}
	return &mappings, nil
}

func (r *RawFileVolume) OpenSnapshot(id, volumeID string) error {
	return nil
}

func (r *RawFileVolume) ReadSnapshot(id, volumeID string, start int64, data []byte) error {
	f, err := os.Open(id)
	if err != nil {
		return err
	}

	_, err = f.ReadAt(data, start)
	return err
}

func (r *RawFileVolume) CloseSnapshot(id, volumeID string) error {
	return nil
}

func (s *TestSuite) getSnapshotName(snapPrefix string, i int) string {
	return filepath.Join(s.BasePath, snapPrefix+strconv.Itoa(i))
}

func (s *TestSuite) randomChange(data []byte, offset, length int64) {
	for i := int64(0); i < length; i++ {
		data[offset+i] = letterBytes[rand.Intn(len(letterBytes))]
	}
}

func (s *TestSuite) SetUpSuite(c *C) {
	rand.Seed(time.Now().UTC().UnixNano())

	dir, err := os.MkdirTemp("", "backupstore-test")
	c.Assert(err, IsNil)

	s.BasePath = dir
	s.BackupStorePath = filepath.Join(s.BasePath, "backupstore")

	err = exec.Command("mkdir", "-p", s.BackupStorePath).Run()
	c.Assert(err, IsNil)
}

func (s *TestSuite) TearDownSuite(c *C) {
	err := exec.Command("rm", "-rf", s.BasePath).Run()
	c.Assert(err, IsNil)
}

func (s *TestSuite) getDestURL() string {
	//return "vfs://" + s.BackupStorePath

	_, runningInDapper := os.LookupEnv("DAPPER_SOURCE")

	if runningInDapper {
		nfsIPAddr := os.Getenv("NFS_IPADDR")
		return "nfs://" + nfsIPAddr + ":/"
	}

	return "nfs://127.0.0.1:/opt/backupstore"
}

func getBackupNameFromBackupNameInConfig(backupNameInConfig string) string {
	if backupNameInConfig == "" {
		return util.GenerateName("backup")
	}
	return backupNameInConfig
}

func (s *TestSuite) createAndWaitForBackup(c *C, config *backupstore.DeltaBackupConfig, deltaOps *RawFileVolume) string {
	backupName := getBackupNameFromBackupNameInConfig(config.BackupName)
	_, err := backupstore.CreateDeltaBlockBackup(backupName, config)
	c.Assert(err, IsNil)
	if config.BackupName != "" {
		c.Assert(backupName, Equals, config.BackupName)
	} else {
		c.Assert(backupName, Not(Equals), "")
	}

	retryCount := 120
	var bError, bURL string
	for j := 0; j < retryCount; j++ {

		bURL, bError = deltaOps.GetBackupStatus()
		c.Assert(bError, Equals, "")
		if bURL != "" {
			break
		}
		time.Sleep(1 * time.Second)
	}

	c.Assert(bURL, Not(Equals), "")
	deltaOps.ResetBackupStatus()

	return bURL
}

func (s *TestSuite) waitForRestoreCompletion(c *C, deltaOps *RawFileVolume) {
	var rError error
	retryCount := 120
	rProgress := 0

	for j := 0; j < retryCount; j++ {
		rProgress, rError = deltaOps.GetRestoreStatus()
		c.Assert(rError, Equals, nil)
		if rProgress == 100 {
			break
		}
		time.Sleep(1 * time.Second)
	}

	c.Assert(rProgress, Equals, 100)
	deltaOps.ResetRestoreStatus()
}

func (s *TestSuite) TestBackupBasic(c *C) {
	compressionMethods := []string{"none", "lz4", "gzip"}
	concurrentLimit := 5
	for _, compressionMethod := range compressionMethods {
		volumeNameInTest := volumeName + "-" + compressionMethod

		// Make identical blocks in the file
		data := make([]byte, volumeSize)
		blockSize := int64(backupstore.DEFAULT_BLOCK_SIZE)

		for i := int64(0); i < blockSize; i++ {
			data[i] = letterBytes[rand.Intn(len(letterBytes))]
		}
		for i := int64(1); i < volumeContentSize/blockSize; i++ {
			for j := int64(0); j < blockSize; j++ {
				data[i*blockSize+j] = data[j]
			}
		}

		volume := RawFileVolume{
			v: backupstore.Volume{
				Name:              volumeNameInTest,
				Size:              volumeSize,
				CreatedTime:       util.Now(),
				CompressionMethod: compressionMethod,
				DataEngine:        string(backupstore.DataEngineV1),
			},
			stopChan: make(chan struct{}),
		}
		// Each snapshot will be one more block different from before
		for i := 0; i < snapshotCounts; i++ {
			snapName := s.getSnapshotName("snapshot-", i)
			volume.Snapshots = append(volume.Snapshots,
				backupstore.Snapshot{
					Name:        snapName,
					CreatedTime: util.Now(),
				})

			err := os.WriteFile(snapName, data, 0600)
			c.Assert(err, IsNil)

			s.randomChange(data, int64(i)*blockSize, 10)
		}

		backupN := ""
		for i := 0; i < snapshotCounts; i++ {
			config := &backupstore.DeltaBackupConfig{
				BackupName:      util.GenerateName("backup"),
				Volume:          &volume.v,
				Snapshot:        &volume.Snapshots[i],
				DestURL:         s.getDestURL(),
				DeltaOps:        &volume,
				ConcurrentLimit: int32(concurrentLimit),
				Labels: map[string]string{
					"SnapshotName": volume.Snapshots[i].Name,
					"RandomKey":    "RandomValue",
				},
			}
			backup := s.createAndWaitForBackup(c, config, &volume)
			if i == snapshotCounts-1 {
				backupN = backup
			}

			restore := filepath.Join(s.BasePath, "restore-"+strconv.Itoa(i))
			rConfig := &backupstore.DeltaRestoreConfig{
				BackupURL:       backup,
				DeltaOps:        &volume,
				Filename:        restore,
				ConcurrentLimit: int32(concurrentLimit),
			}

			err := backupstore.RestoreDeltaBlockBackup(context.Background(), rConfig)
			c.Assert(err, IsNil)
			s.waitForRestoreCompletion(c, &volume)

			err = exec.Command("diff", volume.Snapshots[i].Name, restore).Run()
			c.Assert(err, IsNil)

			// inspect the backup config
			backupName, volumeName, _, err := backupstore.DecodeBackupURL(backup)
			c.Assert(err, IsNil)
			backupInfo, err := backupstore.InspectBackup(backup)
			c.Assert(err, IsNil)
			c.Assert(backupInfo.Name, Equals, backupName)
			c.Assert(backupInfo.URL, Equals, backup)
			c.Assert(backupInfo.SnapshotName, Equals, volume.Snapshots[i].Name)
			c.Assert(backupInfo.SnapshotCreated, Equals, volume.Snapshots[i].CreatedTime)
			c.Assert(backupInfo.Created, Not(Equals), "")
			c.Assert(backupInfo.Size, Equals, volumeContentSize)
			c.Assert(backupInfo.Labels["SnapshotName"], Equals, volume.Snapshots[i].Name)
			c.Assert(backupInfo.Labels["RandomKey"], Equals, "RandomValue")
			if i == 0 {
				c.Assert(backupInfo.IsIncremental, Equals, false)
			} else {
				c.Assert(backupInfo.IsIncremental, Equals, true)
			}
			c.Assert(backupInfo.VolumeName, Equals, volumeName)
			c.Assert(backupInfo.VolumeSize, Equals, volumeSize)
			c.Assert(backupInfo.VolumeCreated, Equals, volume.v.CreatedTime)
			c.Assert(backupInfo.VolumeBackingImageName, Equals, "")
		}

		// list backup volume names only
		listName, err := backupstore.List(volume.v.Name, s.getDestURL(), true)
		c.Assert(err, IsNil)
		c.Assert(len(listName), Equals, 1)
		volumeName, ok := listName[volume.v.Name]
		c.Assert(ok, Equals, true)
		c.Assert(len(volumeName.Backups), Equals, 0)

		// list backup volume name and it's backups
		listName, err = backupstore.List(volume.v.Name, s.getDestURL(), false)
		c.Assert(err, IsNil)
		c.Assert(len(listName), Equals, 1)
		volumeName, ok = listName[volume.v.Name]
		c.Assert(ok, Equals, true)
		c.Assert(len(volumeName.Backups), Equals, snapshotCounts)

		// inspect backup volume config
		volumeInfo, err := backupstore.InspectVolume(backupstore.EncodeBackupURL("", volume.v.Name, s.getDestURL()))
		c.Assert(err, IsNil)
		c.Assert(volumeInfo.Name, Equals, volume.v.Name)
		c.Assert(volumeInfo.Size, Equals, volumeSize)
		c.Assert(volumeInfo.Created, Equals, volume.v.CreatedTime)
		c.Assert(volumeInfo.DataStored, Equals, int64(snapshotCounts*backupstore.DEFAULT_BLOCK_SIZE))
		backupName, _, _, err := backupstore.DecodeBackupURL(backupN)
		c.Assert(err, IsNil)
		c.Assert(volumeInfo.LastBackupName, Equals, backupName)
		c.Assert(volumeInfo.LastBackupAt, Not(Equals), "")
		c.Assert(volumeInfo.BackingImageName, Equals, "")
		c.Assert(volumeInfo.BackingImageChecksum, Equals, "")
	}
}

func (s *TestSuite) TestBackupRestoreExtra(c *C) {
	compressionMethods := []string{"none", "gzip", "lz4"}
	concurrentLimit := 5
	for _, compressionMethod := range compressionMethods {
		volumeNameInTest := volumeName2 + "-" + compressionMethod
		// Make one block data
		blockSize := int64(backupstore.DEFAULT_BLOCK_SIZE)
		data := make([]byte, blockSize)
		dataEmpty := make([]byte, blockSize)
		dataModified := make([]byte, blockSize)
		for i := int64(0); i < blockSize; i++ {
			data[i] = letterBytes[rand.Intn(len(letterBytes))]
			dataModified[i] = letterBytes[rand.Intn(len(letterBytes))]
		}

		volume := RawFileVolume{
			v: backupstore.Volume{
				Name:              volumeNameInTest,
				Size:              volumeSize,
				CompressionMethod: compressionMethod,
				CreatedTime:       util.Now(),
				DataEngine:        string(backupstore.DataEngineV1),
			},
			stopChan: make(chan struct{}),
		}

		for i := 0; i < snapshotCounts; i++ {
			volume.Snapshots = append(volume.Snapshots,
				backupstore.Snapshot{
					Name:        s.getSnapshotName(snapIncrePreifix, i),
					CreatedTime: util.Now(),
				})
		}

		// snap0: blk * 4
		snap0, err := os.Create(volume.Snapshots[0].Name)
		c.Assert(err, IsNil)
		defer snap0.Close()
		for i := int64(0); i < 4; i++ {
			_, err := snap0.WriteAt(data, i*blockSize)
			c.Assert(err, IsNil)
		}
		for i := int64(4); i < volumeSize/blockSize; i++ {
			_, err := snap0.WriteAt(dataEmpty, i*blockSize)
			c.Assert(err, IsNil)
		}
		err = snap0.Truncate(volumeSize)
		c.Assert(err, IsNil)

		// snap1: blk * 2 + empty blk * 1 + blk * 1
		snap1, err := os.Create(volume.Snapshots[1].Name)
		c.Assert(err, IsNil)
		_, err = io.Copy(snap1, snap0)
		c.Assert(err, IsNil)
		_, err = snap1.WriteAt(dataEmpty, 2*blockSize)
		c.Assert(err, IsNil)
		err = snap1.Truncate(volumeSize)
		c.Assert(err, IsNil)
		err = snap1.Close()
		c.Assert(err, IsNil)

		//delta1 file between snap1 and snap0
		delta1, err := os.Create("delta1")
		c.Assert(err, IsNil)
		_, err = delta1.WriteAt(dataEmpty, 2*blockSize)
		c.Assert(err, IsNil)
		err = delta1.Truncate(volumeSize)
		c.Assert(err, IsNil)
		err = delta1.Close()
		c.Assert(err, IsNil)

		// snap2: blk * 5
		snap2, err := os.Create(volume.Snapshots[2].Name)
		c.Assert(err, IsNil)
		_, err = io.Copy(snap2, snap0)
		c.Assert(err, IsNil)
		_, err = snap2.WriteAt(data, 4*blockSize)
		c.Assert(err, IsNil)
		err = snap2.Truncate(volumeSize)
		c.Assert(err, IsNil)
		err = snap2.Close()
		c.Assert(err, IsNil)

		//delta2 file between snap2 and snap1
		delta2, err := os.Create("delta2")
		c.Assert(err, IsNil)
		_, err = delta2.WriteAt(data, 4*blockSize)
		c.Assert(err, IsNil)
		err = delta2.Truncate(volumeSize)
		c.Assert(err, IsNil)
		err = delta2.Close()
		c.Assert(err, IsNil)

		// snap3: blk * 2 + modified blk * 1 + blk * 1
		snap3, err := os.Create(volume.Snapshots[3].Name)
		c.Assert(err, IsNil)
		_, err = io.Copy(snap3, snap0)
		c.Assert(err, IsNil)
		_, err = snap3.WriteAt(dataModified, 2*blockSize)
		c.Assert(err, IsNil)
		err = snap3.Truncate(volumeSize)
		c.Assert(err, IsNil)
		err = snap3.Close()
		c.Assert(err, IsNil)

		//delta3 file between snap3 and snap2
		delta3, err := os.Create("delta3")
		c.Assert(err, IsNil)
		_, err = delta3.WriteAt(dataModified, 2*blockSize)
		c.Assert(err, IsNil)
		err = delta3.Truncate(volumeSize)
		c.Assert(err, IsNil)
		err = delta3.Close()
		c.Assert(err, IsNil)

		// snap4 is consist of: blk * 1 + empty blk * 1 + modified blk * 1 + blk * 2
		snap4, err := os.Create(volume.Snapshots[4].Name)
		c.Assert(err, IsNil)
		_, err = io.Copy(snap4, snap0)
		c.Assert(err, IsNil)
		_, err = snap4.WriteAt(dataEmpty, 1*blockSize)
		c.Assert(err, IsNil)
		_, err = snap4.WriteAt(dataModified, 2*blockSize)
		c.Assert(err, IsNil)
		_, err = snap4.WriteAt(data, 4*blockSize)
		c.Assert(err, IsNil)
		err = snap4.Truncate(volumeSize)
		c.Assert(err, IsNil)
		err = snap4.Close()
		c.Assert(err, IsNil)

		//delta4 file between snap4 and snap3
		delta4, err := os.Create("delta4")
		c.Assert(err, IsNil)
		_, err = delta4.WriteAt(dataEmpty, 1*blockSize)
		c.Assert(err, IsNil)
		_, err = delta4.WriteAt(data, 4*blockSize)
		c.Assert(err, IsNil)
		err = delta4.Truncate(volumeSize)
		c.Assert(err, IsNil)
		err = delta4.Close()
		c.Assert(err, IsNil)

		lastBackupName := ""
		restoreIncre := filepath.Join(s.BasePath, "restore-incre-file")
		for i := 0; i < snapshotCounts; i++ {
			config := &backupstore.DeltaBackupConfig{
				Volume:          &volume.v,
				Snapshot:        &volume.Snapshots[i],
				DestURL:         s.getDestURL(),
				DeltaOps:        &volume,
				ConcurrentLimit: int32(concurrentLimit),
				Labels: map[string]string{
					"SnapshotName": volume.Snapshots[i].Name,
					"RandomKey":    "RandomValue",
				},
			}
			backup := s.createAndWaitForBackup(c, config, &volume)
			restore := filepath.Join(s.BasePath, "restore-"+strconv.Itoa(i))
			rConfig := &backupstore.DeltaRestoreConfig{
				BackupURL:       backup,
				DeltaOps:        &volume,
				Filename:        restore,
				ConcurrentLimit: int32(concurrentLimit),
			}

			err := backupstore.RestoreDeltaBlockBackup(context.Background(), rConfig)
			c.Assert(err, IsNil)
			s.waitForRestoreCompletion(c, &volume)

			err = exec.Command("diff", restore, volume.Snapshots[i].Name).Run()
			c.Assert(err, IsNil)

			rConfig = &backupstore.DeltaRestoreConfig{
				BackupURL:       backup,
				DeltaOps:        &volume,
				LastBackupName:  lastBackupName,
				Filename:        restoreIncre,
				ConcurrentLimit: int32(concurrentLimit),
			}

			err = backupstore.RestoreDeltaBlockBackupIncrementally(context.Background(), rConfig)
			if i == 0 {
				c.Assert(err, NotNil)
				c.Assert(err, ErrorMatches, "invalid parameter lastBackupName "+lastBackupName)
			} else {
				c.Assert(err, IsNil)

				s.waitForRestoreCompletion(c, &volume)
				deltaName := "delta" + strconv.Itoa(i)
				err = exec.Command("diff", restoreIncre, deltaName).Run()
				c.Assert(err, IsNil)
				os.Remove(deltaName)
			}

			backupInfo, err := backupstore.InspectBackup(backup)
			c.Assert(err, IsNil)
			lastBackupName = backupInfo.Name
			c.Assert(backupInfo.URL, Equals, backup)
			c.Assert(backupInfo.SnapshotName, Equals, volume.Snapshots[i].Name)
			c.Assert(backupInfo.SnapshotCreated, Equals, volume.Snapshots[i].CreatedTime)
			c.Assert(backupInfo.Created, Not(Equals), "")
			c.Assert(backupInfo.Labels["SnapshotName"], Equals, volume.Snapshots[i].Name)
			c.Assert(backupInfo.Labels["RandomKey"], Equals, "RandomValue")
			if i == 0 {
				c.Assert(backupInfo.IsIncremental, Equals, false)
			} else {
				c.Assert(backupInfo.IsIncremental, Equals, true)
			}
			c.Assert(backupInfo.VolumeName, Equals, volumeNameInTest)
			c.Assert(backupInfo.VolumeSize, Equals, volumeSize)
			c.Assert(backupInfo.VolumeCreated, Equals, volume.v.CreatedTime)
			c.Assert(backupInfo.VolumeBackingImageName, Equals, "")
		}
	}
}
