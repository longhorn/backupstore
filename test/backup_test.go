package test

import (
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	//"github.com/sirupsen/logrus"
	"github.com/rancher/backupstore"
	_ "github.com/rancher/backupstore/nfs"
	"github.com/rancher/backupstore/util"
	. "gopkg.in/check.v1"
)

const (
	volumeName        = "BackupStoreTestVolume"
	volumeName2       = "BackupStoreExtraTestVolume"
	volumeContentSize = int64(5 * 2 * 1024 * 1024)       // snapshotCounts number of blocks
	volumeSize        = int64((5 + 4) * 2 * 1024 * 1024) // snapshotCounts number of blocks + intented empty block
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
	lock           sync.Mutex
	v              backupstore.Volume
	Snapshots      []backupstore.Snapshot
	BackupProgress int
	BackupError    string
	BackupURL      string
}

func (r *RawFileVolume) UpdateBackupStatus(id, volumeID string, backupProgress int, backupURL string, backupError string) error {
	r.lock.Lock()
	r.BackupProgress = backupProgress
	r.BackupURL = backupURL
	r.BackupError = backupError
	r.lock.Unlock()
	return nil
}

func (r *RawFileVolume) GetBackupStatus() (string, string) {
	r.lock.Lock()
	bUrl := r.BackupURL
	bErr := r.BackupError
	r.lock.Unlock()
	return bUrl, bErr
}

func (r *RawFileVolume) ResetBackupStatus() {
	r.lock.Lock()
	r.BackupURL = ""
	r.BackupError = ""
	r.lock.Unlock()
}

func (r *RawFileVolume) HasSnapshot(id, volumeID string) bool {
	_, ok := os.Stat(id)
	return ok == nil
}

func (r *RawFileVolume) CompareSnapshot(id, compareID, volumeID string) (*backupstore.Mappings, error) {
	mappings := backupstore.Mappings{
		Mappings:  []backupstore.Mapping{},
		BlockSize: backupstore.DEFAULT_BLOCK_SIZE,
	}

	snap1, err := os.Open(id)
	defer snap1.Close()
	if err != nil {
		fmt.Println("Fail to open", id)
		return nil, err
	}

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
				mappings.Mappings = append(mappings.Mappings, backupstore.Mapping{
					Offset: offset,
					Size:   blockSize,
				})
			}
		}
		return &mappings, nil
	}

	snap2, err := os.Open(compareID)
	defer snap2.Close()
	if err != nil {
		fmt.Println("Fail to open", compareID)
		return nil, err
	}

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
			mappings.Mappings = append(mappings.Mappings, backupstore.Mapping{
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

	if _, err := f.ReadAt(data, start); err != nil {
		return err
	}
	return nil
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
	//logrus.SetLevel(logrus.DebugLevel)
	rand.Seed(time.Now().UTC().UnixNano())

	dir, err := ioutil.TempDir("", "backupstore-test")
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
	return "nfs://127.0.0.1:/opt/backupstore"
}

func (s *TestSuite) createAndWaitForBackup(c *C, config *backupstore.DeltaBackupConfig, deltaOps *RawFileVolume) string {
	_, err := backupstore.CreateDeltaBlockBackup(config)
	c.Assert(err, IsNil)

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

func (s *TestSuite) TestBackupBasic(c *C) {
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
			Name:        volumeName,
			Size:        volumeSize,
			CreatedTime: util.Now(),
		},
	}
	// Each snapshot will be one more block different from before
	for i := 0; i < snapshotCounts; i++ {
		snapName := s.getSnapshotName("snapshot-", i)
		volume.Snapshots = append(volume.Snapshots,
			backupstore.Snapshot{
				Name:        snapName,
				CreatedTime: util.Now(),
			})

		err := ioutil.WriteFile(snapName, data, 0600)
		c.Assert(err, IsNil)

		s.randomChange(data, int64(i)*blockSize, 10)
	}

	backup0 := ""
	for i := 0; i < snapshotCounts; i++ {
		config := &backupstore.DeltaBackupConfig{
			Volume:   &volume.v,
			Snapshot: &volume.Snapshots[i],
			DestURL:  s.getDestURL(),
			DeltaOps: &volume,
			Labels: map[string]string{
				"SnapshotName": volume.Snapshots[i].Name,
				"RandomKey":    "RandomValue",
			},
		}
		backup := s.createAndWaitForBackup(c, config, &volume)
		if i == 0 {
			backup0 = backup
		}

		restore := filepath.Join(s.BasePath, "restore-"+strconv.Itoa(i))
		err := backupstore.RestoreDeltaBlockBackup(backup, restore)
		c.Assert(err, IsNil)

		err = exec.Command("diff", volume.Snapshots[i].Name, restore).Run()
		c.Assert(err, IsNil)

		backupInfo, err := backupstore.InspectBackup(backup)
		c.Assert(err, IsNil)

		c.Assert(backupInfo.URL, Equals, backup)
		c.Assert(backupInfo.SnapshotName, Equals, volume.Snapshots[i].Name)
		c.Assert(backupInfo.SnapshotCreated, Equals, volume.Snapshots[i].CreatedTime)
		c.Assert(backupInfo.Created, Not(Equals), "")
		c.Assert(backupInfo.Size, Equals, volumeContentSize)
		c.Assert(backupInfo.VolumeName, Equals, volumeName)
		c.Assert(backupInfo.VolumeSize, Equals, volumeSize)
		c.Assert(backupInfo.VolumeCreated, Equals, volume.v.CreatedTime)
		c.Assert(backupInfo.Labels["SnapshotName"], Equals, volume.Snapshots[i].Name)
		c.Assert(backupInfo.Labels["RandomKey"], Equals, "RandomValue")
	}

	listInfo, err := backupstore.List(volume.v.Name, s.getDestURL(), false)
	c.Assert(err, IsNil)
	c.Assert(len(listInfo), Equals, 1)
	volumeInfo, ok := listInfo[volume.v.Name]
	c.Assert(ok, Equals, true)
	c.Assert(volumeInfo.Name, Equals, volume.v.Name)
	c.Assert(volumeInfo.Size, Equals, volumeSize)
	c.Assert(volumeInfo.Created, Equals, volume.v.CreatedTime)
	c.Assert(volumeInfo.DataStored, Equals, int64(snapshotCounts*backupstore.DEFAULT_BLOCK_SIZE))
	c.Assert(len(volumeInfo.Backups), Equals, snapshotCounts)

	backupInfo0, ok := volumeInfo.Backups[backup0]
	c.Assert(backupInfo0.URL, Equals, backup0)
	c.Assert(backupInfo0.SnapshotName, Equals, volume.Snapshots[0].Name)
	c.Assert(backupInfo0.SnapshotCreated, Equals, volume.Snapshots[0].CreatedTime)
	c.Assert(backupInfo0.Created, Not(Equals), "")
	c.Assert(backupInfo0.Size, Equals, volumeContentSize)
	//Because it's in volume list so volume specific details are omitted
	c.Assert(backupInfo0.VolumeName, Equals, "")
	c.Assert(backupInfo0.VolumeSize, Equals, int64(0))
	c.Assert(backupInfo0.VolumeCreated, Equals, "")
	c.Assert(backupInfo0.Labels["SnapshotName"], Equals, volume.Snapshots[0].Name)
	c.Assert(backupInfo0.Labels["RandomKey"], Equals, "RandomValue")

	volumeList, err := backupstore.List(volume.v.Name, s.getDestURL(), true)
	c.Assert(err, IsNil)
	c.Assert(len(volumeList), Equals, 1)
	volumeInfo, ok = volumeList[volume.v.Name]
	c.Assert(ok, Equals, true)
	c.Assert(volumeInfo.Name, Equals, volume.v.Name)
	c.Assert(volumeInfo.Size, Equals, volumeSize)
	c.Assert(volumeInfo.Created, Equals, volume.v.CreatedTime)
	c.Assert(len(volumeInfo.Backups), Equals, 0)
}

func (s *TestSuite) TestBackupRestoreExtra(c *C) {
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
			Name:        volumeName2,
			Size:        volumeSize,
			CreatedTime: util.Now(),
		},
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

	lastBackupName := ""
	restoreIncre := filepath.Join(s.BasePath, "restore-incre-file")
	for i := 0; i < snapshotCounts; i++ {
		config := &backupstore.DeltaBackupConfig{
			Volume:   &volume.v,
			Snapshot: &volume.Snapshots[i],
			DestURL:  s.getDestURL(),
			DeltaOps: &volume,
			Labels: map[string]string{
				"SnapshotName": volume.Snapshots[i].Name,
				"RandomKey":    "RandomValue",
			},
		}
		backup := s.createAndWaitForBackup(c, config, &volume)
		restore := filepath.Join(s.BasePath, "restore-"+strconv.Itoa(i))
		err = backupstore.RestoreDeltaBlockBackup(backup, restore)
		c.Assert(err, IsNil)

		err = exec.Command("diff", restore, volume.Snapshots[i].Name).Run()
		c.Assert(err, IsNil)

		err = backupstore.RestoreDeltaBlockBackupIncrementally(backup, restoreIncre, lastBackupName)
		if i == 0 {
			c.Assert(err, NotNil)
			c.Assert(err, ErrorMatches, "Invalid parameter lastBackupName "+lastBackupName)

			err = os.Rename(restore, restoreIncre)
			c.Assert(err, IsNil)
		} else {
			c.Assert(err, IsNil)

			err = exec.Command("diff", restoreIncre, volume.Snapshots[i].Name).Run()
			c.Assert(err, IsNil)
		}

		backupInfo, err := backupstore.InspectBackup(backup)
		c.Assert(err, IsNil)
		lastBackupName = backupInfo.Name

		c.Assert(backupInfo.URL, Equals, backup)
		c.Assert(backupInfo.SnapshotName, Equals, volume.Snapshots[i].Name)
		c.Assert(backupInfo.SnapshotCreated, Equals, volume.Snapshots[i].CreatedTime)
		c.Assert(backupInfo.Created, Not(Equals), "")
		c.Assert(backupInfo.VolumeName, Equals, volumeName2)
		c.Assert(backupInfo.VolumeSize, Equals, volumeSize)
		c.Assert(backupInfo.VolumeCreated, Equals, volume.v.CreatedTime)
		c.Assert(backupInfo.Labels["SnapshotName"], Equals, volume.Snapshots[i].Name)
		c.Assert(backupInfo.Labels["RandomKey"], Equals, "RandomValue")
	}
}
