package test

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	//"github.com/Sirupsen/logrus"
	"github.com/rancher/backupstore"
	_ "github.com/rancher/backupstore/nfs"
	"github.com/rancher/backupstore/util"
	. "gopkg.in/check.v1"
)

const (
	volumeName        = "BackupStoreTestVolume"
	volumeContentSize = int64(5 * 2 * 1024 * 1024)       // snapshotCounts number of blocks
	volumeSize        = int64((5 + 4) * 2 * 1024 * 1024) // snapshotCounts number of blocks + intented empty block
	snapPrefix        = "volume_snap"
	snapshotCounts    = 5
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
	BasePath        string
	BackupStorePath string
	Volume          RawFileVolume
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
	v         backupstore.Volume
	Snapshots []backupstore.Snapshot
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
		for i := int64(0); i < volumeContentSize/blockSize; i++ {
			mappings.Mappings = append(mappings.Mappings, backupstore.Mapping{
				Offset: i * blockSize,
				Size:   blockSize,
			})
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

func (s *TestSuite) getSnapshotName(i int) string {
	return filepath.Join(s.BasePath, "snapshot-"+strconv.Itoa(i))
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

	s.Volume = RawFileVolume{
		v: backupstore.Volume{
			Name:        volumeName,
			Size:        volumeSize,
			CreatedTime: util.Now(),
		},
	}
	// Each snapshot will be one more block different from before
	for i := 0; i < snapshotCounts; i++ {
		snapName := s.getSnapshotName(i)
		s.Volume.Snapshots = append(s.Volume.Snapshots,
			backupstore.Snapshot{
				Name:        snapName,
				CreatedTime: util.Now(),
			})

		err = ioutil.WriteFile(snapName, data, 0600)
		c.Assert(err, IsNil)

		s.randomChange(data, int64(i)*blockSize, 10)
	}
}

func (s *TestSuite) TearDownSuite(c *C) {
	err := exec.Command("rm", "-rf", s.BasePath).Run()
	c.Assert(err, IsNil)
}

func (s *TestSuite) getDestURL() string {
	//return "vfs://" + s.BackupStorePath
	return "nfs://127.0.0.1:/opt/backupstore"
}

func (s *TestSuite) TestBackupBasic(c *C) {
	backup0 := ""
	for i := 0; i < snapshotCounts; i++ {
		config := &backupstore.DeltaBackupConfig{
			Volume:   &s.Volume.v,
			Snapshot: &s.Volume.Snapshots[i],
			DestURL:  s.getDestURL(),
			DeltaOps: &s.Volume,
			Labels: map[string]string{
				"SnapshotName": s.Volume.Snapshots[i].Name,
				"RandomKey":    "RandomValue",
			},
		}
		backup, err := backupstore.CreateDeltaBlockBackup(config)
		if i == 0 {
			backup0 = backup
		}
		c.Assert(err, IsNil)

		restore := filepath.Join(s.BasePath, "restore-"+strconv.Itoa(i))
		err = backupstore.RestoreDeltaBlockBackup(backup, restore)
		c.Assert(err, IsNil)

		err = exec.Command("diff", s.Volume.Snapshots[i].Name, restore).Run()
		c.Assert(err, IsNil)

		backupInfo, err := backupstore.InspectBackup(backup)
		c.Assert(err, IsNil)

		c.Assert(backupInfo.URL, Equals, backup)
		c.Assert(backupInfo.SnapshotName, Equals, s.Volume.Snapshots[i].Name)
		c.Assert(backupInfo.SnapshotCreated, Equals, s.Volume.Snapshots[i].CreatedTime)
		c.Assert(backupInfo.Created, Not(Equals), "")
		c.Assert(backupInfo.Size, Equals, volumeContentSize)
		c.Assert(backupInfo.VolumeName, Equals, volumeName)
		c.Assert(backupInfo.VolumeSize, Equals, volumeSize)
		c.Assert(backupInfo.VolumeCreated, Equals, s.Volume.v.CreatedTime)
		c.Assert(backupInfo.Labels["SnapshotName"], Equals, s.Volume.Snapshots[i].Name)
		c.Assert(backupInfo.Labels["RandomKey"], Equals, "RandomValue")
	}

	listInfo, err := backupstore.List(s.Volume.v.Name, s.getDestURL(), false)
	c.Assert(err, IsNil)
	c.Assert(len(listInfo), Equals, 1)
	volumeInfo, ok := listInfo[s.Volume.v.Name]
	c.Assert(ok, Equals, true)
	c.Assert(volumeInfo.Name, Equals, s.Volume.v.Name)
	c.Assert(volumeInfo.Size, Equals, volumeSize)
	c.Assert(volumeInfo.Created, Equals, s.Volume.v.CreatedTime)
	c.Assert(volumeInfo.SpaceUsage, Equals, int64(snapshotCounts*backupstore.DEFAULT_BLOCK_SIZE))
	c.Assert(len(volumeInfo.Backups), Equals, snapshotCounts)

	backupInfo0, ok := volumeInfo.Backups[backup0]
	c.Assert(backupInfo0.URL, Equals, backup0)
	c.Assert(backupInfo0.SnapshotName, Equals, s.Volume.Snapshots[0].Name)
	c.Assert(backupInfo0.SnapshotCreated, Equals, s.Volume.Snapshots[0].CreatedTime)
	c.Assert(backupInfo0.Created, Not(Equals), "")
	c.Assert(backupInfo0.Size, Equals, volumeContentSize)
	//Because it's in volume list so volume specific details are omitted
	c.Assert(backupInfo0.VolumeName, Equals, "")
	c.Assert(backupInfo0.VolumeSize, Equals, int64(0))
	c.Assert(backupInfo0.VolumeCreated, Equals, "")
	c.Assert(backupInfo0.Labels["SnapshotName"], Equals, s.Volume.Snapshots[0].Name)
	c.Assert(backupInfo0.Labels["RandomKey"], Equals, "RandomValue")

	volumeList, err := backupstore.List(s.Volume.v.Name, s.getDestURL(), true)
	c.Assert(err, IsNil)
	c.Assert(len(volumeList), Equals, 1)
	volumeInfo, ok = volumeList[s.Volume.v.Name]
	c.Assert(ok, Equals, true)
	c.Assert(volumeInfo.Name, Equals, s.Volume.v.Name)
	c.Assert(volumeInfo.Size, Equals, volumeSize)
	c.Assert(volumeInfo.Created, Equals, s.Volume.v.CreatedTime)
	c.Assert(len(volumeInfo.Backups), Equals, 0)
}
