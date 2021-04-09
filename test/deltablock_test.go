package test

import (
	"io/ioutil"
	"math/rand"

	. "gopkg.in/check.v1"

	"github.com/longhorn/backupstore"
	_ "github.com/longhorn/backupstore/nfs"
	"github.com/longhorn/backupstore/util"
)

func (s *TestSuite) TestDeleteDeltaBlockBackup(c *C) {
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

	volumeName := "DeltaBlockTestVolume"
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
			},
		)

		err := ioutil.WriteFile(snapName, data, 0600)
		c.Assert(err, IsNil)

		s.randomChange(data, int64(0)*blockSize, 10)
	}

	backups := make(map[int]string)
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
		backups[i] = backup
	}

	// delete all backups starting from the last to test volume backup info
	// update
	for i := snapshotCounts; i > 0; i-- {
		err := backupstore.DeleteDeltaBlockBackup(backups[i-1])
		c.Assert(err, IsNil)
	}
}
