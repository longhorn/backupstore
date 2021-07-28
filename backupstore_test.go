package backupstore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeAndDecodeBackupURL(t *testing.T) {
	testCases := []struct {
		volumeName        string
		backupName        string
		destURL           string
		expectDecodeError bool
		expectBackupURL   string
	}{
		{
			volumeName:      "vol-1",
			destURL:         "s3://backupstore@minio/",
			expectBackupURL: "s3://backupstore@minio/?volume=vol-1",
		},
		{
			volumeName:      "vol-2",
			backupName:      "backup-2",
			destURL:         "s3://backupstore@minio/",
			expectBackupURL: "s3://backupstore@minio/?backup=backup-2&volume=vol-2",
		},
		{
			// Test invalid volume name
			volumeName:        "-3-vol",
			destURL:           "s3://backupstore@minio/",
			expectDecodeError: true,
			expectBackupURL:   "s3://backupstore@minio/?volume=-3-vol",
		},
		{
			// Test invalid backup name
			volumeName:        "vol-4",
			backupName:        "-4-backup",
			destURL:           "s3://backupstore@minio/",
			expectBackupURL:   "s3://backupstore@minio/?backup=-4-backup&volume=vol-4",
			expectDecodeError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s%s&%s", tc.destURL, tc.backupName, tc.volumeName), func(t *testing.T) {
			assert := assert.New(t)

			gotBackupURL := EncodeBackupURL(tc.backupName, tc.volumeName, tc.destURL)
			assert.Equal(gotBackupURL, tc.expectBackupURL)

			backupName, volumeName, destURL, err := DecodeBackupURL(gotBackupURL)
			if tc.expectDecodeError {
				assert.NotNil(err)
			} else {
				assert.Nil(err)
				assert.Equal(backupName, tc.backupName)
				assert.Equal(volumeName, tc.volumeName)
				assert.Equal(destURL, tc.destURL)
			}
		})
	}
}
