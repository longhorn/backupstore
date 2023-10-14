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
		expectDecodeURL   string
	}{
		{
			volumeName:      "vol-1",
			destURL:         "s3://backupstore@minio/",
			expectBackupURL: "s3://backupstore@minio/?volume=vol-1",
			expectDecodeURL: "s3://backupstore@minio/",
		},
		{
			volumeName:      "vol-2",
			backupName:      "backup-2",
			destURL:         "s3://backupstore@minio/",
			expectBackupURL: "s3://backupstore@minio/?backup=backup-2&volume=vol-2",
			expectDecodeURL: "s3://backupstore@minio/",
		},
		{
			// Test invalid volume name
			volumeName:        "-3-vol",
			destURL:           "s3://backupstore@minio/",
			expectBackupURL:   "s3://backupstore@minio/?volume=-3-vol",
			expectDecodeError: true,
		},
		{
			// Test invalid backup name
			volumeName:        "vol-4",
			backupName:        "-4-backup",
			destURL:           "s3://backupstore@minio/",
			expectBackupURL:   "s3://backupstore@minio/?backup=-4-backup&volume=vol-4",
			expectDecodeError: true,
		},
		{
			// Test NFS target with no mount options.
			volumeName:      "vol-5",
			backupName:      "backup-5",
			destURL:         "nfs://longhorn-test-nfs-svc.default:/opt/backupstore",
			expectBackupURL: "nfs://longhorn-test-nfs-svc.default:/opt/backupstore?backup=backup-5&volume=vol-5",
			expectDecodeURL: "nfs://longhorn-test-nfs-svc.default:/opt/backupstore",
		},
		{
			// Test NFS target with mount options (dropped by DecodeBackupURL.)
			volumeName:      "vol-6",
			backupName:      "backup-6",
			destURL:         "nfs://longhorn-test-nfs-svc.default:/opt/backupstore?nfsOptions=soft,timeo=150,retrans=3",
			expectBackupURL: "nfs://longhorn-test-nfs-svc.default:/opt/backupstore?backup=backup-6&volume=vol-6",
			expectDecodeURL: "nfs://longhorn-test-nfs-svc.default:/opt/backupstore",
		},
		{
			// Test NFS target with empty Query tag.
			volumeName:      "vol-7",
			backupName:      "backup-7",
			destURL:         "nfs://longhorn-test-nfs-svc.default:/opt/backupstore?",
			expectBackupURL: "nfs://longhorn-test-nfs-svc.default:/opt/backupstore?backup=backup-7&volume=vol-7",
			expectDecodeURL: "nfs://longhorn-test-nfs-svc.default:/opt/backupstore",
		},
		{
			// Test NFS target with empty mount options.
			volumeName:      "vol-8",
			backupName:      "backup-8",
			destURL:         "nfs://longhorn-test-nfs-svc.default:/opt/backupstore?nfsOptions=",
			expectBackupURL: "nfs://longhorn-test-nfs-svc.default:/opt/backupstore?backup=backup-8&volume=vol-8",
			expectDecodeURL: "nfs://longhorn-test-nfs-svc.default:/opt/backupstore",
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
				assert.Equal(destURL, tc.expectDecodeURL)
			}
		})
	}
}
