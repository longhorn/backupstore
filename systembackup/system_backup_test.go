package systembackup

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetSystemBackupURL(t *testing.T) {
	testCases := []struct {
		systemBackupName      string
		longhornVersion       string
		backupTarget          string
		expectSystemBackupURL string
		expectError           string
	}{
		{
			systemBackupName:      "foobar",
			longhornVersion:       "v1.4.0",
			backupTarget:          "s3://backupstore@minio",
			expectSystemBackupURL: "s3://backupstore@minio/backupstore/system-backups/v1.4.0/foobar",
		},
		{
			systemBackupName: "",
			longhornVersion:  "v1.4.0",
			backupTarget:     "s3://backupstore@minio",
			expectError:      "getting system backup URL: missing system backup name",
		},
		{
			systemBackupName: "foobar",
			longhornVersion:  "",
			backupTarget:     "s3://backupstore@minio",
			expectError:      "getting system backup URL: missing Longhorn version",
		},
		{
			backupTarget: "://backupstore@minio",
			expectError:  "parse \"://backupstore@minio\": missing protocol scheme",
		},
	}

	for _, tc := range testCases {

		t.Run(fmt.Sprintf("name=%v\nlonghornVersion=%v\nbackupTarget=%v", tc.systemBackupName, tc.longhornVersion, tc.backupTarget), func(t *testing.T) {
			assert := assert.New(t)

			systemBackupURL, err := GetSystemBackupURL(tc.systemBackupName, tc.longhornVersion, tc.backupTarget)

			if tc.expectError != "" {
				assert.NotNil(err)
				assert.Equal(tc.expectError, err.Error())
			} else {
				assert.Nil(err)
				assert.Equal(tc.expectSystemBackupURL, systemBackupURL)
			}
		})
	}
}

func TestParseSystemBackupURL(t *testing.T) {
	testCases := []struct {
		systemBackupURL        string
		expectBackupTarget     string
		expectLonghornVersion  string
		expectSystemBackupName string
		expectError            string
	}{
		{
			systemBackupURL:        "s3://backupstore@minio/backupstore/system-backups/v1.4.0/foobar",
			expectBackupTarget:     "s3://backupstore@minio/",
			expectLonghornVersion:  "v1.4.0",
			expectSystemBackupName: "foobar",
		},
		{
			systemBackupURL: "s3://backupstore@minio/system-backups/v1.4.0/foobar",
			expectError:     "invalid system backup URI: /system-backups/v1.4.0/foobar",
		},
		{
			systemBackupURL: "s3://backupstore@minio/backupstore/invalid/v1.4.0/foobar",
			expectError:     "invalid system backup URL subdirectory invalid: s3://backupstore@minio/backupstore/invalid/v1.4.0/foobar",
		},
		{
			systemBackupURL: "s3://backupstore@minio/invalid/system-backups/v1.4.0/foobar",
			expectError:     "invalid system backup URL backupstore directory invalid: s3://backupstore@minio/invalid/system-backups/v1.4.0/foobar",
		},
	}

	for _, tc := range testCases {

		t.Run(tc.systemBackupURL, func(t *testing.T) {
			assert := assert.New(t)

			backupTarget, longhornVersion, systemBackupName, err := ParseSystemBackupURL(tc.systemBackupURL)

			if tc.expectError != "" {
				assert.NotNil(err)
				assert.Equal(tc.expectError, err.Error())
			} else {
				assert.Nil(err)
				assert.Equal(tc.expectBackupTarget, backupTarget)
				assert.Equal(tc.expectLonghornVersion, longhornVersion)
				assert.Equal(tc.expectSystemBackupName, systemBackupName)
			}
		})
	}
}
