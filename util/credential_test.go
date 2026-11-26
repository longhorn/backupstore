package util

import (
	"os"
	"testing"

	"github.com/longhorn/backupstore/types"
)

func TestGetBackupCredentialForwardsEncryptionEnv(t *testing.T) {
	t.Setenv(types.BackupEncryptionSecret, "longhorn-backup-encryption")
	t.Setenv(types.BackupEncryptionKey, "dGVzdC1rZXktMzItYnl0ZXMtZXhhbXBsZQ==")
	// Clear storage-provider vars so S3 collection stays empty aside from optional blanks.
	for _, key := range []string{
		types.AWSAccessKey, types.AWSSecretKey, types.AWSEndPoint, types.AWSCert,
		types.HTTPSProxy, types.HTTPProxy, types.NOProxy, types.VirtualHostedStyle,
		types.AWSSignAcceptEncoding,
	} {
		t.Setenv(key, "")
	}

	for _, backupURL := range []string{
		"s3://bucket@us-east-1/",
		"nfs://nfs-server.example.com:/backups",
		"cifs://cifs-server.example.com/backups",
		"azblob://container@account/",
	} {
		t.Run(backupURL, func(t *testing.T) {
			credential, err := GetBackupCredential(backupURL)
			if err != nil {
				t.Fatalf("GetBackupCredential(%q): %v", backupURL, err)
			}
			if got := credential[types.BackupEncryptionSecret]; got != "longhorn-backup-encryption" {
				t.Fatalf("secret = %q, want longhorn-backup-encryption", got)
			}
			if got := credential[types.BackupEncryptionKey]; got != "dGVzdC1rZXktMzItYnl0ZXMtZXhhbXBsZQ==" {
				t.Fatalf("key = %q, want encryption key from env", got)
			}
		})
	}
}

func TestGetBackupCredentialOmitsUnsetEncryptionEnv(t *testing.T) {
	os.Unsetenv(types.BackupEncryptionSecret)
	os.Unsetenv(types.BackupEncryptionKey)

	credential, err := GetBackupCredential("nfs://nfs-server.example.com:/backups")
	if err != nil {
		t.Fatalf("GetBackupCredential: %v", err)
	}
	if _, ok := credential[types.BackupEncryptionSecret]; ok {
		t.Fatal("unexpected encryption secret in credential map")
	}
	if _, ok := credential[types.BackupEncryptionKey]; ok {
		t.Fatal("unexpected encryption key in credential map")
	}
}
