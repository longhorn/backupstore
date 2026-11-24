package backupstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/longhorn/backupstore/util"
)

// TestEncryptedBlockReadPath drives an encrypted block through the real restore read path,
// readBlockWithFallback, rather than calling the cipher directly.
func TestEncryptedBlockReadPath(t *testing.T) {
	driver := newMemDriver()
	cfg := newTestEncryptionConfig(t, "secret-a")
	volume := &Volume{Name: "vol"}
	blkCipher, err := reconcileBackupEncryption(driver, volume, cfg)
	require.NoError(t, err)

	block := make([]byte, 2*1024*1024)
	_, _ = rand.Read(block)
	checksum := util.GetChecksum(block)
	blkFile := getBlockFilePath("vol", checksum)

	for _, method := range []string{"none", "lz4", "gzip"} {
		rs, err := util.CompressData(method, block)
		require.NoError(t, err, method)
		rs, err = blkCipher.encrypt(rs)
		require.NoError(t, err, method)
		require.NoError(t, driver.Write(blkFile, rs), method)

		r, err := readBlockWithFallback(context.Background(), driver, blkFile, method, checksum, blkCipher)
		require.NoError(t, err, "%s: encrypted block must restore", method)
		got, err := io.ReadAll(r)
		require.NoError(t, err, method)
		assert.True(t, bytes.Equal(block, got), "%s: restored data must match", method)
	}

	// A tampered block fails immediately as an authentication error, not after minutes of retries.
	stored := driver.files[blkFile]
	stored[len(stored)/2] ^= 0x01
	_, err = readBlockWithFallback(context.Background(), driver, blkFile, "gzip", checksum, blkCipher)
	assert.True(t, errors.Is(err, ErrBackupDecryptionFailed), "tampered block: %v", err)
}
