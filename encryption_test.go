package backupstore

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/longhorn/backupstore/types"
)

// memDriver is an in-memory BackupStoreDriver that persists writes, used to exercise the
// encryption metadata and block paths without a real backup target.
type memDriver struct {
	files map[string][]byte
}

func newMemDriver() *memDriver { return &memDriver{files: map[string][]byte{}} }

func (m *memDriver) Kind() string                   { return "mem" }
func (m *memDriver) GetURL() string                 { return "mem://localhost" }
func (m *memDriver) FileTime(string) time.Time      { return time.Now() }
func (m *memDriver) Upload(string, string) error    { return nil }
func (m *memDriver) Download(string, string) error  { return nil }
func (m *memDriver) FileSize(filePath string) int64 { return int64(len(m.files[filePath])) }

func (m *memDriver) FileExists(filePath string) bool {
	if _, ok := m.files[filePath]; ok {
		return true
	}
	prefix := strings.TrimSuffix(filePath, "/") + "/"
	for name := range m.files {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

func (m *memDriver) Remove(path string) error {
	prefix := strings.TrimSuffix(path, "/")
	for name := range m.files {
		if name == prefix || strings.HasPrefix(name, prefix+"/") {
			delete(m.files, name)
		}
	}
	return nil
}

func (m *memDriver) Read(src string) (io.ReadCloser, error) {
	data, ok := m.files[src]
	if !ok {
		return nil, fmt.Errorf("%v not found", src)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *memDriver) Write(dst string, rs io.ReadSeeker) error {
	data, err := io.ReadAll(rs)
	if err != nil {
		return err
	}
	m.files[dst] = data
	return nil
}

func (m *memDriver) List(listPath string) ([]string, error) {
	prefix := strings.TrimSuffix(listPath, "/") + "/"
	seen := map[string]struct{}{}
	for name := range m.files {
		if rest, ok := strings.CutPrefix(name, prefix); ok {
			seen[strings.SplitN(rest, "/", 2)[0]] = struct{}{}
		}
	}
	if len(seen) == 0 {
		return nil, fmt.Errorf("%v not found", listPath)
	}
	names := []string{}
	for name := range seen {
		names = append(names, name)
	}
	return names, nil
}

func newTestEncryptionConfig(t *testing.T, secretName string) *EncryptionConfig {
	t.Helper()
	key := make([]byte, encryptionKeySize)
	_, err := rand.Read(key)
	require.NoError(t, err)
	cfg, err := NewEncryptionConfig(map[string]string{
		types.BackupEncryptionSecret: secretName,
		types.BackupEncryptionKey:    base64.StdEncoding.EncodeToString(key),
	})
	require.NoError(t, err)
	return cfg
}

func TestNewEncryptionConfig(t *testing.T) {
	validKey := base64.StdEncoding.EncodeToString(make([]byte, encryptionKeySize))
	shortKey := base64.StdEncoding.EncodeToString(make([]byte, 16))

	cfg, err := NewEncryptionConfig(nil)
	assert.NoError(t, err)
	assert.Nil(t, cfg, "no Secret means no encryption")

	cfg, err = NewEncryptionConfig(map[string]string{types.BackupEncryptionSecret: "s", types.BackupEncryptionKey: validKey})
	assert.NoError(t, err)
	assert.Equal(t, "s", cfg.SecretName)

	for name, credential := range map[string]map[string]string{
		"missing key":    {types.BackupEncryptionSecret: "s"},
		"malformed key":  {types.BackupEncryptionSecret: "s", types.BackupEncryptionKey: "not-base64!"},
		"wrong length":   {types.BackupEncryptionSecret: "s", types.BackupEncryptionKey: shortKey},
		"key, no secret": {types.BackupEncryptionKey: validKey},
	} {
		_, err := NewEncryptionConfig(credential)
		assert.Error(t, err, name)
		assert.NotContains(t, err.Error(), validKey, "%s: error must not contain key material", name)
	}
}

func TestEncryptionConfigFormattingHidesKey(t *testing.T) {
	cfg := newTestEncryptionConfig(t, "s")
	for _, out := range []string{fmt.Sprintf("%v", cfg), fmt.Sprintf("%+v", cfg), fmt.Sprintf("%#v", cfg)} {
		assert.NotContains(t, out, string(cfg.kek))
		assert.NotContains(t, out, base64.StdEncoding.EncodeToString(cfg.kek))
	}
}

func TestBlockCipherRoundTripAndTamper(t *testing.T) {
	_, blkCipher, err := newEncryptionKeyFile(newTestEncryptionConfig(t, "s"))
	require.NoError(t, err)

	big := make([]byte, 2*1024*1024+123) // multiple DARE packages
	_, _ = rand.Read(big)
	pattern := bytes.Repeat([]byte("LONGHORN-PLAINTEXT-PATTERN"), 4096)

	for name, plaintext := range map[string][]byte{"small": []byte("hello"), "block": big, "pattern": pattern} {
		rs, err := blkCipher.encrypt(bytes.NewReader(plaintext))
		require.NoError(t, err, name)
		ciphertext, err := io.ReadAll(rs)
		require.NoError(t, err, name)
		assert.False(t, bytes.Contains(ciphertext, []byte("LONGHORN-PLAINTEXT-PATTERN")), "%s: plaintext leaked", name)

		got, err := blkCipher.decrypt(ciphertext)
		require.NoError(t, err, name)
		assert.True(t, bytes.Equal(plaintext, got), name)

		modified := bytes.Clone(ciphertext)
		modified[len(modified)/2] ^= 0x01
		_, err = blkCipher.decrypt(modified)
		assert.True(t, errors.Is(err, ErrBackupDecryptionFailed), "%s: modified", name)

		_, err = blkCipher.decrypt(ciphertext[:len(ciphertext)-1])
		assert.True(t, errors.Is(err, ErrBackupDecryptionFailed), "%s: truncated", name)
	}

	_, otherCipher, err := newEncryptionKeyFile(newTestEncryptionConfig(t, "s"))
	require.NoError(t, err)

	// DARE cannot authenticate an empty stream: never write one, never accept one.
	_, err = blkCipher.encrypt(bytes.NewReader(nil))
	assert.ErrorContains(t, err, "refusing to encrypt an empty block")
	_, err = blkCipher.decrypt(nil)
	assert.True(t, errors.Is(err, ErrBackupDecryptionFailed), "empty ciphertext")

	rs, err := blkCipher.encrypt(bytes.NewReader([]byte("data")))
	require.NoError(t, err)
	ciphertext, _ := io.ReadAll(rs)
	_, err = otherCipher.decrypt(ciphertext)
	assert.True(t, errors.Is(err, ErrBackupDecryptionFailed), "wrong DEK")
}

func TestEncryptionKeyFileUnwrap(t *testing.T) {
	cfg := newTestEncryptionConfig(t, "secret-a")
	keyFile, _, err := newEncryptionKeyFile(cfg)
	require.NoError(t, err)

	_, err = keyFile.unwrap(cfg)
	require.NoError(t, err)

	_, err = keyFile.unwrap(newTestEncryptionConfig(t, "secret-a"))
	assert.ErrorContains(t, err, "does not match the key used for this backup", "replaced key material")

	_, err = keyFile.unwrap(newTestEncryptionConfig(t, "secret-b"))
	assert.ErrorContains(t, err, `required backup encryption secret "secret-a" is unavailable`)

	_, err = keyFile.unwrap(nil)
	assert.ErrorContains(t, err, "is unavailable")

	newer := *keyFile
	newer.FormatVersion = EncryptionFormatVersion + 1
	_, err = newer.unwrap(cfg)
	assert.ErrorContains(t, err, "unsupported backup encryption format version")

	// Editing authenticated data must fail the unwrap rather than change behavior.
	edited := *keyFile
	wrapped, _ := base64.StdEncoding.DecodeString(edited.WrappedDEK)
	wrapped[0] ^= 0x01
	edited.WrappedDEK = base64.StdEncoding.EncodeToString(wrapped)
	_, err = edited.unwrap(cfg)
	assert.True(t, errors.Is(err, ErrBackupDecryptionFailed), "modified wrapped DEK")
}

func TestReconcileBackupEncryption(t *testing.T) {
	cfgA := newTestEncryptionConfig(t, "secret-a")
	cfgB := newTestEncryptionConfig(t, "secret-b")

	t.Run("fresh volume without Secret stays unencrypted", func(t *testing.T) {
		driver := newMemDriver()
		volume := &Volume{Name: "vol"}
		c, err := reconcileBackupEncryption(driver, volume, nil)
		assert.NoError(t, err)
		assert.Nil(t, c)
		assert.Nil(t, volume.Encryption)
	})

	t.Run("fresh volume with Secret is established before any block", func(t *testing.T) {
		driver := newMemDriver()
		volume := &Volume{Name: "vol"}
		c, err := reconcileBackupEncryption(driver, volume, cfgA)
		require.NoError(t, err)
		require.NotNil(t, c)
		assert.Equal(t, "secret-a", volume.Encryption.SecretName())
		assert.True(t, driver.FileExists(getEncryptionKeyFilePath("vol")))

		stored, err := loadVolume(driver, "vol")
		require.NoError(t, err)
		assert.Equal(t, "secret-a", stored.Encryption.SecretName(), "state recorded in volume.cfg")

		// The same Secret reopens the same DEK, so incremental blocks stay readable.
		c2, err := reconcileBackupEncryption(driver, stored, cfgA)
		require.NoError(t, err)
		assert.Equal(t, c.dek, c2.dek)

		_, err = reconcileBackupEncryption(driver, stored, cfgB)
		assert.ErrorContains(t, err, `cannot change from "secret-a"`)
		_, err = reconcileBackupEncryption(driver, stored, nil)
		assert.ErrorContains(t, err, `cannot change from "secret-a"`)
	})

	t.Run("volume with history stays unencrypted", func(t *testing.T) {
		driver := newMemDriver()
		_, err := reconcileBackupEncryption(driver, &Volume{Name: "vol", LastBackupName: "backup-1"}, cfgA)
		assert.ErrorContains(t, err, "cannot change from unencrypted")

		c, err := reconcileBackupEncryption(driver, &Volume{Name: "vol", LastBackupName: "backup-1"}, nil)
		assert.NoError(t, err)
		assert.Nil(t, c)
	})

	t.Run("blocks left by a failed unencrypted backup block encryption", func(t *testing.T) {
		driver := newMemDriver()
		driver.files[getBlockFilePath("vol", strings.Repeat("ab", 32))] = []byte("plaintext block")
		_, err := reconcileBackupEncryption(driver, &Volume{Name: "vol"}, cfgA)
		assert.ErrorContains(t, err, "cannot change from unencrypted")
	})
}

func TestLoadBackupCipherDecidesFromMetadata(t *testing.T) {
	driver := newMemDriver()
	cfgA := newTestEncryptionConfig(t, "secret-a")
	volume := &Volume{Name: "vol"}
	_, err := reconcileBackupEncryption(driver, volume, cfgA)
	require.NoError(t, err)

	c, err := loadBackupCipher(driver, "vol", nil, cfgA)
	assert.NoError(t, err)
	assert.Nil(t, c, "an unencrypted backup is never decrypted, even when a key is supplied")

	_, err = loadBackupCipher(driver, "vol", volume.Encryption, nil)
	assert.ErrorContains(t, err, `required backup encryption secret "secret-a" is unavailable`)

	c, err = loadBackupCipher(driver, "vol", volume.Encryption, cfgA)
	assert.NoError(t, err)
	assert.NotNil(t, c)

	_, err = loadBackupCipher(driver, "vol", &BackupEncryption{FormatVersion: EncryptionFormatVersion + 1, Secret: "secret-a"}, cfgA)
	assert.ErrorContains(t, err, "unsupported backup encryption format version")
}
