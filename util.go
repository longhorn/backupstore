package backupstore

import (
	"compress/gzip"
	"context"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/cockroachdb/errors"

	"github.com/longhorn/backupstore/util"

	lhbackup "github.com/longhorn/go-common-libs/backup"
)

func getBlockPath(volumeName string) string {
	return filepath.Join(getVolumePath(volumeName), BLOCKS_DIRECTORY) + "/"
}

func getBlockFilePath(volumeName, checksum string) string {
	blockSubDirLayer1 := checksum[0:BLOCK_SEPARATE_LAYER1]
	blockSubDirLayer2 := checksum[BLOCK_SEPARATE_LAYER1:BLOCK_SEPARATE_LAYER2]
	path := filepath.Join(getBlockPath(volumeName), blockSubDirLayer1, blockSubDirLayer2)
	fileName := checksum + BLK_SUFFIX

	return filepath.Join(path, fileName)
}

// mergeErrorChannels will merge all error channels into a single error out channel.
// the error out channel will be closed once the ctx is done or all error channels are closed
// if there is an error on one of the incoming channels the error will be relayed.
func mergeErrorChannels(ctx context.Context, channels ...<-chan error) <-chan error {
	var wg sync.WaitGroup
	wg.Add(len(channels))

	out := make(chan error, len(channels))
	output := func(c <-chan error) {
		defer wg.Done()
		select {
		case err, ok := <-c:
			if ok {
				out <- err
			}
			return
		case <-ctx.Done():
			return
		}
	}

	for _, c := range channels {
		go output(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

var backoffDuration = [...]time.Duration{
	time.Second,
	5 * time.Second,
	30 * time.Second,
}

// DecompressAndVerifyWithFallback decompresses the given data and verifies the data integrity.
// If the decompression fails, it will try to decompress with the fallback method.
func DecompressAndVerifyWithFallback(bsDriver BackupStoreDriver, blkFile, decompression, checksum string) (io.Reader, error) {
	attempts := 0
	var lastErr error
	for {
		rc, err := bsDriver.Read(blkFile)
		if err != nil {
			lastErr = err
			if attempts < len(backoffDuration) {
				time.Sleep(backoffDuration[attempts])
				attempts++
				continue
			}
			return nil, errors.Wrapf(lastErr, "failed to read block %v after %d attempts", blkFile, attempts+1)
		}

		r, err := util.DecompressAndVerify(decompression, rc, checksum)
		if err == nil {
			return r, nil
		}

		alternativeDecompression := ""
		if strings.Contains(err.Error(), gzip.ErrHeader.Error()) {
			alternativeDecompression = "lz4"
		} else if strings.Contains(err.Error(), "lz4: bad magic number") {
			alternativeDecompression = "gzip"
		}

		if alternativeDecompression != "" {
			rcAlt, errAlt := bsDriver.Read(blkFile)
			if errAlt != nil {
				lastErr = errAlt
			} else {
				rAlt, errAlt := util.DecompressAndVerify(alternativeDecompression, rcAlt, checksum)
				if errAlt == nil {
					return rAlt, nil
				}
				lastErr = errors.Wrapf(errAlt, "fallback decompression also failed for block %v", blkFile)
			}
		} else {
			lastErr = errors.Wrapf(err, "decompression verification failed for block %v", blkFile)
		}

		if attempts < len(backoffDuration) {
			time.Sleep(backoffDuration[attempts])
			attempts++
			continue
		}
		return nil, lastErr
	}
}

func getBlockSizeFromParameters(parameters map[string]string) (int64, error) {
	if parameters == nil {
		return DEFAULT_BLOCK_SIZE, nil
	}
	sizeVal, exist := parameters[lhbackup.LonghornBackupParameterBackupBlockSize]
	if !exist || sizeVal == "" {
		return DEFAULT_BLOCK_SIZE, nil
	}
	quantity, err := resource.ParseQuantity(sizeVal)
	if err != nil {
		return 0, errors.Wrapf(err, "invalid block size %s from parameter %s", sizeVal, lhbackup.LonghornBackupParameterBackupBlockSize)
	}
	if quantity.IsZero() {
		return DEFAULT_BLOCK_SIZE, nil
	}
	return quantity.Value(), nil
}
