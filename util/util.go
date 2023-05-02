package util

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.uber.org/multierr"
	mount "k8s.io/mount-utils"
)

const (
	PreservedChecksumLength = 64

	forceCleanupMountTimeout = 30 * time.Second

	MountDir = "/var/lib/longhorn-backupstore-mounts"
)

var (
	cmdTimeout = time.Minute // one minute by default
)

func GenerateName(prefix string) string {
	suffix := strings.Replace(NewUUID(), "-", "", -1)
	return prefix + "-" + suffix[:16]
}

func NewUUID() string {
	return uuid.New().String()
}

func GetChecksum(data []byte) string {
	checksumBytes := sha512.Sum512(data)
	checksum := hex.EncodeToString(checksumBytes[:])[:PreservedChecksumLength]
	return checksum
}

func GetFileChecksum(filePath string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

func CompressData(data []byte) (io.ReadSeeker, error) {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	if _, err := w.Write(data); err != nil {
		w.Close()
		return nil, err
	}
	w.Close()
	return bytes.NewReader(b.Bytes()), nil
}

func DecompressAndVerify(src io.Reader, checksum string) (io.Reader, error) {
	r, err := gzip.NewReader(src)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	block, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if GetChecksum(block) != checksum {
		return nil, fmt.Errorf("checksum verification failed for block")
	}
	return bytes.NewReader(block), nil
}

func Now() string {
	return time.Now().UTC().Format(time.RFC3339)
}

func UnorderedEqual(x, y []string) bool {
	if len(x) != len(y) {
		return false
	}
	known := make(map[string]struct{})
	for _, value := range x {
		known[value] = struct{}{}
	}
	for _, value := range y {
		if _, present := known[value]; !present {
			return false
		}
	}
	return true
}

func Filter(elements []string, predicate func(string) bool) []string {
	var filtered []string
	for _, elem := range elements {
		if predicate(elem) {
			filtered = append(filtered, elem)
		}
	}
	return filtered
}

func ExtractNames(names []string, prefix, suffix string) []string {
	result := []string{}
	for _, f := range names {
		// Remove additional slash if exists
		f = strings.TrimLeft(f, "/")

		// missing prefix or suffix
		if !strings.HasPrefix(f, prefix) || !strings.HasSuffix(f, suffix) {
			continue
		}

		f = strings.TrimPrefix(f, prefix)
		f = strings.TrimSuffix(f, suffix)
		if !ValidateName(f) {
			logrus.Errorf("Invalid name %v was processed to extract name with prefix %v suffix %v",
				f, prefix, suffix)
			continue
		}
		result = append(result, f)
	}
	return result
}

func ValidateName(name string) bool {
	validName := regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]+$`)
	return validName.MatchString(name)
}

func Execute(binary string, args []string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cmdTimeout)
	defer cancel()
	return execute(ctx, binary, args)
}

func ExecuteWithCustomTimeout(binary string, args []string, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return execute(ctx, binary, args)
}

func execute(ctx context.Context, binary string, args []string) (string, error) {
	var output []byte
	var err error

	cmd := exec.CommandContext(ctx, binary, args...)
	done := make(chan struct{})

	go func() {
		output, err = cmd.CombinedOutput()
		close(done)
	}()

	select {
	case <-done:
		break
	case <-ctx.Done():
		return "", fmt.Errorf("timeout executing: %v %v, output %v, error %v", binary, args, string(output), err)
	}

	if err != nil {
		return "", fmt.Errorf("failed to execute: %v %v, output %v, error %v", binary, args, string(output), err)
	}

	return string(output), nil
}

func UnescapeURL(url string) string {
	// Deal with escape in url inputted from bash
	result := strings.Replace(url, "\\u0026", "&", 1)
	result = strings.Replace(result, "u0026", "&", 1)
	return result
}

func IsMounted(mountPoint string) bool {
	output, err := Execute("mount", []string{})
	if err != nil {
		return false
	}
	lines := strings.Split(output, "\n")
	for _, line := range lines {
		if strings.Contains(line, " "+mountPoint+" ") {
			return true
		}
	}
	return false
}

func cleanupMount(mountDir string, mounter mount.Interface, log logrus.FieldLogger) error {
	forceUnmounter, ok := mounter.(mount.MounterForceUnmounter)
	if ok {
		log.Infof("Trying to force clean up mount point %v", mountDir)
		return mount.CleanupMountWithForce(mountDir, forceUnmounter, false, forceCleanupMountTimeout)
	}

	log.Infof("Trying to clean up mount point %v", mountDir)
	return mount.CleanupMountPoint(mountDir, forceUnmounter, false)
}

// CleanUpMountPoints tries to clean up all existing mount points for existing backup stores
func CleanUpMountPoints(mounter mount.Interface, log logrus.FieldLogger) error {
	var errs error

	filepath.Walk(MountDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			errs = multierr.Append(errs, errors.Wrapf(err, "failed to get file info of %v", path))
			return nil
		}

		if !info.IsDir() {
			return nil
		}

		notMounted, err := mount.IsNotMountPoint(mounter, path)
		if err != nil {
			errs = multierr.Append(errs, errors.Wrapf(err, "failed to check if %s is not mounted", path))
			return nil
		}

		if notMounted {
			return nil
		}

		if err := cleanupMount(path, mounter, log); err != nil {
			errs = multierr.Append(errs, errors.Wrapf(err, "failed to clean up mount point %v", path))
		}

		return nil
	})

	return errs
}
