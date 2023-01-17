package cifs

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/backupstore/fsops"
	"github.com/longhorn/backupstore/util"
)

var (
	log            = logrus.WithFields(logrus.Fields{"pkg": "cifs"})
	defaultTimeout = 5 * time.Second
)

type BackupStoreDriver struct {
	destURL    string
	serverPath string
	mountDir   string

	username string
	password string

	*fsops.FileSystemOperator
}

const (
	KIND = "cifs"

	MountDir = "/var/lib/longhorn-backupstore-mounts"

	MaxCleanupLevel = 10
)

func init() {
	if err := backupstore.RegisterDriver(KIND, initFunc); err != nil {
		panic(err)
	}
}

func initFunc(destURL string) (backupstore.BackupStoreDriver, error) {
	b := &BackupStoreDriver{}
	b.FileSystemOperator = fsops.NewFileSystemOperator(b)

	u, err := url.Parse(destURL)
	if err != nil {
		return nil, err
	}

	if u.Scheme != KIND {
		return nil, fmt.Errorf("BUG: Why dispatch %v to %v?", u.Scheme, KIND)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("CIFS path must follow: cifs://server/path/ format")
	}
	if u.Path == "" {
		return nil, fmt.Errorf("cannot find CIFS path")
	}

	b.serverPath = u.Host + u.Path
	b.mountDir = filepath.Join(MountDir, strings.TrimRight(strings.Replace(u.Host, ".", "_", -1), ":"), u.Path)
	if _, err = util.ExecuteWithCustomTimeout("mkdir", []string{"-m", "700", "-p", b.mountDir}, defaultTimeout); err != nil {
		return nil, errors.Wrapf(err, "cannot create mount directory %v for CIFS server", b.mountDir)
	}

	b.username = os.Getenv("CIFS_USERNAME")
	b.password = os.Getenv("CIFS_PASSWORD")

	if err := b.mount(); err != nil {
		return nil, errors.Wrapf(err, "cannot mount CIFS %v", b.serverPath)
	}
	if _, err := b.List(""); err != nil {
		return nil, fmt.Errorf("CIFS path %v doesn't exist or is not a directory", b.serverPath)
	}

	b.destURL = KIND + "://" + b.serverPath
	log.Infof("Loaded driver for %v", b.destURL)
	return b, nil
}

func (b *BackupStoreDriver) mount() (err error) {
	if util.IsMounted(b.mountDir) {
		return nil
	}

	cmd := []string{
		"-t", "cifs",
	}
	if b.username != "" {
		cmd = append(cmd, "-o", fmt.Sprintf("username=%v", b.username))
	}
	if b.password != "" {
		cmd = append(cmd, "-o", fmt.Sprintf("password=%v", b.password))
	}
	cmd = append(cmd, "//"+b.serverPath, b.mountDir)

	_, err = util.ExecuteWithCustomTimeout("mount", cmd, defaultTimeout)
	if err != nil {
		return errors.Wrapf(err, "failed to mount using CIFS")
	}
	return nil
}

func (b *BackupStoreDriver) Kind() string {
	return KIND
}

func (b *BackupStoreDriver) GetURL() string {
	return b.destURL
}

func (b *BackupStoreDriver) LocalPath(path string) string {
	return filepath.Join(b.mountDir, path)
}
