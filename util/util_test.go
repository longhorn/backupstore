package util

import (
	"io"
	"math/rand"
	"net/url"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
	imageFile string
}

var _ = Suite(&TestSuite{})

const (
	testRoot  = "/tmp/util"
	emptyFile = "/tmp/util/empty"
	testImage = "test.img"
	imageSize = int64(1 << 27)
)

var (
	firstLetters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	letters      = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-")
	nameLength   = 32
)

func (s *TestSuite) createFile(file string, size int64) error {
	return exec.Command("truncate", "-s", strconv.FormatInt(size, 10), file).Run()
}

func (s *TestSuite) SetUpSuite(c *C) {
	err := exec.Command("mkdir", "-p", testRoot).Run()
	c.Assert(err, IsNil)

	s.imageFile = filepath.Join(testRoot, testImage)
	err = s.createFile(s.imageFile, imageSize)
	c.Assert(err, IsNil)

	err = exec.Command("mkfs.ext4", "-F", s.imageFile).Run()
	c.Assert(err, IsNil)

	err = exec.Command("touch", emptyFile).Run()
	c.Assert(err, IsNil)
}

func (s *TestSuite) TearDownSuite(c *C) {
	err := exec.Command("rm", "-rf", testRoot).Run()
	c.Assert(err, IsNil)
}

func (s *TestSuite) TestCompress(c *C) {
	compressionMethods := []string{"none", "gzip", "lz4"}

	for _, compressionMethod := range compressionMethods {
		var err error
		data := []byte("Some random string")
		checksum := GetChecksum(data)

		compressed, err := CompressData(compressionMethod, data)
		c.Assert(err, IsNil)

		decompressed, err := DecompressAndVerify(compressionMethod, io.NopCloser(compressed), checksum)
		c.Assert(err, IsNil)

		result, err := io.ReadAll(decompressed)
		c.Assert(err, IsNil)

		c.Assert(result, DeepEquals, data)
	}
}

func GenerateRandString() string {
	r := make([]rune, nameLength)
	r[0] = firstLetters[rand.Intn(len(firstLetters))]
	for i := 1; i < nameLength; i++ {
		r[i] = letters[rand.Intn(len(letters))]
	}
	return string(r)
}

func (s *TestSuite) TestExtractNames(c *C) {
	prefix := "prefix_"
	suffix := ".suffix"
	counts := 10
	names := make([]string, counts)
	files := make([]string, counts)
	for i := 0; i < counts; i++ {
		names[i] = GenerateRandString()
		files[i] = prefix + names[i] + suffix
	}

	result := ExtractNames(files, "prefix_", ".suffix")
	for i := 0; i < counts; i++ {
		c.Assert(result[i], Equals, names[i])
	}

	files[0] = "/" + files[0]
	result = ExtractNames(files, "prefix_", ".suffix")
	c.Assert(result[0], Equals, names[0])

	files[0] = "prefix_.dd_xx.suffix"
	result = ExtractNames(files, "prefix_", ".suffix")
	c.Assert(len(result), Equals, len(files)-1)
	c.Assert(result[0], Equals, names[1]) // files[0] is invalid

	files[0] = "prefix_-dd_xx.suffix"
	result = ExtractNames(files, "prefix_", ".suffix")
	c.Assert(len(result), Equals, len(files)-1)
	c.Assert(result[0], Equals, names[1]) // files[0] is invalid

	files[0] = "prefix__dd_xx.suffix"
	result = ExtractNames(files, "prefix_", ".suffix")
	c.Assert(len(result), Equals, len(files)-1)
	c.Assert(result[0], Equals, names[1]) // files[0] is invalid

	files[0] = "prefix_1234@failure.suffix"
	result = ExtractNames(files, "prefix_", ".suffix")
	c.Assert(len(result), Equals, len(files)-1)
	c.Assert(result[0], Equals, names[1]) // files[0] is invalid
}

func (s *TestSuite) TestValidateName(c *C) {
	c.Assert(ValidateName(""), Equals, false)
	c.Assert(ValidateName("_09123a."), Equals, false)
	c.Assert(ValidateName("ubuntu14.04_v1"), Equals, true)
	c.Assert(ValidateName("123/456.a"), Equals, false)
	c.Assert(ValidateName("a.\t"), Equals, false)
	c.Assert(ValidateName("ubuntu14.04_v1 "), Equals, false)
}

func (s *TestSuite) TestSplitMountOptions(c *C) {
	testCases := []struct {
		destURL          string
		expectParseError bool
		expectOptions    []string
	}{
		{
			// Test NFS target with no mount options.
			destURL:       "nfs://longhorn-test-nfs-svc.default:/opt/backupstore",
			expectOptions: []string{},
		},
		{
			// Test NFS target with empty Query tag.
			destURL:       "nfs://longhorn-test-nfs-svc.default:/opt/backupstore?",
			expectOptions: []string{},
		},
		{
			// Test NFS target with mount options as comma-separate string in single query tag.
			destURL:       "nfs://longhorn-test-nfs-svc.default:/opt/backupstore?nfsOptions=soft,timeo=150,retrans=3",
			expectOptions: []string{"soft", "timeo=150", "retrans=3"},
		},
		{
			// Test NFS target with mount options as escaped, comma-separate string in single query tag.
			destURL:       "nfs://longhorn-test-nfs-svc.default:/opt/backupstore?nfsOptions=soft%2Ctimeo%3D150%2Cretrans%3D3",
			expectOptions: []string{"soft", "timeo=150", "retrans=3"},
		},
		{
			// Test NFS target with mount options as multiple query tags
			destURL:       "nfs://longhorn-test-nfs-svc.default:/opt/backupstore?nfsOptions=soft&nfsOptions=timeo=150&nfsOptions=retrans=3",
			expectOptions: []string{"soft", "timeo=150", "retrans=3"},
		},
		{
			// Test NFS target with mount options and other tags
			destURL:       "nfs://longhorn-test-nfs-svc.default:/opt/backupstore?nfsOptions=soft,timeo=150,retrans=3&otherTag=mumble",
			expectOptions: []string{"soft", "timeo=150", "retrans=3"},
		},
		{
			// Test NFS target with empty mount options.
			destURL:       "nfs://longhorn-test-nfs-svc.default:/opt/backupstore?nfsOptions=",
			expectOptions: []string{""},
		},
		{
			// Test invalid target URL.  Second "?" is treated as literal data.
			destURL:       "nfs://longhorn-test-nfs-svc.default:/opt/backupstore?nfsOptions=mumble?foo=bar",
			expectOptions: []string{"mumble?foo=bar"},
		},
	}

	for _, tc := range testCases {
		u, err := url.Parse(tc.destURL)
		if tc.expectParseError {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			nfsOptions, exist := u.Query()["nfsOptions"]
			if exist {
				options := SplitMountOptions(nfsOptions)
				c.Assert(options, DeepEquals, tc.expectOptions)
			}
		}
	}
}
