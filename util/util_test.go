package util

import (
	"io/ioutil"
	"math/rand"
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
	var err error
	data := []byte("Some random string")
	checksum := GetChecksum(data)

	compressed, err := CompressData(data)
	c.Assert(err, IsNil)

	decompressed, err := DecompressAndVerify(compressed, checksum)
	c.Assert(err, IsNil)

	result, err := ioutil.ReadAll(decompressed)
	c.Assert(err, IsNil)

	c.Assert(result, DeepEquals, data)
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

	result, err := ExtractNames(files, "prefix_", ".suffix")
	c.Assert(err, Equals, nil)
	for i := 0; i < counts; i++ {
		c.Assert(result[i], Equals, names[i])
	}

	files[0] = "/" + files[0]
	result, err = ExtractNames(files, "prefix_", ".suffix")
	c.Assert(err, Equals, nil)
	c.Assert(result[0], Equals, names[0])

	files[0] = "prefix_.dd_xx.suffix"
	result, err = ExtractNames(files, "prefix_", ".suffix")
	c.Assert(err, ErrorMatches, "Invalid name.*")

	files[0] = "prefix_-dd_xx.suffix"
	result, err = ExtractNames(files, "prefix_", ".suffix")
	c.Assert(err, ErrorMatches, "Invalid name.*")

	files[0] = "prefix__dd_xx.suffix"
	result, err = ExtractNames(files, "prefix_", ".suffix")
	c.Assert(err, ErrorMatches, "Invalid name.*")
}

func (s *TestSuite) TestValidateName(c *C) {
	c.Assert(ValidateName(""), Equals, false)
	c.Assert(ValidateName("_09123a."), Equals, false)
	c.Assert(ValidateName("ubuntu14.04_v1"), Equals, true)
	c.Assert(ValidateName("123/456.a"), Equals, false)
	c.Assert(ValidateName("a.\t"), Equals, false)
	c.Assert(ValidateName("ubuntu14.04_v1 "), Equals, false)
}
