//go:build s3test

package s3

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"os"
	"testing"

	"github.com/sirupsen/logrus"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
	service *service
}

var _ = Suite(&TestSuite{})

// Before running the test, make sure to set the following environment variables for S3 test:
// export CONVOY_TEST_BACKUPTARGET_URL="s3://bucket_name@region/"
// export AWS_ACCESS_KEY_ID=""
// export AWS_SECRET_ACCESS_KEY=""
// export AWS_ENDPOINTS=""
// Command: go test -tags=s3test ./s3/

const (
	ENV_TEST_BACKUPTARGET_URL = "CONVOY_TEST_BACKUPTARGET_URL"
)

func (s *TestSuite) SetUpSuite(c *C) {
	logrus.SetLevel(logrus.DebugLevel)
	logrus.SetOutput(os.Stderr)

	destURL := os.Getenv(ENV_TEST_BACKUPTARGET_URL)
	u, err := url.Parse(destURL)
	c.Assert(err, IsNil)

	s.service, err = newService(u)
	c.Assert(err, IsNil)

	if s.service.Region == "" || s.service.Bucket == "" {
		c.Skip("S3 test environment variables not provided.")
	}
}

func (s *TestSuite) TestFuncs(c *C) {
	var err error
	ctx := context.Background()
	body := []byte("this is only a test file")

	key := "test_file"
	key1 := "test_file_1"
	key2 := "test_file_2"

	err = s.service.PutObject(ctx, key1, bytes.NewReader(body))
	c.Assert(err, IsNil)
	err = s.service.PutObject(ctx, key2, bytes.NewReader(body))
	c.Assert(err, IsNil)

	objs, _, err := s.service.ListObjects(ctx, key, "")
	c.Assert(err, IsNil)
	c.Assert(objs, HasLen, 2)

	r, err := s.service.GetObject(ctx, key1)
	c.Assert(err, IsNil)

	newBody, err := io.ReadAll(r)
	c.Assert(err, IsNil)
	c.Assert(newBody, DeepEquals, body)

	err = s.service.DeleteObjects(ctx, key)
	c.Assert(err, IsNil)

	objs, _, err = s.service.ListObjects(ctx, key, "")
	c.Assert(err, IsNil)
	c.Assert(objs, HasLen, 0)
}

func (s *TestSuite) TestList(c *C) {
	var err error
	ctx := context.Background()

	body := []byte("this is only a test file")
	dir1_key1 := "dir/dir1/test_file_1"
	dir1_key2 := "dir/dir1/test_file_2"
	dir2_key1 := "dir/dir2/test_file_1"
	dir2_key2 := "dir/dir2/test_file_2"

	err = s.service.PutObject(ctx, dir1_key1, bytes.NewReader(body))
	c.Assert(err, IsNil)
	err = s.service.PutObject(ctx, dir1_key2, bytes.NewReader(body))
	c.Assert(err, IsNil)
	err = s.service.PutObject(ctx, dir2_key1, bytes.NewReader(body))
	c.Assert(err, IsNil)
	err = s.service.PutObject(ctx, dir2_key2, bytes.NewReader(body))
	c.Assert(err, IsNil)

	objs, prefixes, err := s.service.ListObjects(ctx, "dir/", "/")
	c.Assert(err, IsNil)
	c.Assert(objs, HasLen, 0)
	c.Assert(prefixes, HasLen, 2)

	objs, prefixes, err = s.service.ListObjects(ctx, "dir/dir1/", "/")
	c.Assert(err, IsNil)
	c.Assert(objs, HasLen, 2)
	c.Assert(prefixes, HasLen, 0)

	err = s.service.DeleteObjects(ctx, "dir")
	c.Assert(err, IsNil)

	objs, prefixes, err = s.service.ListObjects(ctx, "dir/", "")
	c.Assert(err, IsNil)
	c.Assert(objs, HasLen, 0)
	c.Assert(prefixes, HasLen, 0)
}

func (s *TestSuite) TestPutObjectSinglePart(c *C) {
	var err error
	ctx := context.Background()
	body := []byte("this is a test file for single part upload")
	key := "test_single_part_file"

	// Create an S3 client
	svc, err := s.service.newInstance(ctx, false)
	c.Assert(err, IsNil)
	defer s.service.Close()

	// Test successful single part upload
	err = s.service.PutObjectSinglePart(ctx, svc, key, bytes.NewReader(body))
	c.Assert(err, IsNil)

	// Verify the object was uploaded by retrieving it
	r, err := s.service.GetObject(ctx, key)
	c.Assert(err, IsNil)
	defer r.Close()

	retrievedBody, err := io.ReadAll(r)
	c.Assert(err, IsNil)
	c.Assert(retrievedBody, DeepEquals, body)

	// Clean up
	err = s.service.DeleteObjects(ctx, key)
	c.Assert(err, IsNil)
}
