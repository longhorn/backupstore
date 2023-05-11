package azblob

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/pkg/errors"
)

const (
	azureURL           = "core.windows.net"
	azureConnNameKey   = "AccountName=%s;AccountKey=%s;"
	blobEndpoint       = "BlobEndpoint=%s;"
	blobEndpointSuffix = "EndpointSuffix=%s;"

	downloadMaxRetryRequests = 1024
)

type service struct {
	Container       string
	ServiceURL      string
	ContainerClient azblob.ContainerClient
}

func newService(u *url.URL) (*service, error) {
	s := service{}
	if u.User != nil {
		s.ServiceURL = u.Host
		s.Container = u.User.Username()
	} else {
		s.Container = u.Host
	}

	accountName := os.Getenv("AZBLOB_ACCOUNT_NAME")
	accountKey := os.Getenv("AZBLOB_ACCOUNT_KEY")
	azureEndpoint := os.Getenv("AZBLOB_ENDPOINT")

	connStr := fmt.Sprintf(azureConnNameKey, accountName, accountKey)
	if azureEndpoint != "" {
		blobEndpointURL := strings.TrimRight(azureEndpoint, "/") + "/" + accountName
		connStr = fmt.Sprintf(connStr+blobEndpoint, blobEndpointURL)
	}

	if s.ServiceURL == "" {
		s.ServiceURL = azureURL
	}
	if s.ServiceURL != azureURL {
		connStr = connStr + fmt.Sprintf(blobEndpointSuffix, s.ServiceURL)
	}

	serviceClient, err := azblob.NewServiceClientFromConnectionString(connStr, nil)
	if err != nil {
		return nil, err
	}

	s.ContainerClient = serviceClient.NewContainerClient(s.Container)
	return &s, nil
}

// listBlobs returns items that contains blobs or blob prefixes
func (s *service) listBlobs(prefix, delimiter string) (*[]string, error) {
	listOptions := &azblob.ContainerListBlobHierarchySegmentOptions{Prefix: &prefix}
	pager := s.ContainerClient.ListBlobsHierarchy(delimiter, listOptions)

	var blobs []string
	for pager.NextPage(context.Background()) {
		resp := pager.PageResponse()
		for _, v := range resp.ContainerListBlobHierarchySegmentResult.Segment.BlobItems {
			blobs = append(blobs, *v.Name)
		}
		for _, v := range resp.ContainerListBlobHierarchySegmentResult.Segment.BlobPrefixes {
			blobs = append(blobs, *v.Name)
		}
	}

	if err := pager.Err(); err != nil {
		return nil, err
	}

	return &blobs, nil
}

func (s *service) getBlobProperties(blob string) (*azblob.GetBlobPropertiesResponse, error) {
	blobClient := s.ContainerClient.NewBlockBlobClient(blob)

	response, err := blobClient.GetProperties(context.Background(), nil)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

func (s *service) putBlob(blob string, reader io.ReadSeeker) error {
	blobClient := s.ContainerClient.NewBlockBlobClient(blob)

	_, err := blobClient.Upload(context.Background(), streaming.NopCloser(reader), nil)
	if err != nil {
		return err
	}

	return nil
}

func (s *service) getBlob(blob string) (io.ReadCloser, error) {
	blobClient := s.ContainerClient.NewBlockBlobClient(blob)

	response, err := blobClient.Download(context.Background(), nil)
	if err != nil {
		return nil, err
	}

	return response.Body(&azblob.RetryReaderOptions{MaxRetryRequests: downloadMaxRetryRequests}), nil
}

func (s *service) deleteBlobs(blob string) error {
	blobs, err := s.listBlobs(blob, "")
	if err != nil {
		return errors.Wrapf(err, "failed to list blobs with prefix %v before removing them", blob)
	}

	var deletionFailures []string
	for _, blob := range *blobs {
		blobClient := s.ContainerClient.NewBlockBlobClient(blob)
		_, err = blobClient.Delete(context.Background(), nil)
		if err != nil {
			log.WithError(err).Errorf("Failed to delete blob object: %v", blob)
			deletionFailures = append(deletionFailures, blob)
		}
	}

	if len(deletionFailures) > 0 {
		return fmt.Errorf("failed to delete blobs %v", deletionFailures)
	}

	return nil
}
