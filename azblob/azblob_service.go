package azblob

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/cockroachdb/errors"

	azblobsvc "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"

	"github.com/longhorn/backupstore/http"
)

const (
	azureURL = "core.windows.net"
)

type service struct {
	Container       string
	EndpointSuffix  string
	ContainerClient *container.Client
}

func newService(u *url.URL) (*service, error) {
	s := service{}
	if u.User != nil {
		s.EndpointSuffix = u.Host
		s.Container = u.User.Username()
	} else {
		s.Container = u.Host
	}

	accountName := os.Getenv("AZBLOB_ACCOUNT_NAME")
	accountKey := os.Getenv("AZBLOB_ACCOUNT_KEY")
	azureEndpoint := os.Getenv("AZBLOB_ENDPOINT")

	if accountName == "" {
		accountName = extractAccountNameFromURL(u)
	}
	if accountName == "" {
		return nil, fmt.Errorf("AZBLOB_ACCOUNT_NAME is required")
	}

	serviceURL := buildServiceURL(accountName, azureEndpoint, s.EndpointSuffix)

	customCerts := getCustomCerts()
	httpClient, err := http.GetClientWithCustomCerts(customCerts)
	if err != nil {
		return nil, err
	}
	opts := azblobsvc.ClientOptions{ClientOptions: azcore.ClientOptions{Transport: httpClient}}

	var serviceClient *azblobsvc.Client

	if accountKey != "" {
		log.Infof("Using SharedKeyCredential for Azure Blob Storage (account: %s)", accountName)
		cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create SharedKeyCredential: %w", err)
		}
		serviceClient, err = azblobsvc.NewClientWithSharedKeyCredential(serviceURL, cred, &opts)
		if err != nil {
			return nil, fmt.Errorf("failed to create service client with SharedKeyCredential: %w", err)
		}
	} else {
		// Azure AD authentication via DefaultAzureCredential
		// Automatically chains through: EnvironmentCredential, WorkloadIdentityCredential,
		// ManagedIdentityCredential, AzureCLICredential
		log.Infof("Using DefaultAzureCredential for Azure Blob Storage (account: %s)", accountName)
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create DefaultAzureCredential: %w", err)
		}
		serviceClient, err = azblobsvc.NewClient(serviceURL, cred, &opts)
		if err != nil {
			return nil, fmt.Errorf("failed to create service client with DefaultAzureCredential: %w", err)
		}
	}

	s.ContainerClient = serviceClient.NewContainerClient(s.Container)
	return &s, nil
}

// extractAccountNameFromURL attempts to extract the storage account name from the backup target URL.
// For URLs like azblob://container@mystorageaccount.blob.core.windows.net/path,
// the account name is the first part of the host before ".blob.".
func extractAccountNameFromURL(u *url.URL) string {
	if u.User != nil && u.Host != "" {
		// Check if host contains ".blob." indicating full endpoint format
		// e.g., "mystorageaccount.blob.core.windows.net"
		if idx := strings.Index(u.Host, ".blob."); idx > 0 {
			return u.Host[:idx]
		}
	}
	return ""
}

// isFullBlobEndpoint checks if the host is a full blob endpoint (contains ".blob.")
// vs just an endpoint suffix like "core.windows.net" or "core.chinacloudapi.cn"
func isFullBlobEndpoint(host string) bool {
	return strings.Contains(host, ".blob.")
}

func buildServiceURL(accountName, customEndpoint, endpointSuffix string) string {
	if customEndpoint != "" {
		// Custom endpoint (e.g., Azurite or sovereign cloud)
		return fmt.Sprintf("%s/%s", strings.TrimRight(customEndpoint, "/"), accountName)
	}
	if endpointSuffix != "" {
		// Check if endpointSuffix is already a full blob endpoint
		// e.g., "mystorageaccount.blob.core.windows.net"
		if isFullBlobEndpoint(endpointSuffix) {
			return fmt.Sprintf("https://%s", endpointSuffix)
		}
		// Legacy format: endpoint suffix like "core.chinacloudapi.cn"
		if endpointSuffix != azureURL {
			return fmt.Sprintf("https://%s.blob.%s", accountName, endpointSuffix)
		}
	}
	// Default Azure public cloud
	return fmt.Sprintf("https://%s.blob.%s", accountName, azureURL)
}

func getCustomCerts() []byte {
	// Certificates in PEM format (base64)
	certs := os.Getenv("AZBLOB_CERT")
	if certs == "" {
		return nil
	}

	return []byte(certs)
}

func (s *service) listBlobs(prefix, delimiter string) (*[]string, error) {
	listOptions := &container.ListBlobsHierarchyOptions{Prefix: &prefix}
	pager := s.ContainerClient.NewListBlobsHierarchyPager(delimiter, listOptions)

	var blobs []string
	for pager.More() {
		page, err := pager.NextPage(context.Background())
		if err != nil {
			return nil, err
		}
		for _, v := range page.Segment.BlobItems {
			blobs = append(blobs, *v.Name)
		}
		for _, v := range page.Segment.BlobPrefixes {
			blobs = append(blobs, *v.Name)
		}
	}

	return &blobs, nil
}

func (s *service) getBlobProperties(blob string) (*blob.GetPropertiesResponse, error) {
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

	response, err := blobClient.DownloadStream(context.Background(), nil)
	if err != nil {
		return nil, err
	}

	return response.Body, nil
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
