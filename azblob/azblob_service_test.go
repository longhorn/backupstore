package azblob

import (
	"net/url"
	"testing"
)

func TestExtractAccountNameFromURL(t *testing.T) {
	tests := []struct {
		name        string
		urlStr      string
		expectedAcc string
	}{
		{
			name:        "standard Azure URL with account in host",
			urlStr:      "azblob://mycontainer@mystorageaccount.blob.core.windows.net/backups",
			expectedAcc: "mystorageaccount",
		},
		{
			name:        "sovereign cloud URL",
			urlStr:      "azblob://mycontainer@mystorageaccount.blob.core.chinacloudapi.cn/backups",
			expectedAcc: "mystorageaccount",
		},
		{
			name:        "URL without user part (container only)",
			urlStr:      "azblob://mycontainer/backups",
			expectedAcc: "",
		},
		{
			name:        "empty host",
			urlStr:      "azblob://mycontainer@/backups",
			expectedAcc: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := url.Parse(tt.urlStr)
			if err != nil {
				t.Fatalf("failed to parse URL: %v", err)
			}

			got := extractAccountNameFromURL(u)
			if got != tt.expectedAcc {
				t.Errorf("extractAccountNameFromURL() = %q, want %q", got, tt.expectedAcc)
			}
		})
	}
}

func TestBuildServiceURL(t *testing.T) {
	tests := []struct {
		name           string
		accountName    string
		customEndpoint string
		endpointSuffix string
		expected       string
	}{
		{
			name:           "default public cloud",
			accountName:    "mystorageaccount",
			customEndpoint: "",
			endpointSuffix: "",
			expected:       "https://mystorageaccount.blob.core.windows.net",
		},
		{
			name:           "public cloud with core.windows.net suffix",
			accountName:    "mystorageaccount",
			customEndpoint: "",
			endpointSuffix: "core.windows.net",
			expected:       "https://mystorageaccount.blob.core.windows.net",
		},
		{
			name:           "China sovereign cloud",
			accountName:    "mystorageaccount",
			customEndpoint: "",
			endpointSuffix: "core.chinacloudapi.cn",
			expected:       "https://mystorageaccount.blob.core.chinacloudapi.cn",
		},
		{
			name:           "US Government cloud",
			accountName:    "mystorageaccount",
			customEndpoint: "",
			endpointSuffix: "core.usgovcloudapi.net",
			expected:       "https://mystorageaccount.blob.core.usgovcloudapi.net",
		},
		{
			name:           "custom endpoint (Azurite)",
			accountName:    "devstoreaccount1",
			customEndpoint: "http://127.0.0.1:10000",
			endpointSuffix: "",
			expected:       "http://127.0.0.1:10000/devstoreaccount1",
		},
		{
			name:           "custom endpoint with trailing slash",
			accountName:    "devstoreaccount1",
			customEndpoint: "http://127.0.0.1:10000/",
			endpointSuffix: "",
			expected:       "http://127.0.0.1:10000/devstoreaccount1",
		},
		{
			name:           "custom endpoint takes precedence over suffix",
			accountName:    "mystorageaccount",
			customEndpoint: "https://custom.endpoint.com",
			endpointSuffix: "core.chinacloudapi.cn",
			expected:       "https://custom.endpoint.com/mystorageaccount",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildServiceURL(tt.accountName, tt.customEndpoint, tt.endpointSuffix)
			if got != tt.expected {
				t.Errorf("buildServiceURL() = %q, want %q", got, tt.expected)
			}
		})
	}
}
