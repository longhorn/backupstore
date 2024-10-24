package backupbackingimage

import (
	"net/url"
	"testing"
)

func TestEncodeBackupBackingImageURL(t *testing.T) {
	tests := []struct {
		backingImageName string
		destURL          string
		expectedURL      string
	}{
		{
			backingImageName: "test-image",
			destURL:          "http://example.com",
			expectedURL:      "http://example.com?backingImage=test-image",
		},
		{
			backingImageName: "test-image",
			destURL:          "http://example.com?param=value",
			expectedURL:      "http://example.com?param=value&backingImage=test-image",
		},
		{
			backingImageName: "another-image",
			destURL:          "https://example.org/path",
			expectedURL:      "https://example.org/path?backingImage=another-image",
		},
		{
			backingImageName: "another-image",
			destURL:          "https://example.org/path?existing=param",
			expectedURL:      "https://example.org/path?existing=param&backingImage=another-image",
		},
		{
			backingImageName: "test-image",
			destURL:          "nfs://longhorn-test-nfs-svc.default:/opt/backupstore",
			expectedURL:      "nfs://longhorn-test-nfs-svc.default:/opt/backupstore?backingImage=test-image",
		},
		{
			backingImageName: "test-image",
			destURL:          "nfs://longhorn-test-nfs-svc.default:/opt/backupstore?nfsOptions=soft,timeo=330,retrans=3",
			expectedURL:      "nfs://longhorn-test-nfs-svc.default:/opt/backupstore?nfsOptions=soft,timeo=330,retrans=3&backingImage=test-image",
		},
	}

	for _, tt := range tests {
		t.Run(tt.backingImageName, func(t *testing.T) {
			result := EncodeBackupBackingImageURL(tt.backingImageName, tt.destURL)
			// Validate the result is a well-formed URL
			if _, err := url.Parse(result); err != nil {
				t.Errorf("Generated URL is not valid: %v", err)
			}
			if result != tt.expectedURL {
				t.Errorf("EncodeBackupBackingImageURL(%s, %s) = %s; want %s", tt.backingImageName, tt.destURL, result, tt.expectedURL)
			}
		})
	}
}

// Add negative test cases
func TestEncodeBackupBackingImageURLInvalid(t *testing.T) {
	tests := []struct {
		name             string
		backingImageName string
		destURL          string
	}{
		{"empty backing image", "", "nfs://valid.host:/path"},
		{"empty dest URL", "image", ""},
		{"invalid URL", "image", "not-a-url"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EncodeBackupBackingImageURL(tt.backingImageName, tt.destURL)
			if result != "" {
				t.Errorf("Expected empty result for invalid input, got %s", result)
			}
		})
	}
}
