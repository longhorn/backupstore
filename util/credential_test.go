package util

import (
	"os"
	"testing"

	"github.com/longhorn/backupstore/types"
)

// The retry settings are configured in one process and the S3 operation runs in
// another, with the credential map carrying the values between them. A key that
// is not copied out and set back is silently lost, and newInstance falls back to
// the defaults.
func TestS3CredentialRoundTripCarriesRetrySettings(t *testing.T) {
	want := map[string]string{
		types.AWSAccessKey:            "access",
		types.AWSSecretKey:            "secret",
		types.AWSRetryMaxAttempts:     "7",
		types.AWSRetryMaximumAttempts: "20",
		types.AWSRetryMaximumBackoff:  "45s",
	}
	for k, v := range want {
		t.Setenv(k, v)
	}

	credential, err := getS3CredentialFromEnvVars()
	if err != nil {
		t.Fatalf("getS3CredentialFromEnvVars() error = %v", err)
	}
	for k, v := range want {
		if credential[k] != v {
			t.Errorf("credential[%s] = %q, want %q", k, credential[k], v)
		}
	}

	// Simulate the receiving process, which starts without the variables set.
	for k := range want {
		if err := os.Unsetenv(k); err != nil {
			t.Fatalf("Unsetenv(%s) error = %v", k, err)
		}
	}
	if err := setupS3Credential(credential); err != nil {
		t.Fatalf("setupS3Credential() error = %v", err)
	}
	for k, v := range want {
		if got := os.Getenv(k); got != v {
			t.Errorf("after setupS3Credential, os.Getenv(%s) = %q, want %q", k, got, v)
		}
	}
}
