package s3

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
)

// TestIgnoreAcceptEncodingSigning covers the decision itself, including the
// Google Cloud Storage endpoint, which cannot be exercised through the fake
// server below because that server is reached by its own address.
func TestIgnoreAcceptEncodingSigning(t *testing.T) {
	cases := []struct {
		name      string
		endpoints string
		env       string
		want      bool
	}{
		{"empty, custom endpoint", "https://s3.example.com", "", false},
		{"true, custom endpoint", "https://s3.example.com", "true", false},
		{"false, custom endpoint", "https://s3.example.com", "false", true},
		{"False, custom endpoint", "https://s3.example.com", "False", true},
		{"0, custom endpoint", "https://s3.example.com", "0", true},
		// A Secret value commonly arrives with a trailing newline.
		{"false with trailing newline", "https://s3.example.com", "false\n", true},
		{"false with trailing CR", "https://s3.example.com", "false\r\n", true},
		{"garbage falls back to signing", "https://s3.example.com", "yes-please", false},
		{"empty, no endpoint", "", "", false},
		{"false, no endpoint", "", "false", true},
		{"empty, GCS endpoint", "https://storage.googleapis.com", "", true},
		{"true, GCS endpoint", "https://storage.googleapis.com", "true", true},
		{"false, GCS endpoint", "https://storage.googleapis.com", "false", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(AWSSignAcceptEncoding, tc.env)
			if got := ignoreAcceptEncodingSigning(tc.endpoints); got != tc.want {
				t.Fatalf("ignoreAcceptEncodingSigning(%q) with %s=%q = %v, want %v",
					tc.endpoints, AWSSignAcceptEncoding, tc.env, got, tc.want)
			}
		})
	}
}

// TestAcceptEncodingIsSignedByDefault pins the default behavior: aws-sdk-go-v2
// signs `Accept-Encoding`, so an endpoint that does not alter the header keeps
// working exactly as before.
func TestAcceptEncodingIsSignedByDefault(t *testing.T) {
	server := newFakeS3Server()
	defer server.Close()

	svc := newTestService(t, server.URL)
	t.Setenv(AWSSignAcceptEncoding, "")

	if err := svc.PutObjectAsSinglePart(context.Background(), "backups/volume.cfg", bytes.NewReader([]byte("test"))); err != nil {
		t.Fatalf("PutObjectAsSinglePart failed: %v", err)
	}

	requests := server.recordedRequests()
	if len(requests) != 1 {
		t.Fatalf("expected exactly 1 request, got %d: %+v", len(requests), requests)
	}

	if signed := requests[0].signedHeaders(); !strings.Contains(signed, "accept-encoding") {
		t.Fatalf("expected accept-encoding in SignedHeaders, got %q", signed)
	}
}

// TestSignAcceptEncodingFalseExcludesAcceptEncodingFromSignature is the
// regression test for backup targets reached through a proxy that rewrites
// `Accept-Encoding` in transit (e.g. a Cloudflare Tunnel, which replaces the
// value with "gzip, br"). With AWS_SIGN_ACCEPT_ENCODING=false the header must
// be absent from SignedHeaders, so the endpoint verifies the signature without
// it, while the header itself is still sent on the wire.
func TestSignAcceptEncodingFalseExcludesAcceptEncodingFromSignature(t *testing.T) {
	server := newFakeS3Server()
	defer server.Close()

	svc := newTestService(t, server.URL)
	t.Setenv(AWSSignAcceptEncoding, "false")

	if err := svc.PutObjectAsSinglePart(context.Background(), "backups/volume.cfg", bytes.NewReader([]byte("test"))); err != nil {
		t.Fatalf("PutObjectAsSinglePart failed: %v", err)
	}

	requests := server.recordedRequests()
	if len(requests) != 1 {
		t.Fatalf("expected exactly 1 request, got %d: %+v", len(requests), requests)
	}

	req := requests[0]
	signed := req.signedHeaders()
	// Guard first: signedHeaders() also returns "" for an unsigned request, which
	// would make the exclusion check below pass without asserting anything.
	if !strings.Contains(signed, "host") {
		t.Fatalf("expected a signed request, got SignedHeaders %q", signed)
	}
	if strings.Contains(signed, "accept-encoding") {
		t.Fatalf("expected accept-encoding to be excluded from SignedHeaders, got %q", signed)
	}
	// Assert the exact value, not merely that the header is present. Go's
	// http.Transport substitutes "gzip" whenever the header is absent, so a
	// non-empty check would still pass if restoreIgnored stopped working.
	if req.acceptEncoding != "identity" {
		t.Fatalf("expected Accept-Encoding %q on the wire after signing, got %q", "identity", req.acceptEncoding)
	}
}

// TestIgnoreAcceptEncodingSigningWhenUnset covers the genuinely unset variable.
// t.Setenv can only set an empty value, and the table above relies on that.
func TestIgnoreAcceptEncodingSigningWhenUnset(t *testing.T) {
	// t.Setenv registers the cleanup that restores the original value.
	t.Setenv(AWSSignAcceptEncoding, "")
	if err := os.Unsetenv(AWSSignAcceptEncoding); err != nil {
		t.Fatalf("failed to unset %v: %v", AWSSignAcceptEncoding, err)
	}

	if ignoreAcceptEncodingSigning("https://s3.example.com") {
		t.Fatal("expected Accept-Encoding to stay signed when the key is unset")
	}
}
