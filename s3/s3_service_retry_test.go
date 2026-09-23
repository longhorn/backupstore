package s3

import (
	"context"
	"testing"
	"time"
)

func TestRetryMaxAttempts_EnvOverride(t *testing.T) {
	cases := []struct {
		name string
		env  string
		want int
	}{
		{"unset", "", AWSRetryMaxAttempts},
		{"valid", "7", 7},
		{"zero falls back", "0", AWSRetryMaxAttempts},
		{"negative falls back", "-1", AWSRetryMaxAttempts},
		{"garbage falls back", "abc", AWSRetryMaxAttempts},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(EnvAWSRetryMaxAttempts, tc.env)
			if got := retryMaxAttempts(); got != tc.want {
				t.Fatalf("retryMaxAttempts() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestRetryMaximumAttempts_EnvOverride(t *testing.T) {
	cases := []struct {
		name string
		env  string
		want int
	}{
		{"unset", "", AWSRetryMaximumAttempts},
		{"valid", "20", 20},
		{"zero falls back", "0", AWSRetryMaximumAttempts},
		{"garbage falls back", "x", AWSRetryMaximumAttempts},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(EnvAWSRetryMaximumAttempts, tc.env)
			if got, _ := retryMaximumAttempts(); got != tc.want {
				t.Fatalf("retryMaximumAttempts() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestRetryMaximumBackoff_EnvOverride(t *testing.T) {
	cases := []struct {
		name string
		env  string
		want time.Duration
	}{
		{"unset", "", AWSRetryMaximumBackoff},
		{"valid seconds", "60s", 60 * time.Second},
		{"valid minutes", "5m", 5 * time.Minute},
		{"zero falls back", "0s", AWSRetryMaximumBackoff},
		{"garbage falls back", "nope", AWSRetryMaximumBackoff},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(EnvAWSRetryMaximumBackoff, tc.env)
			if got := retryMaximumBackoff(); got != tc.want {
				t.Fatalf("retryMaximumBackoff() = %v, want %v", got, tc.want)
			}
		})
	}
}

// The env-parsing tests above pass whether or not the retryer is actually wired
// into the SDK, so assert the effective value on the constructed client.
// s3.NewFromConfig runs finalizeRetryMaxAttempts after the option callback and
// caps a custom retryer with o.RetryMaxAttempts, which would silently limit
// AWS_RETRY_MAXIMUM_ATTEMPTS to AWS_RETRY_MAX_ATTEMPTS.
func TestRetryMaximumAttempts_AppliedToClient(t *testing.T) {
	t.Setenv(EnvAWSRetryMaxAttempts, "5")
	t.Setenv(EnvAWSRetryMaximumAttempts, "20")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	s := &service{Region: "us-east-1"}

	client, err := s.newInstance(context.Background(), true)
	if err != nil {
		t.Fatalf("newInstance() error = %v", err)
	}
	if got := client.Options().Retryer.MaxAttempts(); got != 20 {
		t.Errorf("with retry backoff, Retryer.MaxAttempts() = %d, want 20", got)
	}

	// Without the custom retryer, AWS_RETRY_MAX_ATTEMPTS still governs.
	client, err = s.newInstance(context.Background(), false)
	if err != nil {
		t.Fatalf("newInstance() error = %v", err)
	}
	if got := client.Options().Retryer.MaxAttempts(); got != 5 {
		t.Errorf("without retry backoff, Retryer.MaxAttempts() = %d, want 5", got)
	}
}

// Leaving AWS_RETRY_MAXIMUM_ATTEMPTS unset must not change the effective number
// of attempts. The custom retryer has always been wrapped by
// finalizeRetryMaxAttempts with AWS_RETRY_MAX_ATTEMPTS, so lifting that cap
// unconditionally would silently double the default from 5 to 10.
func TestRetryMaximumAttempts_DefaultIsUnchanged(t *testing.T) {
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv(EnvAWSRetryMaxAttempts, "")
	t.Setenv(EnvAWSRetryMaximumAttempts, "")

	s := &service{Region: "us-east-1"}

	client, err := s.newInstance(context.Background(), true)
	if err != nil {
		t.Fatalf("newInstance() error = %v", err)
	}
	if got := client.Options().Retryer.MaxAttempts(); got != AWSRetryMaxAttempts {
		t.Errorf("with no overrides, Retryer.MaxAttempts() = %d, want %d", got, AWSRetryMaxAttempts)
	}
}

// A malformed override is not an override, so the cap must stay in place.
func TestRetryMaximumAttempts_MalformedOverrideKeepsCap(t *testing.T) {
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv(EnvAWSRetryMaximumAttempts, "not-a-number")

	s := &service{Region: "us-east-1"}

	client, err := s.newInstance(context.Background(), true)
	if err != nil {
		t.Fatalf("newInstance() error = %v", err)
	}
	if got := client.Options().Retryer.MaxAttempts(); got != AWSRetryMaxAttempts {
		t.Errorf("with a malformed override, Retryer.MaxAttempts() = %d, want %d", got, AWSRetryMaxAttempts)
	}
}
