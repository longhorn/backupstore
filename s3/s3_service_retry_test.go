package s3

import (
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
			if got := retryMaximumAttempts(); got != tc.want {
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
