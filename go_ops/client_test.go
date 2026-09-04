package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestParseLatestWindow(t *testing.T) {
	got, err := ParseLatestWindow("2026-09-02T07:22:00")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := time.Date(2026, 9, 2, 7, 22, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestStaleSeconds(t *testing.T) {
	latest := time.Date(2026, 9, 2, 7, 22, 0, 0, time.UTC)
	now := time.Date(2026, 9, 2, 7, 23, 30, 0, time.UTC)
	if got := StaleSeconds(latest, now, time.Minute); got != 30 {
		t.Fatalf("got %v want 30", got)
	}
	nowWithinWindow := time.Date(2026, 9, 2, 7, 22, 30, 0, time.UTC)
	if got := StaleSeconds(latest, nowWithinWindow, time.Minute); got != 0 {
		t.Fatalf("got %v want 0", got)
	}
}

func TestTopics(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/health/topics" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"topics":[{"topic":"clicks.raw","partitions":3},{"topic":"clicks.dlq","partitions":3}]}`))
	}))
	defer server.Close()

	client := NewAPIClient(server.URL)
	topics, err := client.Topics(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(topics) != 2 || topics[0].Topic != "clicks.raw" || topics[0].Partitions != 3 {
		t.Fatalf("unexpected topics: %+v", topics)
	}
}

func TestFetchSummaryAndFreshness(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/summary" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"latest_window":"2026-09-02T07:22:00"}`))
	}))
	defer server.Close()

	client := NewAPIClient(server.URL)
	summary, err := client.FetchSummary(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	latest, err := ParseLatestWindow(summary.LatestWindow)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	now := time.Date(2026, 9, 2, 7, 24, 0, 0, time.UTC)
	if got := StaleSeconds(latest, now, time.Minute); got != 60 {
		t.Fatalf("got %v want 60", got)
	}
}

func TestHealthFailsOn503(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"detail":"not ready"}`, http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client := NewAPIClient(server.URL)
	if _, err := client.Health(context.Background()); err == nil {
		t.Fatal("expected error for 503 ready check")
	}
}
