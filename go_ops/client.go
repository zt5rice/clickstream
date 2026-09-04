// Package main implements a small read-only control-plane CLI for the clickstream
// FastAPI (P2-06). It talks to /health, /ready, /api/v1/health/topics and
// /api/v1/summary and is designed to be script-friendly (exit codes).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// APIClient talks to the clickstream read-only API.
type APIClient struct {
	BaseURL string
	HTTP    *http.Client
}

// NewAPIClient returns a client with a sane timeout.
func NewAPIClient(baseURL string) *APIClient {
	return &APIClient{
		BaseURL: strings.TrimRight(baseURL, "/"),
		HTTP:    &http.Client{Timeout: 5 * time.Second},
	}
}

// Health reports whether the API is live and ready.
func (c *APIClient) Health(ctx context.Context) (ready bool, err error) {
	if err := c.getJSON(ctx, "/ready", &map[string]any{}); err != nil {
		return false, fmt.Errorf("ready check failed: %w", err)
	}
	return true, nil
}

// Topic is one Kafka topic reported by the API.
type Topic struct {
	Topic      string `json:"topic"`
	Partitions int    `json:"partitions"`
}

// Topics lists the Kafka topics known to the API.
func (c *APIClient) Topics(ctx context.Context) ([]Topic, error) {
	var body struct {
		Topics []Topic `json:"topics"`
	}
	if err := c.getJSON(ctx, "/api/v1/health/topics", &body); err != nil {
		return nil, err
	}
	return body.Topics, nil
}

// Summary is the subset of /api/v1/summary that freshness needs.
type Summary struct {
	LatestWindow string `json:"latest_window"`
}

// FetchSummary returns the summary payload from the API.
func (c *APIClient) FetchSummary(ctx context.Context) (Summary, error) {
	var s Summary
	if err := c.getJSON(ctx, "/api/v1/summary", &s); err != nil {
		return s, err
	}
	return s, nil
}

// ParseLatestWindow parses the API's naive-UTC window timestamp.
func ParseLatestWindow(value string) (time.Time, error) {
	return time.Parse("2006-01-02T15:04:05", value)
}

// StaleSeconds reports how far behind real time the latest curated window is.
func StaleSeconds(latest time.Time, now time.Time, window time.Duration) float64 {
	age := now.Sub(latest)
	stale := age - window
	if stale < 0 {
		return 0
	}
	return stale.Seconds()
}

func (c *APIClient) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
