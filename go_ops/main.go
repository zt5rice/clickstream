// Command go_ops is a read-only control-plane CLI for the clickstream API.
//
// Usage:
//
//	go_ops health     --base-url http://localhost:8000
//	go_ops topics     --base-url http://localhost:8000
//	go_ops freshness  --base-url http://localhost:8000
//	go_ops status     --base-url http://localhost:8000
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: go_ops <health|topics|freshness|status> [--base-url URL]")
		os.Exit(2)
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	baseURL := fs.String("base-url", "http://localhost:8000", "clickstream API base URL")
	_ = fs.Parse(os.Args[2:])

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := NewAPIClient(*baseURL)

	var runErr error
	switch cmd {
	case "health":
		runErr = runHealth(ctx, client)
	case "topics":
		runErr = runTopics(ctx, client)
	case "freshness":
		runErr = runFreshness(ctx, client)
	case "status":
		runErr = runStatus(ctx, client)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", cmd)
		os.Exit(2)
	}
	if runErr != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", runErr)
		os.Exit(1)
	}
}

func runHealth(ctx context.Context, client *APIClient) error {
	ready, err := client.Health(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("ready=%v\n", ready)
	return nil
}

func runTopics(ctx context.Context, client *APIClient) error {
	topics, err := client.Topics(ctx)
	if err != nil {
		return err
	}
	for _, t := range topics {
		fmt.Printf("%s\tpartitions=%d\n", t.Topic, t.Partitions)
	}
	return nil
}

func runFreshness(ctx context.Context, client *APIClient) error {
	summary, err := client.FetchSummary(ctx)
	if err != nil {
		return err
	}
	latest, err := ParseLatestWindow(summary.LatestWindow)
	if err != nil {
		return fmt.Errorf("parse latest_window %q: %w", summary.LatestWindow, err)
	}
	stale := StaleSeconds(latest, time.Now().UTC(), time.Minute)
	fmt.Printf("latest_window=%s stale_seconds=%.1f\n", latest.Format(time.RFC3339), stale)
	if stale > 180 {
		return fmt.Errorf("pipeline is stale: %.1f seconds behind", stale)
	}
	return nil
}

func runStatus(ctx context.Context, client *APIClient) error {
	if err := runHealth(ctx, client); err != nil {
		return err
	}
	if err := runTopics(ctx, client); err != nil {
		return err
	}
	return runFreshness(ctx, client)
}
