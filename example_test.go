package ravensight_test

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/Reality-Software-Entertainment/ravensight-go"
)

func Example() {
	client := ravensight.New(ravensight.Config{
		IngestKey:   os.Getenv("RAVENSIGHT_KEY"), // empty: inert, sends nothing
		Optional:    true,
		GameVersion: "1.0.0",
	})
	defer client.Close()
	defer client.Recover()

	client.Track("level_completed", map[string]any{"level": 3, "deaths": 2})
}

func ExampleClient_LogHandler() {
	client := ravensight.New(ravensight.Config{IngestKey: os.Getenv("RAVENSIGHT_KEY"), Optional: true})
	defer client.Close()

	slog.SetDefault(slog.New(client.LogHandler(slog.NewTextHandler(os.Stderr, nil))))
	httpClient := &http.Client{Transport: client.WrapTransport(http.DefaultTransport)}
	_ = httpClient
}

func ExampleClient_Identify() {
	client := ravensight.New(ravensight.Config{IngestKey: os.Getenv("RAVENSIGHT_KEY"), Optional: true})
	defer client.Close()

	// After your own sign-in, with a random id stored with the account:
	if err := client.Identify("3f2a9c1e-7b4d-4e8a-9c3f-1a2b3c4d5e6f"); err != nil {
		return // *ravensight.RefusalError: empty, too_long or looks_like_email
	}
	// On sign-out:
	client.ResetIdentity()
}

func ExampleServer() {
	srv := ravensight.NewServer(ravensight.ServerConfig{ServerKey: os.Getenv("RAVENSIGHT_SERVER_KEY"), Optional: true})
	defer srv.Close()

	_ = srv.Track(ravensight.ServerEvent{Name: "match_finished", ActorID: "acct_7f3a9c1e2b4d"})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = srv.Send(ctx, ravensight.ServerEvent{Name: "signup_completed", ActorID: "acct_7f3a9c1e2b4d"})
}
