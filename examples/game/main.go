// A small stand-in for a game loop that reports to Ravensight.
//
//	RAVENSIGHT_KEY=gt_live_... go run ./examples/game
//
// Without RAVENSIGHT_KEY the client is inert: the program runs and sends
// nothing.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/Reality-Software-Entertainment/ravensight-go"
)

func main() {
	client := ravensight.New(ravensight.Config{
		IngestKey:   os.Getenv("RAVENSIGHT_KEY"),
		Optional:    true,
		GameVersion: "0.1.0",
	})
	defer client.Close()
	defer client.Recover()

	// The game's own log feeds error capture; every record still prints.
	logger := slog.New(client.LogHandler(slog.NewTextHandler(os.Stderr, nil)))

	speed := client.ConfigNumber("enemy_speed", 2.0)
	for level := 1; level <= 3; level++ {
		client.TrackProgression(ravensight.Progression{Status: "start", P1: fmt.Sprintf("level_%d", level)})
		time.Sleep(50 * time.Millisecond) // a frame or two of play
		if level == 2 {
			logger.Error("boss script failed", "err", errors.New("missing animation"))
		}
		client.TrackProgression(ravensight.Progression{Status: "complete", P1: fmt.Sprintf("level_%d", level)})
	}
	client.Track("session_summary", map[string]any{"enemy_speed": speed, "levels": 3})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Flush(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "not everything was sent:", err)
	}
	fmt.Println("status:", statusText(client.Status()))
}

func statusText(s ravensight.Status) string {
	if s == ravensight.StatusOK {
		return "ok"
	}
	return string(s)
}
