// A backend that sends server events to a website's analytics.
//
//	RAVENSIGHT_SERVER_KEY=gt_srv_... go run ./examples/backend
//
// Without RAVENSIGHT_SERVER_KEY the Server is inert: every call returns a
// refusal and nothing is sent.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/Reality-Software-Entertainment/ravensight-go"
)

func main() {
	srv := ravensight.NewServer(ravensight.ServerConfig{
		ServerKey: os.Getenv("RAVENSIGHT_SERVER_KEY"),
		Optional:  true,
	})
	defer srv.Close()

	// Your own opaque, random id for the account, never an email.
	actor := "acct_7f3a9c1e2b4d"

	// Queued and batched in the background.
	if err := srv.Track(ravensight.ServerEvent{
		Name:       "match_finished",
		ActorID:    actor,
		Properties: map[string]any{"mode": "ranked", "won": true, "minutes": 14},
	}); err != nil {
		report(err)
	}

	// Sent now, waiting for the answer.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := srv.Send(ctx, ravensight.ServerEvent{Name: "signup_completed", ActorID: actor})
	if err != nil {
		report(err)
		return
	}
	fmt.Printf("accepted %d, duplicates %d\n", result.Accepted, result.Duplicates)
}

func report(err error) {
	var refused *ravensight.RefusalError
	var invalid *ravensight.ValidationError
	switch {
	case errors.As(err, &invalid):
		fmt.Fprintln(os.Stderr, "fix the event:", invalid)
	case errors.As(err, &refused):
		fmt.Fprintln(os.Stderr, "not sent:", refused.Code)
	default:
		fmt.Fprintln(os.Stderr, err)
	}
}
