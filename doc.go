// Package ravensight sends a Go game's player analytics, and a Go server's
// events, to Ravensight (https://ravensight.io).
//
// It has two modes in one package:
//
//   - A Client, for a game, with the game's publishable gt_live_ or gt_pub_
//     key: sessions, batched events with retries and a saved offline queue,
//     the player's opt-out and consent, a random device id with a reset,
//     players who sign in, campaign links, remote config with experiment
//     exposure, feedback, and automatic error capture. Track never waits on
//     the network or the disk, so it is safe to call from a game loop.
//   - A Server, for your own game server or backend, with a website's
//     secret gt_srv_ server key: server events, validated on your side,
//     idempotent, with identity passed explicitly per event and never a
//     player's device id.
//
// Without a key either one is inert, so a public repository can ship with no
// key in it. The standard library is the only dependency.
//
//	client := ravensight.New(ravensight.Config{IngestKey: key, GameVersion: "1.0.0"})
//	defer client.Close()
//	defer client.Recover()
//	client.Track("level_completed", map[string]any{"level": 3})
package ravensight
