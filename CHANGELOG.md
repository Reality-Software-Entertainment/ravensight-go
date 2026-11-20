# Changelog

## 0.1.0 - 2026-11-20

First release, a preview. Go 1.22 or newer, standard library only.

### Added

- `Client`, for Go games, with a publishable `gt_live_` or public `gt_pub_`
  key. Inert without a key: no goroutine, no file, no request.
- Sessions, `game_started` and `game_exited`, batched events with
  `rs_event_id`, the delivery contract's answer table, a backoff of 10
  seconds doubling to 5 minutes with jitter, `Retry-After` waited exactly,
  the 400 and 413 splits, gzip from 1,024 bytes, signed batches when the
  session signs, and the 8 KB event cap with an error's text counted one
  byte a character.
- One goroutine per client and a bounded queue (500 events): `Track` copies
  and serializes the data and returns, never waiting on the network or the
  disk. `Flush(ctx)`, `Close()` and `Shutdown(ctx)`.
- The saved queue (version 2 format, one file per key, temporary file and
  rename, the session origin per group, 89 days).
- Opt-out and opt-in consent, saved across launches, with
  `SetTrackingEnabled` before a client exists; `ResetDeviceID`; the runtime
  switch; status reasons and the invalid key stop with one warning; the
  kill switch.
- `Identify`, `ResetIdentity` and `IsIdentified`: only an HMAC-SHA256 hash
  of the account id, keyed with the game's salt from the session answer, is
  sent, once per session.
- `SetCampaignLink` and `Config.CampaignLink`: sent once, lowercased, on the
  next session the client opens for its own player.
- Remote config reads with fallbacks, a scoped disk cache with its ETag, and
  experiment exposure tagging after the first typed read.
- `SubmitFeedback` with local validation.
- Typed purchase, resource, progression and error events.
- Automatic error capture: `Recover` records a panic for the next launch,
  `LogHandler` and `Log` feed a `log/slog` log, `WrapTransport` captures
  the game's failed HTTP requests. The game's switch, the boot hold, device
  caps, device scrubbing (scrub vectors version 6), the opt-in trail, and
  gameplay events ahead of captures.
- The playtest path, from the environment or the command line.
- `JoinToken` for a Go game server that shares the player's session.
- `Server`, for Go game servers and backends, with a website's secret
  `gt_srv_` key: server events validated on your side, idempotent event
  ids, explicit actor ids, retries, pauses kept for later, `Send` and
  `Track`.
- A small package-level convenience: `Init`, `Default`, `Track`, `Close`.
- Tests against a fake HTTP server, with the platform's scrub, identify and
  delivery vectors. CI runs nightly on weekdays and on demand on Go 1.22 and
  the current release; `.githooks/pre-push` runs gofmt, `go vet` and
  `go test -race` before a push.
