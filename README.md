# Ravensight for Go

Player analytics for games written in Go, and server events for Go game
servers and backends, sent to [Ravensight](https://ravensight.io). One
module, standard library only.

Preview: version 0.1.0. Tested with the Go toolchain's race detector against
a fake HTTP server on macOS (see "Running the tests"). It has not yet
shipped inside a released game. Read the code before you ship it in a
release build, and please open an issue with anything you hit.

* Two modes: a `Client` for a game, with a publishable game key, and a
  `Server` for your own server, with a website's secret server key.
* Go 1.22 or newer. No dependencies outside the standard library.
* Docs: https://ravensight.io/docs/go-sdk/
* Privacy: a random per-install device id, no advertising identifier, a
  saved player opt-out and a reset. What your store label can say:
  https://ravensight.io/docs/#privacy-label

## Install

```sh
go get github.com/Reality-Software-Entertainment/ravensight-go@v0.1.0
```

```go
import "github.com/Reality-Software-Entertainment/ravensight-go"
```

The package name is `ravensight`.

## Quickstart: a game

```go
client := ravensight.New(ravensight.Config{
	IngestKey:   "gt_live_your_key", // from the Ravensight dashboard
	GameVersion: "1.0.0",
})
defer client.Close()     // records game_exited and sends what is left
defer client.Recover()   // records a panic for the next launch, then panics again

client.Track("level_completed", map[string]any{
	"level":   3,
	"deaths":  2,
	"seconds": 41.7,
})
```

That is the whole setup. The client reads the server's kill switch, opens
a session and sends batches from its own goroutine. `Track` copies and
serializes your data at once and returns; it never waits on the network or
the disk, so it is safe to call from your game loop (an Ebitengine `Update`,
say). The device id is saved in a per-user directory, so the same install
counts as the same player across launches.

There is also a small package-level convenience for a game that wants one
client and no plumbing:

```go
ravensight.Init(ravensight.Config{IngestKey: key})
defer ravensight.Close()
ravensight.Track("level_start", map[string]any{"level": "forest_1"})
ravensight.Default().Identify(account.AnalyticsID) // every other call
```

## Quickstart: a game server or backend

```go
srv := ravensight.NewServer(ravensight.ServerConfig{
	ServerKey: os.Getenv("RAVENSIGHT_SERVER_KEY"), // gt_srv_..., never in a game build
})
defer srv.Close()

// In the background, batched:
err := srv.Track(ravensight.ServerEvent{
	Name:       "match_finished",
	ActorID:    account.AnalyticsID, // your own opaque id, passed explicitly
	Properties: map[string]any{"mode": "ranked", "won": true},
})

// Or now, waiting for the answer:
result, err := srv.Send(ctx, ravensight.ServerEvent{Name: "signup_completed", ActorID: id})
```

See "Server mode" below.

## The client's calls

| Call | What it does |
| --- | --- |
| `ravensight.New(cfg)` | Makes a client and starts its goroutine. Never fails: without a key it is inert. |
| `client.Track(name, data)` | Queues an event. Returns false when it was not queued. Never blocks. |
| `client.TrackPurchase`, `TrackResource`, `TrackProgression`, `TrackError` | Typed events (see "Typed events"). |
| `client.Flush(ctx)` | Sends what is queued and waits until the queue is empty, nothing more can be sent for now, or `ctx` ends. |
| `client.Close()` | Records `game_exited`, sends for at most about 1.5 seconds, saves what is left for the next launch, stops. |
| `client.Shutdown(ctx)` | `Close` with your own time limit. |
| `client.SetTrackingEnabled(on)` | The player's analytics choice, saved and remembered across launches. |
| `client.TrackingEnabled()` | Whether the player's saved choice allows collecting. |
| `client.ResetDeviceID()` | A fresh random device id from now on. Returns the new id. |
| `client.SetEnabled(on)`, `IsEnabled()` | A runtime switch for your own logic. Not saved. |
| `client.Identify(accountID)` | Links this device to your game's own account (see "Players who sign in"). |
| `client.ResetIdentity()`, `IsIdentified()` | Sign-out, and whether an account is set. |
| `client.SetCampaignLink(linkID)` | Passes on the campaign link the game was opened with (see "Campaign links"). |
| `client.ConfigBool`, `ConfigNumber`, `ConfigString`, `ConfigJSON` | Remote config reads with a fallback (see "Remote config and experiments"). |
| `client.FetchConfig()` | Asks for fresh remote config now. |
| `client.SubmitFeedback(ctx, fb)` | Sends player feedback and waits for the answer. |
| `client.Recover()` | Defer it: records a panic for the next launch (see "Errors"). |
| `client.LogHandler(next)`, `client.Log(level, msg)` | Feed your game's log to error capture. |
| `client.WrapTransport(rt)` | Captures your game's own failed HTTP requests. |
| `client.SetErrorCapture(on)`, `IsErrorCaptureActive()` | Automatic capture for this run. |
| `client.Status()`, `IsCollecting()` | Why the client is or is not sending. |
| `client.DeviceID()`, `SessionToken()`, `IsPlaytest()`, `Stats()` | Read-only state. |
| `ravensight.SetTrackingEnabled(cfg, on)` | Saves the player's choice before a client exists. |
| `ravensight.DefaultStorageDir(key)` | Where a client keeps its files by default. |

Every method is safe from any goroutine and safe on a nil `*Client`, where
it does nothing.

Event data is a `map[string]any` of strings, numbers, bools, nil or
anything `encoding/json` can write. Keys that start with `rs_` are reserved
for Ravensight. An event's name must not be empty and is at most 200
characters; its data is at most 8 KB as JSON, else `Track` returns false,
drops it and logs one warning for the run.

## Configuration

| Field | Default | Meaning |
| --- | --- | --- |
| `IngestKey` | empty | Your publishable `gt_live_` key, or a public `gt_pub_` key. Empty makes the client inert. |
| `Optional` | false | Silences the one warning an inert client logs. |
| `APIURL` | `https://api.ravensight.io` | With or without `/api/v1`; trailing slashes are ignored. |
| `GameVersion` | `1.0.0` | Sent on every session. |
| `Platform` | the build's OS | `windows`, `macos`, `linux`, `android`, `ios` or `web`. |
| `Distribution` | empty | A short label for this build, such as `steam` or `itch-demo`. At most 40 characters. |
| `Device` | empty | Optional `Model`, `OSVersion` and `GPU`, sent once on the session. The SDK never probes for them. |
| `DeviceID` | empty | Your own stable per-install id. Empty uses a random saved `dev_` id. |
| `StorageDir` | per-user | Where the SDK keeps its files (see "Device id and storage"). |
| `Consent` | `ConsentOptOut` | `ConsentOptIn` sends nothing, and makes no device id, until `SetTrackingEnabled(true)`. |
| `StartDisabled` | false | Start switched off, as `SetEnabled(false)`. |
| `MaxQueueSize` | 500 | Unsent events kept, oldest dropped first. At most 500. |
| `FlushInterval` | 5 s | How long an event may wait to be batched. Negative sends only on `Flush`, at 50 events and on `Close`. |
| `RequestTimeout` | 15 s | Per request. |
| `NoLifecycleEvents` | false | Turns off `game_started` and `game_exited`. |
| `NoErrorCapture` | false | Turns off automatic error capture. |
| `MemoryOnlyQueue` | false | Keeps unsent events in memory only. |
| `JoinToken` | empty | A session token from the player's client (see "Dedicated servers"). |
| `CampaignLink` | empty | A campaign link id the game was opened with. |
| `Playtest` | empty | Overrides the playtest values read from the environment and command line. |
| `HTTPClient` | the SDK's own | Sends the requests. |
| `Logger` | warnings to stderr | Receives the SDK's own lines, each starting `Ravensight:`. |
| `OnStatus` | none | Called when `Status()` changes. Keep it quick. |
| `OnConfigReady` | none | Called after each remote config fetch. |

## Automatic events

Two, unless `NoLifecycleEvents` is set:

* `game_started`, queued once when the client may first collect, so it is
  the first event and carries the real start time.
* `game_exited`, queued once by `Close` or `Shutdown`.

The SDK has no engine hooks, so it does not see a mobile app go to the
background. Call `client.Flush(ctx)` from your own pause handler.

## Status

`client.Status()` returns a `ravensight.Status`, with the codes every
Ravensight SDK shares:

| Status | Meaning |
| --- | --- |
| `StatusOK` (empty) | Sending, or about to. |
| `no_key` | Made with no key. Inert for its lifetime. |
| `disabled` | Switched off, opted out, or waiting for an opt-in yes. Nothing touches the network. |
| `invalid_key` | The key was refused. Logged once as `Ravensight: ingest key refused (invalid_key), sending stopped for this run. Check your gt_live_ key.` Nothing more is sent this run. |
| `tracking_disabled` | The game's kill switch on the server. Nothing more is sent this run. |
| `offline` | No connection, a timeout or a server error. Retried with a backoff. |
| `rate_limited` | Waiting out the server's `Retry-After`. |
| `session_failed` | The session could not be opened, or a playtest token was refused. |
| `playtest_job_closed` | The playtest job finished. Nothing more is sent this run. |

## Public builds

* **No key, no analytics.** A client made with an empty `IngestKey` is
  inert: no goroutine, no file, no request, and every call is a quiet
  no-op. So a public repository can ship with no key in it. Set
  `Optional: true` to silence its one warning.
* **Inject the key at build time, never commit it.** Use a public `gt_pub_`
  key for builds anyone can download, set by your release build:

  ```go
  // go build -ldflags "-X main.ravensightKey=gt_pub_..."
  var ravensightKey string

  client := ravensight.New(ravensight.Config{IngestKey: ravensightKey, Optional: true})
  ```

* **The game key is publishable.** It can open sessions, send events and
  feedback, read the kill switch and read this game's public config. It
  cannot read analytics, read feedback or touch your account. Rotate it
  from the dashboard at any time. A `gt_srv_` server key is a secret and
  never belongs in a game build (see "Server mode").
* **`ConsentOptIn`** sends nothing until the player says yes: no device id
  is made and not even the kill switch is read before
  `SetTrackingEnabled(true)`. Events tracked before the yes are dropped,
  not held.

## How delivery works

The client follows the Ravensight delivery contract, so an event survives a
lost connection, a crash, a kill and a build update.

* **Batching.** `Track` only queues. The client sends on its 5 second
  timer, as soon as 50 events wait, on `Flush` and on `Close`, at most 50
  events per request.
* **Bounded.** At most 500 events wait. At the cap the oldest go first:
  automatic error captures before gameplay events.
* **Saved queue.** Unsent events are kept in `queue-<16 hex>.json` in the
  storage directory (64-bit FNV-1a of the key), written through a
  temporary file and a rename about a second after a change and right
  after every answer, and deleted when empty. The next launch sends what is
  left. Events older than 89 days are dropped when it is read.
* **Original session.** The file keeps, per group, the opaque `origin` the
  session answer gave. An event sent under a later session carries it, so
  the server files it under the session, build and platform it happened in.
  The origin is kept only in that file.
* **Event ids.** Every event carries `rs_event_id`, kept across resends, so
  a resend after a lost answer is recognised as a duplicate.
* **Answers.** An event leaves the queue only on a `2xx` answer or a `400`
  that names it. A `400 batch_too_large` or `413` halves the batch (a single
  event still too large is dropped); any other `400` drops the request's
  events. A `401` opens a new session once and resends the same ids; a
  second in a row waits for the backoff. A `403` stops sending for the run
  and keeps the events for the next launch. `429`, or `503` with
  `Retry-After`, waits exactly that long. Anything else keeps the events.
* **Backoff.** 10 seconds, doubling to 5 minutes, each wait multiplied by a
  random factor from 0.5 up to 1. `Flush` and `Close` cut a backoff short,
  never a `Retry-After`.
* **Compression.** A body of 1,024 bytes or more is sent gzip compressed.
* **Signed requests.** When the session answer carries a signing secret
  (public `gt_pub_` keys), every batch is signed over the uncompressed body,
  with the device clock corrected by the server's time. The secret is kept
  in memory only.

## Typed events

```go
// Money is whole minor units: 499 is 4.99. Default name: purchase.
client.TrackPurchase(ravensight.Purchase{Amount: 499, Currency: "usd", ItemType: "pack", ItemID: "gems_500"})

// Default name: resource_source or resource_sink.
client.TrackResource(ravensight.Resource{Flow: "sink", Resource: "gold", Amount: 120, ItemType: "weapon"})

// Default name: progression_start, progression_complete or progression_fail.
score := 1250.5
client.TrackProgression(ravensight.Progression{Status: "complete", P1: "world1", P2: "level3", Score: &score})

// Severity is debug, info, warning, error or critical. Default name: error.
client.TrackError(ravensight.ErrorReport{Severity: "error", Message: "save file unreadable"})
```

Each takes `Extra` properties (a reserved key always wins) and a `Name` that
replaces the default. To mark a level or death event without a typed
helper, set `rs_kind` to `level_start`, `level_complete`, `death` or `none`.

## Errors

An error is an ordinary event on your usage balance; spend is bounded on
the device by the caps below.

| What | How it starts | What is sent |
| --- | --- | --- |
| A panic | `defer client.Recover()` at the top of `main` and of each goroutine you start. | On the next launch: `rs_source: "crash"`, `rs_severity: "critical"`, the panic's type, its message and the stack. |
| Your game's log | `slog.New(client.LogHandler(yourHandler))`, or `client.Log(level, msg)` from your own logger. | A record at `slog.LevelError` or above: `rs_source: "log"`, `rs_severity: "warning"`. Every record feeds the trail. |
| Your game's HTTP requests | `&http.Client{Transport: client.WrapTransport(http.DefaultTransport)}` | A network failure or `5xx` as `error`, a `4xx` as `warning`, `rs_source: "request"`, message `<METHOD> <host><path> <status>`. |
| `client.TrackError(...)` | Your call. | As you wrote it, never held back by the switches below. |

**Recover.** It writes one small file synchronously (readable by the
player's account only), then panics again with the same value, so the game
ends exactly as it would have. The next launch sends it once. A record
older than 30 days is deleted unsent; at most 5 are kept. Go cannot recover
a fatal runtime error (a concurrent map write, running out of memory), and
a panic in a goroutine without its own `defer client.Recover()` is not
recorded.

**The gate.** Nothing is captured without a key, before consent, while the
game's Errors switch on the server is off (`errorCapture.enabled` on the
session answer; an answer without it means on), with `NoErrorCapture`, or
after `SetErrorCapture(false)`. With a key and consent, capture starts
before the first answer: up to 5 captures wait in memory and are sent if it
says on, discarded unsent if it says off.

**Caps and scrubbing.** Per run, dropped silently on the device: 30
captures, 3 per local key (type, message with numbers, ids and quoted text
templated away, and the first stack line), 6 a minute, 10 trails. Text is
scrubbed on the device before it is queued, with the server's rules:
emails become `[email]`; `Bearer` tokens, JWT-shaped strings, `gt_` keys
and runs of 32 or more of `A-Z a-z 0-9 + / = _ -` that contain a digit
become `[token]` (a run that is a file path is kept); IP addresses become
`[ip]`; home folders (`/Users/<name>`, `/home/<name>`, `C:\Users\<name>`)
become `~`; a URL loses its query, fragment and `user:password@`. The
message is cut to 200 characters, the stack to 2,000 and the type to 100.

**The trail** is an in-memory ring of the last 20 lines (log records at any
level, and the names of events you track), `+<ms since start> <level>
<text>`, each scrubbed and cut to 120 characters, at most 1,500 in all. It
is sent only with an error, only while the game's trail switch is on (off
by default), and never written to the queue file. A crash record keeps the
trail only while the switch is on.

**Never sent:** request bodies, headers, query strings, the SDK's own log
lines (anything starting `Ravensight`) and requests, and `rs_fp` (the
server computes the fingerprint). Gameplay events always go ahead of
captured errors.

## Players who sign in

If your game has its own login, call `Identify` once the player signs in
and `ResetIdentity` when they sign out. One person on two devices, or
before and after signing in, then counts as one player.

```go
// after your own sign-in: a random id stored with the account
if err := client.Identify(account.AnalyticsID); err != nil {
	// *ravensight.RefusalError: empty, too_long or looks_like_email
}

// on sign-out
client.ResetIdentity()
```

* **What is sent:** a salted hash of the id (`accountHash`, 64 hex
  characters, HMAC-SHA256 keyed with your game's own salt from the session
  answer) and the session token, once per session. The raw account id is
  never sent and never saved; until the first session answer it is held in
  memory only.
* **Pass a random, high-entropy id**, such as a random UUID stored with the
  account, never a sequential database key, an email, a name or a handle.
  Your game's salt reaches every copy of the game, so anyone could hash 1,
  2, 3 and onward and match a guessable id. The hash is still a
  pseudonymous identifier for the player: declare it in your privacy
  notice.
* **Refused:** an empty id (`empty`), more than 256 bytes of UTF-8
  (`too_long`), or any `@` (`looks_like_email`). The id is used exactly as
  given, never trimmed. A refusal sends nothing and changes nothing.
* **One event per identified session** on your usage balance. The same
  account again sends nothing. A failed request is tried again at the next
  session, never in a loop. When the server does not offer identify, it is
  off until the next launch.
* **A different account** first runs `ResetIdentity`, which deletes the
  saved hash and switches to a fresh device id, so the next player is not
  linked to the last one. `ResetDeviceID` alone keeps the identity.
* The hash is saved as `identity.json` beside the device id. Without a
  key, before consent, while switched off, or in a playtest run it is held
  in memory only, and an opt-out deletes it.

## Campaign links

A Ravensight campaign link adds `rs_link=<link id>` to the address it sends
players to. Pass it on from the address your game was opened with (a custom
URL scheme or a deep link your game already handles); the SDK never reads
the command line for it.

```go
if link := openedURL.Query().Get("rs_link"); link != "" {
	_ = client.SetCampaignLink(link)
}
```

`SetCampaignLink` accepts 24 hex characters in any case and returns a
`*RefusalError` (`invalid_link`) for anything else. The id goes lowercased,
as `campaignLink`, on the next session the client opens for its own player,
and is cleared once a session answers with a token. Set it before the
first session with `Config.CampaignLink`. It is kept in memory only and is
never sent without consent, without a key or in a playtest run.

## Remote config and experiments

Every session start fetches the game's public config; `FetchConfig` asks
again, and `OnConfigReady` says when an answer arrived.

```go
speed := client.ConfigNumber("enemy_speed", 2.0)
hint := client.ConfigBool("show_hint", true)
label := client.ConfigString("welcome_text", "Welcome")
layout := client.ConfigJSON("layout", json.RawMessage(`{}`))
```

Each read returns the fallback before the first answer, offline, or when
the value is another type; values are never converted. The last good answer
is cached on disk for the same key, device, platform and build, and its
ETag is reused. A fetch alone is not an exposure: the first typed read of
any key marks every experiment the last fetch assigned as exposed, queues
one `rs_exposure` event, and every event queued after it carries
`rs_exp_<experiment>` = `<variant>`. An experiment missing from a later
fetch is no longer tagged. A run is one client; `ResetDeviceID` starts a
new one. Do not log or show the device id beside config values. Config is
public game data, never a place for secrets. A fresh fetch costs one
ordinary event; a confirmed cache costs none.

## Feedback

```go
err := client.SubmitFeedback(ctx, ravensight.Feedback{
	Message:  "The boss fight drags",
	Category: "suggestion", // optional: bug, suggestion, complaint, praise, playtest, other
	Rating:   3,            // 0 to leave it out, or 1 to 5
})
```

Bad input is refused on the spot with no request: `empty_message`,
`message_too_long` (over 2,000 characters), `invalid_category` or
`invalid_rating`. While the client is not sending, the error's code is the
status, such as `disabled`. Feedback is never retried on its own.

## Device id and storage

The SDK makes a random device id, `dev_` and 32 hex characters from the
system's random source, and saves it once the player has not opted out.
By default it lives in a per-user directory named after your key:

| Platform | Default storage directory |
| --- | --- |
| Windows | `%APPDATA%\ravensight\<hash>` |
| macOS | `~/Library/Application Support/ravensight/<hash>` |
| Linux and others | `$XDG_DATA_HOME/ravensight/<hash>`, else `~/.local/share/ravensight/<hash>` |

`<hash>` is the first 12 hex characters of the SHA-256 of the key. Set
`StorageDir` where there is no such directory (a browser build, a console).
If no directory can be used the SDK logs one warning and keeps everything
in memory for the run.

The directory holds `device_id`, `state.json` (the player's choice),
`identity.json`, the queue file, `config.json` (the config cache) and
pending crash records. Keep it out of cloud saves and device backups: the
queue file holds scrubbed error captures and origin stamps (a credential
for an earlier session of this device).

No hardware identifier, advertising id or personal data is collected by the
SDK itself. Only the events you choose to send leave the machine, plus the
optional device fields you set. Country is never sent by the SDK.

## Opting out and resetting

```go
client.SetTrackingEnabled(false) // saved at once; nothing more is sent
client.SetTrackingEnabled(true)  // back on, also saved

fresh := client.ResetDeviceID()  // a new identity; the queue is cleared
```

`SetTrackingEnabled(false)` saves the choice before it returns, clears the
queue and deletes its file, pending crash records, the stored identity and
the config cache, and stops every request. Every later client with the same
storage reads it, and a saved opt-out wins: such a client makes no request
of any kind. To save the choice before a client exists, call
`ravensight.SetTrackingEnabled(cfg, false)` with the config you will use.

`ResetDeviceID` makes and saves a new id, clears the queue and drops the
session, so nothing links the old id to the new one. `SetEnabled(false)` is
a runtime switch for your own logic; it is not saved.

A game that sends gameplay events and nothing else can declare Device ID and
Product Interaction as not linked to the player and not used for tracking,
and needs no tracking permission prompt.

## Telling players

You do not need a permission prompt, but saying what the game sends is good
manners. A paragraph your game can show, in a settings screen or on its
store page:

> This game sends anonymous gameplay events (what happens in the game, never
> who you are) to Ravensight so we can improve it. You can turn this off in
> Settings at any time.

If your game calls `Identify`, its events are linked to a pseudonymous hash
of the player's account, so say that instead of "anonymous" and declare the
hash in your privacy notice.

## Dedicated servers

A game server written in Go can share the player's session, so its events
land in the same session as the player's own instead of counting a second
player. The player's client reads its token and sends it to your server
however your game talks to it:

```go
token := client.SessionToken() // on the player's client; empty until a session opens

// On the game server, one client per player connection:
c := ravensight.New(ravensight.Config{
	IngestKey:         key,
	JoinToken:         token,
	NoLifecycleEvents: true, // the player's client sends those
	MemoryOnlyQueue:   true,
	StorageDir:        perPlayerDir,
})
c.Track("match_result", map[string]any{"won": true})
```

The token is sent once, as `joinToken`. If the server refuses it, the
client opens a plain session instead. For events about your backend rather
than a player's session, use server mode.

## Server mode

A `Server` sends events from your own server into a website's analytics (a
title's website or your company website) with that website's secret server
key, made in the dashboard. The events show in the website's Server events
report.

* **Keep the key on your server.** A request that carries an `Origin`
  header is refused, so a server key never works from a browser, and the
  SDK never sends one.
* **Identity is explicit.** `ActorID` is your own opaque id for the person
  or device, 8 to 128 of `A-Z a-z 0-9 _ . : -`, so never an email. The
  server stores only a keyed hash of it. Without it the event belongs to
  your backend itself. The server mode never reads or sends a player's
  device id.
* **Checked before sending.** Every event is checked the way the server
  checks it (name, id, time, at most 24 short properties, no value that
  looks like an email, a credential, a JWT or a key), so one bad event
  never costs a batch. `Send` and `Track` return a `*ValidationError`
  naming the event and the field.
* **Idempotent.** Each event has an `EventID` (the SDK makes one when you
  leave it empty) that stays the same across retries; the same id and
  content again is a duplicate, never counted twice.
* **Answers.** A `429`, `5xx` or timeout is retried with the backoff (a
  `Retry-After` exactly), and a `409 site_changed` after a second. A pause for your studio's
  balance is not an error: the paused ids come back in `RefusedIDs`, cost
  nothing, and `Track` sends them again later. A refused key, or website
  analytics being off, stops the `Server` and returns a `*RefusalError`.
* **Memory only.** `Track` keeps at most `MaxQueueSize` events (10,000 by
  default) in memory and returns `queue_full` past it. `Close` sends for
  at most about 1.5 seconds; events still waiting after that are dropped.
* One accepted event is one event on your website's usage, at the usual
  rate. Duplicates, refusals and paused events are free.

| Call | What it does |
| --- | --- |
| `ravensight.NewServer(cfg)` | Makes a Server. Never fails: without a valid `gt_srv_` key it is inert and every call returns a `*RefusalError`. |
| `srv.Track(ev)` | Validates and queues an event to be sent in the background. Never blocks. |
| `srv.Send(ctx, evs...)` | Validates and sends now, in batches of 50, and returns the server's answer. |
| `srv.Flush(ctx)` | Sends the queued events now and waits. |
| `srv.Close()`, `srv.Shutdown(ctx)` | Sends what is left and stops. |
| `srv.Stats()` | Counters. |

## Playtest

During a Ravensight Playtest run the runner gives the build a token. The
client reads it from `Config.Playtest`, then the environment
(`RAVENSIGHT_PLAYTEST_TOKEN`, `RAVENSIGHT_PLAYTEST_RUN_ID`,
`RAVENSIGHT_PLAYTEST_JOB_ID`, `RAVENSIGHT_PLAYTEST_PERSONA`), then the
command line (`--ravensight-playtest-token=` and
`--ravensight-playtest-run-id=`). With a token the device id is
`pt-<run id>` in memory only, every request carries the token, events are
tagged as the run's, nothing is written to disk, the player's saved opt-out
is ignored, and identify and campaign links send nothing. A token without a
run id never runs as a real player.

## Compatibility

| ravensight-go | Go | Notes |
| --- | --- | --- |
| 0.1.0 | 1.22 or newer | Preview. Built and tested on macOS with the race detector; CI runs the same tests on Linux with Go 1.22 and the current release. Windows, mobile and browser builds have not been run. |

## Running the tests

```sh
go vet ./...
go test -race ./...
```

The tests run against a fake HTTP server and temporary directories: no
network, and nothing written to your real storage directory. They cover the
shared vectors copied from the platform (`testdata/scrub-vectors.json`,
`testdata/identify-vectors.json` and `testdata/delivery-vectors.json`), and
the shared conformance cases: an inert client, opt-in before and after the
yes, a saved opt-out across clients, reset, refused keys, the kill switch,
`Retry-After`, the 400 and 413 splits, the 50 event batch and the 500 event
cap, the saved queue with its origin, event ids across a resend and a
relaunch, gzip and signing, the 8 KB cap, error capture's gate, caps, trail
and boot hold, a panic recorded and sent once, queue order, identify,
campaign links, remote config exposure, feedback, playtest runs and server
mode.

`examples/game` and `examples/backend` are small programs; without a key
they run and send nothing.

## Contributing

Install the local checks once per clone: `git config core.hooksPath
.githooks`. The pre-push hook then runs `gofmt`, `go vet` and
`go test -race` before every push, with a time limit, and refuses the push
if they fail. CI runs nightly and on demand, not on pushes or pull
requests, so this hook is where a change is checked first.
`sh .githooks/test-pre-push.sh` tests the hook's refuse paths.

## License

MIT. See [LICENSE](LICENSE).
