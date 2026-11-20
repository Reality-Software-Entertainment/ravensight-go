package ravensight

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// Version is this SDK's version, sent on the session as "go/<Version>".
const Version = "0.1.0"

// DefaultAPIURL is the Ravensight API.
const DefaultAPIURL = "https://api.ravensight.io"

// Shared limits. Every Ravensight SDK uses the same numbers.
const (
	maxQueue          = 500  // events kept, oldest dropped first
	maxBatch          = 50   // events per request
	maxEventBytes     = 8192 // one event's data as JSON
	maxEventNameUnits = 200  // one event's name
	maxEventAge       = 89 * 24 * time.Hour
	gzipFrom          = 1024 // bodies this size or larger are compressed
	backoffBase       = 10 * time.Second
	backoffMax        = 300 * time.Second
	maxFeedbackUnits  = 2000
	defaultFlush      = 5 * time.Second
	defaultTimeout    = 15 * time.Second
	defaultCloseLimit = 1500 * time.Millisecond
)

// Consent decides whether the SDK may collect before the player says so.
type Consent int

const (
	// ConsentOptOut collects until the player opts out. The default.
	ConsentOptOut Consent = iota
	// ConsentOptIn sends nothing, and makes no device id, until
	// SetTrackingEnabled(true).
	ConsentOptIn
)

// Device holds optional device fields sent once on the session. The SDK
// never probes the machine for them; pass what your game already knows.
type Device struct {
	Model     string // device model name, never a serial
	OSVersion string // OS version only, without the OS name
	GPU       string // GPU renderer or adapter name
}

// Playtest holds the values a Ravensight Playtest run gives the build. They
// are normally read from the environment and the command line; set them
// here only to override.
type Playtest struct {
	Token   string
	RunID   string
	JobID   string
	Persona string
}

// Config configures a Client. The zero value of every field is a sensible
// default except IngestKey: without a key the client is inert.
type Config struct {
	// IngestKey is the game's publishable gt_live_ key or a public gt_pub_
	// key. Empty makes the client inert: no goroutine, no file, no request.
	IngestKey string
	// Optional silences the one warning an inert client logs.
	Optional bool
	// APIURL is the API's address. Trailing slashes and a trailing /api/v1
	// are accepted. Empty means DefaultAPIURL.
	APIURL string
	// GameVersion is sent on every session. Empty means "1.0.0".
	GameVersion string
	// Platform is sent on every session. Empty means the build's operating
	// system: windows, macos, linux, android, ios or web.
	Platform string
	// Distribution is a short label for this build, such as "steam" or
	// "itch-demo". At most 40 characters and no control characters,
	// otherwise not sent.
	Distribution string
	// Device holds optional device fields for the session.
	Device Device
	// DeviceID is your own stable per-install id. Empty uses a random,
	// saved "dev_" id.
	DeviceID string
	// StorageDir is where the device id, the player's choice, the queue and
	// pending crash records are kept. Empty means a per-user directory named
	// after the key (see DefaultStorageDir). If no directory can be made the
	// SDK keeps everything in memory for the run.
	StorageDir string
	// Consent is ConsentOptOut (the default) or ConsentOptIn.
	Consent Consent
	// StartDisabled starts the client switched off, as SetEnabled(false).
	// Not saved.
	StartDisabled bool
	// MaxQueueSize caps queued events (at most 500, the default).
	MaxQueueSize int
	// FlushInterval is how long an event may wait to be batched. Zero means
	// 5 seconds; a negative value sends only on Flush, at 50 queued events
	// and on Close.
	FlushInterval time.Duration
	// RequestTimeout bounds each request. Zero means 15 seconds.
	RequestTimeout time.Duration
	// NoLifecycleEvents turns off the automatic game_started and
	// game_exited events.
	NoLifecycleEvents bool
	// NoErrorCapture turns off automatic error capture for this run.
	NoErrorCapture bool
	// MemoryOnlyQueue keeps unsent events in memory only: nothing is saved
	// for the next launch.
	MemoryOnlyQueue bool
	// JoinToken is a session token from the player's own client, sent once
	// so a game server's events land in the player's session.
	JoinToken string
	// CampaignLink is a Ravensight campaign link id (24 hex characters) the
	// game was opened with. See SetCampaignLink.
	CampaignLink string
	// Playtest overrides the playtest values read from the environment and
	// the command line.
	Playtest Playtest
	// HTTPClient sends the requests. Nil uses a client of the SDK's own.
	HTTPClient *http.Client
	// Logger receives the SDK's own lines, each starting "Ravensight:". Nil
	// writes warnings to standard error.
	Logger *slog.Logger
	// OnStatus is called when Status changes. It may run on any goroutine;
	// keep it quick and do not call Close from it.
	OnStatus func(Status)
	// OnConfigReady is called after each remote config fetch: true for a
	// fresh answer or a confirmed cache, false for a failed refresh that kept
	// the values it had. It runs on the SDK's goroutine.
	OnConfigReady func(fresh bool)
}

// normalizeAPIURL accepts the address with or without /api/v1 and trailing
// slashes, and returns the API base ending in /api/v1.
func normalizeAPIURL(raw string) string {
	base := strings.TrimSpace(raw)
	if base == "" {
		base = DefaultAPIURL
	}
	base = strings.TrimRight(base, "/")
	base = strings.TrimSuffix(base, "/api/v1")
	return strings.TrimRight(base, "/") + "/api/v1"
}

// defaultPlatform names the build's operating system the way every
// Ravensight SDK does.
func defaultPlatform() string {
	switch runtime.GOOS {
	case "darwin":
		return "macos"
	case "js", "wasip1":
		return "web"
	default:
		return runtime.GOOS
	}
}

// cleanLabel keeps a short label only when it is printable and short enough.
func cleanLabel(text string, limit int) string {
	text = strings.TrimSpace(text)
	if text == "" || utf16Len(text) > limit {
		return ""
	}
	for _, r := range text {
		if r < 0x20 || r == 0x7f {
			return ""
		}
	}
	return text
}

// cleanDeviceField trims a device field, keeps printable characters and
// cuts it to 128 characters.
func cleanDeviceField(text string) string {
	var b strings.Builder
	for _, r := range strings.TrimSpace(text) {
		if r >= 0x20 && r != 0x7f {
			b.WriteRune(r)
		}
	}
	return cutUnits(strings.TrimSpace(b.String()), 128)
}

// DefaultStorageDir is where a client keeps its files when Config.StorageDir
// is empty: a per-user data directory, then ravensight/<first 12 hex of the
// key's SHA-256>.
//
//	Windows  %APPDATA%\ravensight\<hash>
//	macOS    ~/Library/Application Support/ravensight/<hash>
//	others   $XDG_DATA_HOME/ravensight/<hash>, else ~/.local/share/ravensight/<hash>
func DefaultStorageDir(ingestKey string) (string, error) {
	sum := sha256.Sum256([]byte(ingestKey))
	hash := hex.EncodeToString(sum[:])[:12]
	var base string
	switch runtime.GOOS {
	case "windows":
		base = os.Getenv("APPDATA")
	case "darwin", "ios":
		if home, err := os.UserHomeDir(); err == nil {
			base = filepath.Join(home, "Library", "Application Support")
		}
	default:
		base = os.Getenv("XDG_DATA_HOME")
		if base == "" {
			if home, err := os.UserHomeDir(); err == nil {
				base = filepath.Join(home, ".local", "share")
			}
		}
	}
	if base == "" {
		return "", errors.New("ravensight: no per-user directory on this platform; set Config.StorageDir")
	}
	return filepath.Join(base, "ravensight", hash), nil
}

// playtestFrom resolves the playtest values: the config first, then the
// environment, then the command line.
func playtestFrom(cfg Playtest, args []string) Playtest {
	out := cfg
	pick := func(current *string, env string, flag string) {
		if *current != "" {
			return
		}
		if v := strings.TrimSpace(os.Getenv(env)); v != "" {
			*current = v
			return
		}
		if flag == "" {
			return
		}
		for _, arg := range args {
			if v, ok := strings.CutPrefix(arg, flag+"="); ok && strings.TrimSpace(v) != "" {
				*current = strings.TrimSpace(v)
			}
		}
	}
	pick(&out.Token, "RAVENSIGHT_PLAYTEST_TOKEN", "--ravensight-playtest-token")
	pick(&out.RunID, "RAVENSIGHT_PLAYTEST_RUN_ID", "--ravensight-playtest-run-id")
	pick(&out.JobID, "RAVENSIGHT_PLAYTEST_JOB_ID", "")
	pick(&out.Persona, "RAVENSIGHT_PLAYTEST_PERSONA", "")
	return out
}
