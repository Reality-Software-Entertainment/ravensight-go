package ravensight

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Status says why a client is or is not sending. The codes are shared by
// every Ravensight SDK.
type Status string

const (
	StatusOK                Status = ""                    // sending, or about to
	StatusNoKey             Status = "no_key"              // built with no key: inert
	StatusDisabled          Status = "disabled"            // switched off, opted out, or waiting for an opt-in yes
	StatusInvalidKey        Status = "invalid_key"         // the key was refused; stopped for the run
	StatusTrackingDisabled  Status = "tracking_disabled"   // the game's kill switch; stopped for the run
	StatusOffline           Status = "offline"             // no connection, a timeout or a server error; retrying
	StatusRateLimited       Status = "rate_limited"        // waiting out the server's Retry-After
	StatusSessionFailed     Status = "session_failed"      // the session could not be opened
	StatusPlaytestJobClosed Status = "playtest_job_closed" // the playtest job finished; stopped for the run
)

// Stats are counters since the client was made.
type Stats struct {
	Queued    int    // events waiting now
	Sent      int    // events the server acknowledged
	Dropped   int    // events dropped on the device (queue full, too large, refused)
	TooOld    int    // saved events older than 89 days, dropped when read
	Batches   int    // batch requests that were acknowledged
	Sessions  int    // sessions opened
	LastError string // the last refusal or error code
}

// ErrNotSent is returned by Flush when events are still queued, because the
// client is offline, waiting out a Retry-After, switched off or stopped.
var ErrNotSent = errors.New("ravensight: events are still queued")

// RefusalError is a call the SDK or the server refused. Code is the shared
// reason code, such as "empty_message", "looks_like_email", "disabled" or
// the server's error code; Status is the HTTP status when the server
// answered.
type RefusalError struct {
	Code   string
	Status int
}

func (e *RefusalError) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("ravensight: refused (%s, HTTP %d)", e.Code, e.Status)
	}
	return "ravensight: refused (" + e.Code + ")"
}

func refused(code string) error { return &RefusalError{Code: code} }

// Client sends a game's events to Ravensight. Make one with New and keep it
// for the life of the game; every method is safe to call from any goroutine
// and none waits on the network except Flush, SubmitFeedback and Shutdown.
type Client struct {
	cfg          Config
	api          string
	apiHost      string
	httpc        *http.Client
	logger       *slog.Logger
	st           store
	queueName    string
	platform     string
	gameVersion  string
	distribution string
	device       map[string]string
	flushEvery   time.Duration
	timeout      time.Duration
	maxQueue     int
	inert        bool
	persist      bool
	isPlaytest   bool
	playtest     Playtest
	now          func() time.Time
	random       func() float64
	crashGrace   time.Duration // how long a recorded panic waits to count as recovered; tests shorten it

	persistMu sync.Mutex // serializes queue file writes and deletes
	ioMu      sync.Mutex // serializes the device id and identity file writes

	mu    sync.Mutex
	after []func() // log lines and callbacks, run after mu is released

	enabled    bool
	optedOut   bool
	optedIn    bool
	stopped    Status
	transient  Status
	lastStatus Status
	closing    bool
	closed     bool
	activated  bool
	queueRead  bool

	deviceID   string
	generation uint64
	runStart   time.Time
	nonce      string
	counter    uint64

	settingsRead bool
	sess         session
	joinToken    string
	campaignLink string
	reauthed     bool
	noGzip       bool
	batchLimit   int

	events   []*qevent
	seq      uint64
	dirty    bool
	dirtyAt  time.Time
	batchDue time.Time

	nextAttempt time.Time
	retryUntil  time.Time
	backoff     time.Duration

	startedQueued bool
	exitedQueued  bool

	cap       captureState
	capLive   atomic.Bool
	trailLive atomic.Bool

	id   identifyState
	rc   remoteState
	jobs []*feedbackJob

	stats  Stats
	warned map[string]bool

	wake      chan struct{}
	flushReq  chan chan struct{}
	stopReq   chan struct{}
	done      chan struct{}
	runCtx    context.Context
	cancelRun context.CancelFunc
}

// session is the current session answer. The signing secret is kept in
// memory only.
type session struct {
	token         string
	expires       time.Time
	origin        json.RawMessage
	originSession string
	secret        string
	offset        int64 // server clock minus device clock, in seconds
}

// New makes a client and, with a key, starts its goroutine. It never fails:
// without a key the client is inert and every call is a quiet no-op.
func New(cfg Config) *Client {
	return newClient(cfg, os.Args, time.Now, rand.Float64)
}

func newClient(cfg Config, args []string, now func() time.Time, random func() float64) *Client {
	c := &Client{
		cfg:          cfg,
		api:          normalizeAPIURL(cfg.APIURL),
		now:          now,
		random:       random,
		enabled:      !cfg.StartDisabled,
		batchLimit:   maxBatch,
		warned:       map[string]bool{},
		wake:         make(chan struct{}, 1),
		flushReq:     make(chan chan struct{}),
		stopReq:      make(chan struct{}),
		done:         make(chan struct{}),
		gameVersion:  strings.TrimSpace(cfg.GameVersion),
		platform:     strings.TrimSpace(cfg.Platform),
		distribution: cleanLabel(cfg.Distribution, 40),
	}
	if u, err := url.Parse(c.api); err == nil {
		c.apiHost = strings.ToLower(u.Host)
	}
	c.logger = cfg.Logger
	if c.logger == nil {
		c.logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	}
	if c.gameVersion == "" {
		c.gameVersion = "1.0.0"
	}
	if c.platform == "" {
		c.platform = defaultPlatform()
	}
	c.flushEvery = cfg.FlushInterval
	if c.flushEvery == 0 {
		c.flushEvery = defaultFlush
	}
	c.timeout = cfg.RequestTimeout
	if c.timeout <= 0 {
		c.timeout = defaultTimeout
	}
	c.maxQueue = cfg.MaxQueueSize
	if c.maxQueue <= 0 || c.maxQueue > maxQueue {
		c.maxQueue = maxQueue
	}
	c.httpc = cfg.HTTPClient
	if c.httpc == nil {
		c.httpc = &http.Client{}
	}
	c.device = map[string]string{}
	for k, v := range map[string]string{"model": cfg.Device.Model, "osVersion": cfg.Device.OSVersion, "gpu": cfg.Device.GPU} {
		if clean := cleanDeviceField(v); clean != "" {
			c.device[k] = clean
		}
	}

	key := strings.TrimSpace(cfg.IngestKey)
	c.st = c.resolveStore(key)
	if key == "" {
		c.inert = true
		if !cfg.Optional {
			c.logger.Warn("Ravensight: no ingest key, so nothing is collected or sent. Set Config.IngestKey, or Config.Optional to silence this.")
		}
		close(c.done)
		return c
	}

	c.playtest = playtestFrom(cfg.Playtest, args)
	c.isPlaytest = c.playtest.Token != ""
	if c.isPlaytest {
		c.st = store{} // a playtest run keeps nothing on disk
	}
	c.persist = !cfg.MemoryOnlyQueue && c.st.enabled()
	c.queueName = queueFileName(key)
	c.runStart = now()
	c.nonce = randomHex(4)
	c.joinToken = strings.TrimSpace(cfg.JoinToken)
	c.cap.disabledByConfig = cfg.NoErrorCapture
	c.cap.perKey = map[string]int{}
	c.cap.tokens = capturesPerMinute

	if !c.isPlaytest {
		state := c.st.loadState()
		c.optedOut, c.optedIn = state.OptedOut, state.OptedIn
		c.id.stored = c.st.loadIdentity()
		c.pruneCrashes() // records past 30 days or 5 go whatever the gate says
	}
	if cfg.CampaignLink != "" {
		_ = c.SetCampaignLink(cfg.CampaignLink) // a bad one is refused with one warning
	}
	c.runCtx, c.cancelRun = context.WithCancel(context.Background())

	c.mu.Lock()
	if c.isPlaytest && c.playtest.RunID == "" {
		c.stopped = StatusSessionFailed
		c.logLocked(slog.LevelWarn, "Ravensight: a playtest token came without a run id, so nothing is sent this run.")
	}
	if c.isPlaytest {
		c.deviceID = "pt-" + c.playtest.RunID
	} else if id := strings.TrimSpace(cfg.DeviceID); id != "" {
		c.deviceID = id
	} else if raw, err := c.st.read(deviceIDFile); err == nil && c.optedOut {
		// An opted out player keeps the id they had; none is made.
		c.deviceID = strings.TrimSpace(string(raw))
	}
	c.lastStatus = c.statusLocked()
	c.activateLocked()
	c.unlock()

	go c.run()
	return c
}

func (c *Client) resolveStore(key string) store {
	if c.cfg.StorageDir != "" {
		return store{dir: c.cfg.StorageDir}
	}
	if key == "" {
		return store{}
	}
	dir, err := DefaultStorageDir(key)
	if err != nil {
		c.logger.Warn("Ravensight: no storage directory, so the device id lasts for this run only. Set Config.StorageDir.")
		return store{}
	}
	return store{dir: dir}
}

// unlock releases mu, then runs the log lines and callbacks queued while it
// was held. Nothing that can call back into the client runs under mu.
func (c *Client) unlock() {
	after := c.after
	c.after = nil
	c.mu.Unlock()
	for _, f := range after {
		f()
	}
}

func (c *Client) logLocked(level slog.Level, msg string) {
	c.after = append(c.after, func() { c.logger.Log(context.Background(), level, msg) })
}

func (c *Client) warnOnceLocked(key, msg string) {
	if c.warned[key] {
		return
	}
	c.warned[key] = true
	c.logLocked(slog.LevelWarn, msg)
}

// consentLocked reports whether the player's choice allows collecting.
func (c *Client) consentLocked() bool {
	if c.isPlaytest {
		return true
	}
	if c.optedOut {
		return false
	}
	return c.cfg.Consent != ConsentOptIn || c.optedIn
}

// activeLocked reports whether the client may collect and send right now.
func (c *Client) activeLocked() bool {
	return !c.inert && !c.closed && c.enabled && c.consentLocked() && c.stopped == ""
}

func (c *Client) statusLocked() Status {
	switch {
	case c.inert:
		return StatusNoKey
	case c.stopped != "":
		return c.stopped
	case !c.enabled || !c.consentLocked():
		return StatusDisabled
	default:
		return c.transient
	}
}

// statusChangedLocked reports a status change to OnStatus, after unlock.
func (c *Client) statusChangedLocked() {
	s := c.statusLocked()
	if s == c.lastStatus {
		return
	}
	c.lastStatus = s
	if f := c.cfg.OnStatus; f != nil {
		c.after = append(c.after, func() { f(s) })
	}
	c.updateCaptureGateLocked()
}

// activateLocked does the work of the first moment the client may collect:
// the device id, the saved queue, the config cache, game_started and saved
// crashes. Each part runs once per run, or again after a reset.
func (c *Client) activateLocked() {
	c.statusChangedLocked()
	c.updateCaptureGateLocked()
	if !c.activeLocked() {
		return
	}
	if c.deviceID == "" {
		c.deviceID = c.loadOrMakeDeviceIDLocked()
	}
	if !c.queueRead {
		c.queueRead = true
		c.loadQueueLocked()
	}
	c.loadConfigCacheLocked()
	if !c.cfg.NoLifecycleEvents && !c.startedQueued {
		c.startedQueued = true
		c.trackLocked("game_started", nil, false, nil)
	}
	c.loadCrashesLocked()
	if c.id.stored.AccountHash != "" || c.id.held != "" {
		c.id.wantSession = true
	}
	c.activated = true
	c.wakeWorker()
}

func (c *Client) loadOrMakeDeviceIDLocked() string {
	if id := strings.TrimSpace(c.cfg.DeviceID); id != "" {
		return id
	}
	if raw, err := c.st.read(deviceIDFile); err == nil {
		if id := strings.TrimSpace(string(raw)); id != "" && len(id) <= 128 {
			return id
		}
	}
	c.saveDeviceIDLater()
	return newDeviceID()
}

func (c *Client) loadQueueLocked() {
	if !c.persist {
		return
	}
	c.st.removeTemps(c.queueName)
	raw, err := c.st.read(c.queueName)
	if err != nil {
		return
	}
	loaded, tooOld, overflow := decodeQueue(raw, c.now(), c.maxQueue)
	c.stats.TooOld += tooOld
	c.stats.Dropped += overflow
	for _, e := range loaded {
		c.seq++
		e.seq = c.seq
	}
	c.events = append(loaded, c.events...)
	if len(c.events) > c.maxQueue {
		c.stats.Dropped += len(c.events) - c.maxQueue
		c.events = c.events[len(c.events)-c.maxQueue:]
	}
	if len(c.events) > 0 {
		c.batchDue = c.now()
	}
}

func (c *Client) wakeWorker() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// Track queues an event. data holds the event's properties: strings,
// numbers, bools, nil or anything encoding/json can write. It is copied and
// serialized at once, so the caller may reuse the map. Track never waits on
// the network or the disk. It returns false when the event was not queued:
// the client is not collecting, the name is empty or over 200 characters,
// the data cannot be written as JSON, or it is over 8 KB as JSON.
func (c *Client) Track(name string, data map[string]any) bool {
	if c == nil {
		return false
	}
	return c.trackData(name, data, false)
}

// trackData serializes the caller's values before taking the lock, so a
// value's own MarshalJSON never runs while the client is locked.
func (c *Client) trackData(name string, data map[string]any, capture bool) bool {
	if c.inert {
		return false
	}
	c.mu.Lock()
	closing := c.closing
	c.mu.Unlock()
	if closing {
		return false
	}
	fields := make(map[string]json.RawMessage, len(data)+4)
	texts := map[string]string{}
	for k, v := range data {
		raw, err := marshalJSON(v)
		if err != nil {
			c.mu.Lock()
			c.stats.Dropped++
			c.stats.LastError = "invalid_event_data"
			c.warnOnceLocked("invalid_data", fmt.Sprintf("Ravensight: dropped event %q: property %q cannot be written as JSON (further drops are not logged).", name, k))
			c.unlock()
			return false
		}
		fields[k] = raw
		if s, ok := v.(string); ok {
			texts[k] = s
		}
	}
	c.mu.Lock()
	ok := c.trackLocked(name, fields, capture, texts)
	c.unlock()
	return ok
}

// trackLocked adds the SDK's keys and queues the event.
func (c *Client) trackLocked(name string, fields map[string]json.RawMessage, capture bool, texts map[string]string) bool {
	return c.trackAtLocked(name, fields, capture, texts, 0)
}

// trackAtLocked is trackLocked with the time it happened (unix ms, 0 for
// now).
func (c *Client) trackAtLocked(name string, fields map[string]json.RawMessage, capture bool, texts map[string]string, ts int64) bool {
	if !c.activeLocked() {
		return false
	}
	if strings.TrimSpace(name) == "" || utf16Len(name) > maxEventNameUnits {
		c.stats.Dropped++
		c.stats.LastError = "invalid_event_name"
		c.warnOnceLocked("bad_name", "Ravensight: dropped an event with an empty name or a name over 200 characters (further drops are not logged).")
		return false
	}
	if fields == nil {
		fields = map[string]json.RawMessage{}
	}
	c.tagExposureLocked(fields)
	if c.isPlaytest {
		fields["synthetic"] = json.RawMessage("true")
		if c.playtest.RunID != "" {
			fields["pt_run"] = mustJSON(c.playtest.RunID)
		}
		if c.playtest.JobID != "" {
			fields["pt_job"] = mustJSON(c.playtest.JobID)
		}
		if c.playtest.Persona != "" {
			fields["persona"] = mustJSON(c.playtest.Persona)
		}
	}
	return c.queueFieldsLocked(name, fields, capture, texts, ts)
}

// queueFieldsLocked stamps rs_event_id, checks the size and queues.
func (c *Client) queueFieldsLocked(name string, fields map[string]json.RawMessage, capture bool, texts map[string]string, ts int64) bool {
	c.counter++
	fields["rs_event_id"] = mustJSON(eventID(c.deviceID, c.runStart.Unix(), c.nonce, c.counter))
	data, err := marshalJSON(fields)
	if err != nil {
		c.stats.Dropped++
		return false
	}
	kind := ""
	_ = json.Unmarshal(fields["rs_kind"], &kind)
	if countedBytes(kind, texts, data) > maxEventBytes {
		c.stats.Dropped++
		c.stats.LastError = "event_too_large"
		c.warnOnceLocked("too_large", fmt.Sprintf("Ravensight: dropped event %q: its data is over %d bytes as JSON (further drops are not logged).", name, maxEventBytes))
		return false
	}
	if ts <= 0 {
		ts = c.now().UnixMilli()
	}
	e := &qevent{name: name, timestamp: ts, data: data, capture: capture,
		origin: c.sess.origin, session: c.sess.originSession}
	if _, hasTrail := fields["rs_trail"]; hasTrail {
		delete(fields, "rs_trail")
		e.saved, _ = marshalJSON(fields)
	}
	if !capture && c.trailLive.Load() && kind != "error" {
		c.pushTrailLocked(trailLine(c.now().Sub(c.runStart), "event", name))
	}
	return c.enqueueLocked(e)
}

// eventID is the first 16 characters of the device id, the start time in
// hex, the per-launch nonce and a counter: unique per device and stable
// across resends.
func eventID(deviceID string, start int64, nonce string, counter uint64) string {
	prefix := []byte(deviceID)
	if len(prefix) > 16 {
		prefix = prefix[:16]
	}
	for i, ch := range prefix {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '_' || ch == '-') {
			prefix[i] = '_'
		}
	}
	return fmt.Sprintf("%s-%x-%s-%d", prefix, start, nonce, counter)
}

// enqueueLocked applies the queue cap: at the cap an incoming capture
// evicts the oldest capture or is dropped itself, and gameplay evicts the
// oldest capture first, then the oldest gameplay event. Nothing in flight
// is evicted.
func (c *Client) enqueueLocked(e *qevent) bool {
	if len(c.events) >= c.maxQueue {
		victim := c.oldestLocked(true)
		if victim < 0 && !e.capture {
			victim = c.oldestLocked(false)
		}
		if victim < 0 {
			c.stats.Dropped++
			return false
		}
		c.events = append(c.events[:victim], c.events[victim+1:]...)
		c.stats.Dropped++
		c.warnOnceLocked("queue_full", "Ravensight: the queue is full, so the oldest events are dropped (further drops are not logged).")
	}
	c.seq++
	e.seq = c.seq
	wasIdle := c.sendableLocked() == 0
	c.events = append(c.events, e)
	c.markDirtyLocked()
	if wasIdle {
		c.batchDue = c.now().Add(c.flushEvery)
		c.wakeWorker()
	}
	if c.sendableLocked() >= maxBatch {
		c.wakeWorker()
	}
	return true
}

func (c *Client) oldestLocked(capture bool) int {
	for i, e := range c.events {
		if e.capture == capture && !e.inFlight {
			return i
		}
	}
	return -1
}

func (c *Client) sendableLocked() int {
	n := 0
	for _, e := range c.events {
		if !e.inFlight {
			n++
		}
	}
	return n
}

func (c *Client) markDirtyLocked() {
	if !c.persist {
		return
	}
	if !c.dirty {
		c.dirtyAt = c.now()
		c.wakeWorker() // so a worker asleep in a long backoff saves within a second
	}
	c.dirty = true
}

// clearQueue empties the queue and deletes its file. Used by opt-out and a
// device id reset.
func (c *Client) clearQueue() {
	c.persistMu.Lock()
	defer c.persistMu.Unlock()
	c.mu.Lock()
	c.events = nil
	c.dirty = false
	c.unlock()
	c.st.remove(c.queueName)
	c.st.removeTemps(c.queueName)
}

// persistNow writes the queue file now if it changed. An empty queue
// deletes the file.
func (c *Client) persistNow() {
	c.persistMu.Lock()
	defer c.persistMu.Unlock()
	c.mu.Lock()
	if !c.persist || !c.dirty {
		c.unlock()
		return
	}
	c.dirty = false
	var raw []byte
	if len(c.events) > 0 {
		raw = encodeQueue(c.events)
	}
	c.unlock()
	if raw == nil {
		c.st.remove(c.queueName)
		return
	}
	if err := c.st.write(c.queueName, raw); err != nil {
		c.mu.Lock()
		c.markDirtyLocked()
		c.warnOnceLocked("queue_write", "Ravensight: could not save the queue file; unsent events may be lost if the game is killed.")
		c.unlock()
	}
}

// Flush sends what is queued now, cutting a retry backoff short (never a
// server's Retry-After), and waits until the queue is empty, nothing more
// can be sent for now, or ctx ends. It returns nil when nothing is left,
// ErrNotSent (wrapped with the reason) when events remain, or ctx's error.
func (c *Client) Flush(ctx context.Context) error {
	if c == nil || c.inert {
		return nil
	}
	reply := make(chan struct{})
	select {
	case c.flushReq <- reply:
	case <-c.done:
		return c.pendingErr()
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-reply:
	case <-c.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	return c.pendingErr()
}

func (c *Client) pendingErr() error {
	c.mu.Lock()
	n, s := len(c.events), c.statusLocked()
	c.unlock()
	if n == 0 {
		return nil
	}
	return fmt.Errorf("%w: %d left (%s)", ErrNotSent, n, statusText(s))
}

func statusText(s Status) string {
	if s == StatusOK {
		return "ok"
	}
	return string(s)
}

// Close records game_exited, sends what it can for at most about 1.5
// seconds, saves what is left for the next launch and stops the client.
func (c *Client) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultCloseLimit)
	defer cancel()
	return c.Shutdown(ctx)
}

// Shutdown is Close with your own time limit: it records game_exited,
// sends until the queue is empty, nothing more can be sent, or ctx ends,
// then saves what is left and stops. Later calls do nothing.
func (c *Client) Shutdown(ctx context.Context) error {
	if c == nil || c.inert {
		return nil
	}
	c.mu.Lock()
	if c.closing {
		c.unlock()
		select {
		case <-c.done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	c.closing = true
	if !c.cfg.NoLifecycleEvents && !c.exitedQueued && c.activeLocked() {
		c.exitedQueued = true
		c.trackLocked("game_exited", nil, false, nil)
	}
	c.unlock()
	close(c.stopReq)
	var err error
	select {
	case <-c.done:
	case <-ctx.Done():
		err = ctx.Err()
		c.cancelRun()
		<-c.done
	}
	c.cancelRun()
	c.mu.Lock()
	c.closed = true
	c.statusChangedLocked()
	c.updateCaptureGateLocked()
	c.failJobsLocked("closed")
	c.unlock()
	return err
}

// SetEnabled is a runtime switch for your own logic, such as a level
// editor or a debug build. Off, nothing is collected or sent. Not saved.
func (c *Client) SetEnabled(on bool) {
	if c == nil || c.inert {
		return
	}
	c.mu.Lock()
	c.enabled = on
	if !on {
		c.clearRemoteLocked()
	}
	c.activateLocked()
	c.unlock()
}

// IsEnabled reports the runtime switch.
func (c *Client) IsEnabled() bool {
	if c == nil || c.inert {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.enabled
}

// SetTrackingEnabled is the player's analytics choice, saved at once and
// remembered across launches. Off clears the queue, deletes its file,
// pending crash records, the stored identity and the config cache, and
// stops every request. With ConsentOptIn, on is the player's yes. A
// playtest run ignores it.
func (c *Client) SetTrackingEnabled(on bool) error {
	if c == nil {
		return nil
	}
	if c.isPlaytest {
		c.mu.Lock()
		c.warnOnceLocked("playtest_choice", "Ravensight: a playtest run ignores SetTrackingEnabled.")
		c.unlock()
		return nil
	}
	c.mu.Lock()
	c.optedOut = !on
	if on && c.cfg.Consent == ConsentOptIn {
		c.optedIn = true
	}
	if !on {
		c.optedIn = false
	}
	c.updateCaptureGateLocked() // the hooks stop before anything is deleted
	st := savedState{OptedOut: c.optedOut, OptedIn: c.optedIn}
	c.unlock()
	err := c.st.saveState(st)
	if c.inert {
		return err
	}
	if !on {
		c.clearQueue()
		c.mu.Lock()
		c.sess = session{}
		c.identifyForgetLocked()
		c.deleteCrashesLocked()
		c.clearRemoteLocked()
		c.st.remove(configFile)
		c.resetCaptureLocked()
		c.activateLocked()
		c.unlock()
		return err
	}
	c.mu.Lock()
	c.activateLocked()
	c.unlock()
	return err
}

// SetTrackingEnabled saves the player's choice before a client exists, for
// example on a first-run settings screen. Pass the config you will make the
// client with.
func SetTrackingEnabled(cfg Config, on bool) error {
	c := &Client{cfg: cfg}
	st := c.resolveStoreQuiet(strings.TrimSpace(cfg.IngestKey))
	state := st.loadState()
	state.OptedOut = !on
	if on && cfg.Consent == ConsentOptIn {
		state.OptedIn = true
	}
	if !on {
		state.OptedIn = false
		st.remove(queueFileName(strings.TrimSpace(cfg.IngestKey)))
		st.remove(identityFile)
		st.remove(configFile)
		for _, name := range st.list(crashPrefix) {
			st.remove(name)
		}
	}
	if !st.enabled() {
		return errors.New("ravensight: no storage directory; set Config.StorageDir")
	}
	return st.saveState(state)
}

func (c *Client) resolveStoreQuiet(key string) store {
	if c.cfg.StorageDir != "" {
		return store{dir: c.cfg.StorageDir}
	}
	if key == "" {
		return store{}
	}
	if dir, err := DefaultStorageDir(key); err == nil {
		return store{dir: dir}
	}
	return store{}
}

// TrackingEnabled reports whether the player's saved choice allows
// collecting (with ConsentOptIn, whether the player said yes).
func (c *Client) TrackingEnabled() bool {
	if c == nil || c.inert {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.consentLocked()
}

// ResetDeviceID switches to a fresh random device id from now on, so
// nothing links the old id to the new one: the queue, its file, the
// session, pending crash records and the config values are dropped. The
// stored identity (see Identify) is kept; use ResetIdentity on sign-out.
// It returns the new id, or "" while the player has not agreed to
// collection. A playtest run ignores it.
func (c *Client) ResetDeviceID() string {
	if c == nil || c.inert {
		return ""
	}
	if c.isPlaytest {
		c.mu.Lock()
		c.warnOnceLocked("playtest_reset", "Ravensight: a playtest run ignores ResetDeviceID.")
		id := c.deviceID
		c.unlock()
		return id
	}
	// The queue is cleared and the id changed in one step, so an event
	// tracked meanwhile never carries the old id into the new identity.
	c.persistMu.Lock()
	defer c.persistMu.Unlock()
	c.mu.Lock()
	c.events = nil
	c.dirty = false
	c.resetDeviceLocked()
	id := c.deviceID
	c.unlock()
	c.st.remove(c.queueName)
	c.st.removeTemps(c.queueName)
	return id
}

func (c *Client) resetDeviceLocked() {
	c.generation++
	c.sess = session{}
	c.reauthed = false
	c.deleteCrashesLocked()
	c.resetCaptureLocked()
	c.clearRemoteLocked()
	c.rc.exposed = map[string]string{}
	c.id.identifiedSession = ""
	c.runStart = c.now()
	c.counter = 0
	if strings.TrimSpace(c.cfg.DeviceID) != "" {
		c.deviceID = newDeviceID() // the game's own id: a new one for this run only
	} else if c.consentLocked() {
		c.deviceID = newDeviceID()
		c.saveDeviceIDLater()
	} else {
		c.deviceID = "" // the file is deleted, so the old id never comes back
		c.saveDeviceIDLater()
	}
	if c.id.stored.AccountHash != "" || c.id.held != "" {
		c.id.wantSession = true // the next session identifies again
	}
	c.wakeWorker()
}

// saveDeviceIDLater writes the device id file once mu is released, with
// the id current then: an empty id deletes the file. A game's own
// Config.DeviceID is never written. ioMu keeps two saves in order.
func (c *Client) saveDeviceIDLater() {
	if strings.TrimSpace(c.cfg.DeviceID) != "" || c.isPlaytest || !c.st.enabled() {
		return
	}
	c.after = append(c.after, func() {
		c.ioMu.Lock()
		defer c.ioMu.Unlock()
		c.mu.Lock()
		id := c.deviceID
		c.mu.Unlock()
		if id == "" {
			c.st.remove(deviceIDFile)
			return
		}
		if err := c.st.write(deviceIDFile, []byte(id)); err != nil {
			c.mu.Lock()
			c.warnOnceLocked("device_id_write", "Ravensight: could not save the device id, so this launch counts as a new player.")
			c.unlock()
		}
	})
}

// saveIdentityLater writes the identity file once mu is released, with the
// identity current then, so a save and a delete never land out of order.
func (c *Client) saveIdentityLater() {
	if c.isPlaytest || !c.st.enabled() {
		return
	}
	c.after = append(c.after, func() {
		c.ioMu.Lock()
		defer c.ioMu.Unlock()
		c.mu.Lock()
		id := c.id.stored
		c.mu.Unlock()
		if err := c.st.saveIdentity(id); err != nil {
			c.mu.Lock()
			c.warnOnceLocked("identity_write", "Ravensight: could not save the identity file.")
			c.unlock()
		}
	})
}

// DeviceID is the id in use, "" before the player agrees under
// ConsentOptIn or without a key. Do not log or show it beside config
// values.
func (c *Client) DeviceID() string {
	if c == nil {
		return ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deviceID
}

// SessionToken is the current session token, "" until a session opens. A
// game server can pass it as Config.JoinToken.
func (c *Client) SessionToken() string {
	if c == nil {
		return ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sess.token
}

// Status says why the client is or is not sending.
func (c *Client) Status() Status {
	if c == nil {
		return StatusNoKey
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.statusLocked()
}

// IsCollecting reports whether the client is collecting: a key, switched
// on, the player's choice allows it and nothing stopped the run.
func (c *Client) IsCollecting() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.activeLocked()
}

// IsPlaytest reports whether this is a Ravensight Playtest run.
func (c *Client) IsPlaytest() bool { return c != nil && c.isPlaytest }

// Stats returns the client's counters.
func (c *Client) Stats() Stats {
	if c == nil {
		return Stats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.stats
	s.Queued = len(c.events)
	return s
}

func mustJSON(v any) json.RawMessage {
	raw, err := marshalJSON(v)
	if err != nil {
		return json.RawMessage("null")
	}
	return raw
}

var experimentKeyPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
var variantPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,39}$`)
