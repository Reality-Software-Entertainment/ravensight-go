package ravensight

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Automatic error capture follows the shared errors contract: device caps,
// device scrubbing, an opt-in trail, a boot hold, and the game's switch.
const (
	capturesPerRun    = 30
	capturesPerKey    = 3
	capturesPerMinute = 6
	trailsPerRun      = 10
	trailMaxLines     = 20
	trailLineUnits    = 120
	trailUnits        = 1500
	holdMax           = 5
	crashMax          = 5
	crashMaxAge       = 30 * 24 * time.Hour
	messageUnits      = 200
	stackUnits        = 2000
	typeUnits         = 100
)

type captureState struct {
	disabledByConfig bool // Config.NoErrorCapture or SetErrorCapture(false)
	known            bool // the game's switch has been read this run
	serverOn         bool
	trailOn          bool
	hold             []held // captures before the switch is known, at most 5
	captured         int
	perKey           map[string]int
	tokens           float64
	tokensAt         time.Time
	trails           int
	trail            []string
}

// prepared is a scrubbed capture.
type prepared struct {
	source, severity, errType, message, stack string
	key                                       string
	timestamp                                 int64 // unix ms; 0 means now
}

type held struct {
	p     prepared
	file  string // a saved crash record, deleted once queued or discarded
	trail string // the trail saved with that record
}

type crashRecord struct {
	Timestamp int64  `json:"timestamp"`
	Type      string `json:"type,omitempty"`
	Message   string `json:"message"`
	Stack     string `json:"stack,omitempty"`
	Trail     string `json:"trail,omitempty"`
}

func (c *Client) captureGateLocked() bool {
	return c.activeLocked() && !c.closing && !c.cap.disabledByConfig && (!c.cap.known || c.cap.serverOn)
}

// updateCaptureGateLocked publishes the gate to the lock-free checks the
// hooks make first, so a game that logs every frame pays almost nothing
// while capture is off or its caps are spent.
func (c *Client) updateCaptureGateLocked() {
	live := c.captureGateLocked()
	c.capLive.Store(live)
	c.trailLive.Store(live && c.cap.known && c.cap.trailOn)
}

// SetErrorCapture turns automatic error capture on or off for this run. It
// is not saved, and TrackError is never affected.
func (c *Client) SetErrorCapture(on bool) {
	if c == nil || c.inert {
		return
	}
	c.mu.Lock()
	c.cap.disabledByConfig = !on
	if !on {
		c.cap.hold = nil
		c.cap.trail = nil
	}
	c.updateCaptureGateLocked()
	c.unlock()
}

// IsErrorCaptureActive reports whether automatic capture is live: a key,
// consent, switched on here and on the server.
func (c *Client) IsErrorCaptureActive() bool { return c != nil && c.capLive.Load() }

// applyCaptureAnswerLocked reads errorCapture {enabled, trail} from a
// settings or session answer. An answer without it means on, no trail.
// Held boot captures are then queued or discarded.
func (c *Client) applyCaptureAnswerLocked(raw json.RawMessage) {
	enabled, trail := true, false
	var sw struct {
		Enabled *bool `json:"enabled"`
		Trail   *bool `json:"trail"`
	}
	if isJSONObject(raw) && json.Unmarshal(raw, &sw) == nil {
		if sw.Enabled != nil {
			enabled = *sw.Enabled
		}
		if sw.Trail != nil {
			trail = *sw.Trail
		}
	}
	c.cap.known, c.cap.serverOn, c.cap.trailOn = true, enabled, trail
	if !trail {
		c.cap.trail = nil
	}
	c.updateCaptureGateLocked()
	hold := c.cap.hold
	c.cap.hold = nil
	for _, h := range hold {
		if c.captureGateLocked() {
			trail := ""
			if c.cap.trailOn && h.trail != "" {
				trail = scrubField(h.trail, trailUnits)
			}
			c.queueCaptureLocked(h.p, trail)
		}
		if h.file != "" {
			c.st.remove(h.file)
		}
	}
}

func (c *Client) resetCaptureLocked() {
	c.cap.hold = nil
	c.cap.captured = 0
	c.cap.perKey = map[string]int{}
	c.cap.tokens = capturesPerMinute
	c.cap.tokensAt = time.Time{}
	c.cap.trails = 0
	c.cap.trail = nil
}

func (c *Client) refillLocked(now time.Time) float64 {
	tokens := c.cap.tokens
	if !c.cap.tokensAt.IsZero() && now.After(c.cap.tokensAt) {
		tokens += now.Sub(c.cap.tokensAt).Seconds() * capturesPerMinute / 60
		if tokens > capturesPerMinute {
			tokens = capturesPerMinute
		}
	}
	return tokens
}

func (c *Client) hasRoomLocked(now time.Time) bool {
	return c.cap.captured < capturesPerRun && c.refillLocked(now) >= 1
}

func (c *Client) allowLocked(key string, now time.Time) bool {
	if c.cap.captured >= capturesPerRun || c.cap.perKey[key] >= capturesPerKey {
		return false
	}
	tokens := c.refillLocked(now)
	c.cap.tokensAt = now
	if tokens < 1 {
		c.cap.tokens = tokens
		return false
	}
	c.cap.tokens = tokens - 1
	c.cap.captured++
	c.cap.perKey[key]++
	return true
}

// prepare scrubs a capture's text. It runs without the lock.
func prepare(source, severity, errType, message, stack string, timestamp int64) (prepared, bool) {
	p := prepared{source: source, severity: severity, timestamp: timestamp}
	p.message = strings.TrimSpace(scrubField(validUTF8(message), messageUnits))
	if p.message == "" {
		return p, false
	}
	p.errType = strings.TrimSpace(scrubField(validUTF8(errType), typeUnits))
	p.stack = scrubField(validUTF8(stack), stackUnits)
	first, _, _ := strings.Cut(p.stack, "\n")
	p.key = p.errType + "|" + messageTemplate(p.message) + "|" + first
	return p, true
}

// capture is the one path every automatic capture takes: the gate and caps
// first, the scrub without the lock, then the hold or the queue.
func (c *Client) capture(source, severity, errType, message, stack string) {
	if c == nil || !c.capLive.Load() {
		return
	}
	c.mu.Lock()
	room := c.hasRoomLocked(c.now())
	c.unlock()
	if !room {
		return
	}
	p, ok := prepare(source, severity, errType, message, stack, 0)
	if !ok {
		return
	}
	c.mu.Lock()
	c.captureLocked(p)
	c.unlock()
}

func (c *Client) captureLocked(p prepared) {
	if !c.captureGateLocked() || !c.allowLocked(p.key, c.now()) {
		return
	}
	if !c.cap.known {
		if len(c.cap.hold) < holdMax {
			c.cap.hold = append(c.cap.hold, held{p: p})
		}
		return
	}
	c.queueCaptureLocked(p, "")
}

// queueCaptureLocked queues a capture as an ordinary error event. The
// trail goes with it only while the game's trail switch is on, at most 10
// times a run.
func (c *Client) queueCaptureLocked(p prepared, trail string) {
	fields := map[string]json.RawMessage{
		"rs_kind":     mustJSON("error"),
		"rs_severity": mustJSON(p.severity),
		"rs_message":  mustJSON(p.message),
		"rs_source":   mustJSON(p.source),
	}
	texts := map[string]string{"rs_message": p.message}
	if p.errType != "" {
		fields["rs_error_type"] = mustJSON(p.errType)
	}
	if p.stack != "" {
		fields["rs_stack"] = mustJSON(p.stack)
		texts["rs_stack"] = p.stack
	}
	if trail == "" && c.cap.trailOn && len(c.cap.trail) > 0 && c.cap.trails < trailsPerRun {
		trail = c.trailTextLocked()
		c.cap.trails++
	}
	if trail != "" {
		fields["rs_trail"] = mustJSON(trail)
		texts["rs_trail"] = trail
	}
	c.trackAtLocked("error", fields, true, texts, p.timestamp)
}

// --- The trail -----------------------------------------------------------------

// trailLine is "+<ms since start> <level> <text>", scrubbed, one line, at
// most 120 characters.
func trailLine(since time.Duration, level, text string) string {
	text = strings.NewReplacer("\n", " ", "\r", " ").Replace(text)
	text = strings.TrimSpace(scrubField(validUTF8(text), trailLineUnits))
	if text == "" {
		return ""
	}
	ms := since.Milliseconds()
	if ms < 0 {
		ms = 0
	}
	return cutUnits("+"+strconv.FormatInt(ms, 10)+" "+level+" "+text, trailLineUnits)
}

func (c *Client) pushTrailLocked(line string) {
	if line == "" || !c.cap.trailOn {
		return
	}
	c.cap.trail = append(c.cap.trail, line)
	if len(c.cap.trail) > trailMaxLines {
		c.cap.trail = c.cap.trail[len(c.cap.trail)-trailMaxLines:]
	}
}

// trailTextLocked joins the ring oldest first, dropping the oldest lines
// until it fits 1,500 characters.
func (c *Client) trailTextLocked() string {
	lines := c.cap.trail
	for len(lines) > 1 && utf16Len(strings.Join(lines, "\n")) > trailUnits {
		lines = lines[1:]
	}
	return cutUnits(strings.Join(lines, "\n"), trailUnits)
}

func levelName(l slog.Level) string {
	switch {
	case l < slog.LevelInfo:
		return "debug"
	case l < slog.LevelWarn:
		return "info"
	case l < slog.LevelError:
		return "warning"
	default:
		return "error"
	}
}

// observeLog feeds one log line to the trail and, at error level, to
// capture. The SDK's own lines are never captured.
func (c *Client) observeLog(level slog.Level, msg string, errType string) {
	if c == nil || strings.HasPrefix(msg, "Ravensight") {
		return
	}
	trail := c.trailLive.Load()
	capture := level >= slog.LevelError && c.capLive.Load()
	if !trail && !capture {
		return // the hot path: nothing is copied or scrubbed
	}
	if capture {
		c.capture("log", "warning", errType, msg, "")
	}
	if trail {
		line := trailLine(c.now().Sub(c.runStart), levelName(level), msg)
		c.mu.Lock()
		c.pushTrailLocked(line)
		c.unlock()
	}
}

// Log feeds one line of your game's own log to error capture: every line
// joins the trail (when the game's trail switch is on), and a line at
// slog.LevelError or above is sent as an error with rs_source "log" and
// severity "warning". It never blocks on the network and never fails.
func (c *Client) Log(level slog.Level, msg string) {
	c.observeLog(level, msg, "")
}

// LogHandler wraps a log/slog handler so your game's log feeds error
// capture: records at slog.LevelError or above are sent as errors (the
// message, plus the first error-valued attribute, whose type becomes
// rs_error_type), and every record joins the trail while the game's trail
// switch is on. Every record still reaches next exactly as before.
//
//	slog.SetDefault(slog.New(client.LogHandler(slog.NewTextHandler(os.Stderr, nil))))
func (c *Client) LogHandler(next slog.Handler) slog.Handler {
	if next == nil {
		next = discardHandler{}
	}
	return &logHandler{c: c, next: next}
}

type logHandler struct {
	c    *Client
	next slog.Handler
}

func (h *logHandler) Enabled(ctx context.Context, level slog.Level) bool {
	if h.next.Enabled(ctx, level) {
		return true
	}
	c := h.c
	return c != nil && (c.trailLive.Load() || (level >= slog.LevelError && c.capLive.Load()))
}

func (h *logHandler) Handle(ctx context.Context, r slog.Record) error {
	if c := h.c; c != nil && (c.trailLive.Load() || (r.Level >= slog.LevelError && c.capLive.Load())) {
		msg, errType := r.Message, ""
		r.Attrs(func(a slog.Attr) bool {
			if err, ok := a.Value.Any().(error); ok && err != nil {
				msg += ": " + err.Error()
				errType = fmt.Sprintf("%T", err)
				return false
			}
			return true
		})
		c.observeLog(r.Level, msg, errType)
	}
	if h.next.Enabled(ctx, r.Level) {
		return h.next.Handle(ctx, r)
	}
	return nil
}

func (h *logHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &logHandler{c: h.c, next: h.next.WithAttrs(attrs)}
}

func (h *logHandler) WithGroup(name string) slog.Handler {
	return &logHandler{c: h.c, next: h.next.WithGroup(name)}
}

type discardHandler struct{}

func (discardHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (discardHandler) Handle(context.Context, slog.Record) error { return nil }
func (d discardHandler) WithAttrs([]slog.Attr) slog.Handler      { return d }
func (d discardHandler) WithGroup(string) slog.Handler           { return d }

// --- Requests ------------------------------------------------------------------

// WrapTransport wraps an http.RoundTripper so your game's failed requests
// are captured: a network failure or a 5xx as severity "error", a 4xx as
// "warning", with the message "<METHOD> <host><path> <status or network>".
// Path segments that are numbers, long hex or UUIDs become ":id". Never a
// query string, a fragment, a header or a body. The SDK's own requests are
// never captured, and the answer reaches your code unchanged.
//
//	httpClient := &http.Client{Transport: client.WrapTransport(http.DefaultTransport)}
func (c *Client) WrapTransport(next http.RoundTripper) http.RoundTripper {
	if next == nil {
		next = http.DefaultTransport
	}
	return &captureTransport{c: c, next: next}
}

type captureTransport struct {
	c    *Client
	next http.RoundTripper
}

func (t *captureTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.next.RoundTrip(req)
	c := t.c
	if c == nil || !c.capLive.Load() || req.Context().Value(sdkRequestKey{}) != nil ||
		strings.EqualFold(req.URL.Host, c.apiHost) {
		return resp, err
	}
	switch {
	case err != nil:
		if errors.Is(err, context.Canceled) {
			return resp, err // the game cancelled it; not a failure
		}
		c.capture("request", "error", "", requestMessage(req, "network"), "")
	case resp.StatusCode >= 500:
		c.capture("request", "error", "", requestMessage(req, strconv.Itoa(resp.StatusCode)), "")
	case resp.StatusCode >= 400:
		c.capture("request", "warning", "", requestMessage(req, strconv.Itoa(resp.StatusCode)), "")
	}
	return resp, err
}

var (
	idSegment   = regexp.MustCompile(`^(?:\d+|[0-9a-fA-F]{8,}|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$`)
	userinfoish = regexp.MustCompile(`^[^@]*@`)
)

func requestMessage(req *http.Request, status string) string {
	host := strings.ToLower(userinfoish.ReplaceAllString(req.URL.Host, ""))
	segments := strings.Split(req.URL.EscapedPath(), "/")
	for i, s := range segments {
		if idSegment.MatchString(s) {
			segments[i] = ":id"
		}
	}
	return req.Method + " " + host + strings.Join(segments, "/") + " " + status
}

// --- Panics --------------------------------------------------------------------

// Recover records a panic for the next launch, then panics again with the
// same value, so the game ends exactly as it would have. Defer it first
// thing in main and in each goroutine you start:
//
//	defer client.Recover()
//
// The record holds the panic's type, its message and the stack, scrubbed on
// the device, and is written synchronously to the storage directory. The
// next launch with a key and consent sends it once as an error with
// rs_source "crash" and severity "critical". Nothing is written without a
// key, before consent, while capture is off, or in a playtest run.
func (c *Client) Recover() {
	r := recover()
	if r == nil {
		return
	}
	c.recordCrash(r, debug.Stack())
	panic(r)
}

func (c *Client) recordCrash(r any, stack []byte) {
	if c == nil || c.inert || c.isPlaytest || !c.capLive.Load() || !c.st.enabled() {
		return
	}
	message := fmt.Sprint(r)
	if err, ok := r.(error); ok {
		message = err.Error()
	}
	if strings.TrimSpace(message) == "" {
		message = "panic"
	}
	rec := crashRecord{
		Timestamp: c.now().UnixMilli(),
		Type:      strings.TrimSpace(scrubField(fmt.Sprintf("%T", r), typeUnits)),
		Message:   strings.TrimSpace(scrubField(validUTF8(message), messageUnits)),
		Stack:     scrubField(validUTF8(panicStack(string(stack))), stackUnits),
	}
	c.mu.Lock()
	if c.cap.known && c.cap.trailOn && len(c.cap.trail) > 0 {
		rec.Trail = c.trailTextLocked() // only while the trail switch is on
	}
	c.unlock()
	raw, err := marshalJSON(rec)
	if err != nil {
		return
	}
	_ = c.st.write(fmt.Sprintf("%s%d-%s.json", crashPrefix, rec.Timestamp, randomHex(3)), raw)
	c.pruneCrashes()
}

// panicStack keeps the frames from the panicking function down: the
// goroutine header, debug.Stack, Recover and the runtime's panic frames are
// dropped, and so are the program counter offsets.
func panicStack(stack string) string {
	lines := strings.Split(strings.TrimRight(stack, "\n"), "\n")
	start := 0
	for i, line := range lines {
		if strings.HasPrefix(line, "panic(") {
			start = i + 2
		}
	}
	if start == 0 && len(lines) > 0 && strings.HasPrefix(lines[0], "goroutine ") {
		start = 1
	}
	if start > len(lines) {
		start = len(lines)
	}
	out := lines[start:]
	for i, line := range out {
		if j := strings.LastIndex(line, " +0x"); j > 0 {
			out[i] = line[:j]
		}
	}
	return strings.Join(out, "\n")
}

// pruneCrashes keeps the newest 5 records.
func (c *Client) pruneCrashes() {
	names := c.st.list(crashPrefix)
	if len(names) <= crashMax {
		return
	}
	sort.Slice(names, func(i, j int) bool { return crashTime(names[i]) < crashTime(names[j]) })
	for _, name := range names[:len(names)-crashMax] {
		c.st.remove(name)
	}
}

func crashTime(name string) int64 {
	rest := strings.TrimPrefix(name, crashPrefix)
	digits, _, _ := strings.Cut(rest, "-")
	n, _ := strconv.ParseInt(digits, 10, 64)
	return n
}

// loadCrashesLocked sends what the last run recorded, through the caps and
// the boot hold. Records older than 30 days are deleted unsent.
func (c *Client) loadCrashesLocked() {
	if !c.captureGateLocked() || c.isPlaytest {
		return
	}
	names := c.st.list(crashPrefix)
	sort.Slice(names, func(i, j int) bool { return crashTime(names[i]) < crashTime(names[j]) })
	for _, name := range names {
		if c.holdsFileLocked(name) {
			continue
		}
		raw, err := c.st.read(name)
		var rec crashRecord
		if err != nil || json.Unmarshal(raw, &rec) != nil || rec.Message == "" ||
			c.now().Sub(time.UnixMilli(rec.Timestamp)) > crashMaxAge {
			c.st.remove(name)
			continue
		}
		p, ok := prepare("crash", "critical", rec.Type, rec.Message, rec.Stack, rec.Timestamp)
		if !ok {
			c.st.remove(name)
			continue
		}
		if !c.allowLocked(p.key, c.now()) {
			c.st.remove(name)
			continue
		}
		if !c.cap.known {
			if len(c.cap.hold) < holdMax {
				c.cap.hold = append(c.cap.hold, held{p: p, file: name, trail: rec.Trail})
			}
			continue
		}
		trail := ""
		if c.cap.trailOn {
			trail = scrubField(rec.Trail, trailUnits)
		}
		c.queueCaptureLocked(p, trail)
		c.st.remove(name)
	}
}

func (c *Client) holdsFileLocked(name string) bool {
	for _, h := range c.cap.hold {
		if h.file == name {
			return true
		}
	}
	return false
}

// deleteCrashesLocked deletes every pending record. Opt-out and a device id
// reset call it.
func (c *Client) deleteCrashesLocked() {
	for _, name := range c.st.list(crashPrefix) {
		c.st.remove(name)
	}
	c.cap.hold = nil
}
