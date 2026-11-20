package ravensight

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Server mode: your own game server or backend sends events into a
// website's analytics (a title's website or your company website) with
// that website's secret server key. It never sends a player's device id:
// identity is what you pass as ActorID, explicitly, per event.

// ServerConfig configures a Server.
type ServerConfig struct {
	// ServerKey is the website's secret gt_srv_ key. Keep it on your server,
	// never in a game build or a browser. Empty makes the Server inert.
	ServerKey string
	// Optional silences the warning an inert Server logs.
	Optional bool
	// APIURL is the API's address. Empty means DefaultAPIURL.
	APIURL string
	// HTTPClient sends the requests. Nil uses a client of the SDK's own.
	HTTPClient *http.Client
	// Logger receives the SDK's own lines. Nil writes warnings to standard
	// error.
	Logger *slog.Logger
	// MaxQueueSize caps events waiting in memory for Track. Zero means
	// 10,000. Track refuses an event when the queue is full.
	MaxQueueSize int
	// FlushInterval is how long a tracked event may wait to be batched.
	// Zero means 5 seconds; negative sends only on Flush, at 50 waiting
	// events and on Close.
	FlushInterval time.Duration
	// RequestTimeout bounds each request. Zero means 15 seconds.
	RequestTimeout time.Duration
}

// ServerEvent is one event from your server.
type ServerEvent struct {
	// EventID makes sending idempotent: the same id and content again is a
	// duplicate, never counted twice. 8 to 128 of A-Z a-z 0-9 _ . : -.
	// Empty means the SDK makes one when the event is first sent or queued.
	EventID string
	// Name starts with a letter, then up to 79 of letters, digits, _ . -.
	Name string
	// Timestamp is when it happened: at most 5 minutes ahead and 90 days
	// back. Zero means now.
	Timestamp time.Time
	// ActorID is your own opaque id for the person or device, 8 to 128 of
	// A-Z a-z 0-9 _ . : - (so never an email). The server stores only a
	// keyed hash of it. Empty means the event belongs to your backend
	// itself, which is never counted as an actor.
	ActorID string
	// Properties are at most 24 short scalar values: strings of at most 200
	// characters, finite numbers, bools or nil, at most 4 KB as JSON. Keys
	// start with a letter, then letters, digits or _, at most 64. Keys
	// starting rs_, history_ or ravensight_ are reserved. A string with an
	// @, a credential label, a JWT, a key or a run of 80 or more letters and
	// digits is refused.
	Properties map[string]any
}

// ServerResult is the server's answer for the events sent.
type ServerResult struct {
	Accepted    int      // stored and counted
	Duplicates  int      // already stored with the same id and content
	Conflicts   int      // an id already stored with other content; never stored
	Refused     int      // stopped by a pause (the studio's balance); free, send again later
	AcceptedIDs []string // accepted and duplicates
	ConflictIDs []string
	RefusedIDs  []string
	Paused      bool
	Reason      string   // why it paused, such as "needs_funds"
	Invalid     int      // refused by the server as invalid (a 400 naming the event); never stored
	InvalidIDs  []string // their ids
}

// ServerStats are a Server's counters.
type ServerStats struct {
	Queued    int
	Sent      int // accepted or duplicate
	Conflicts int
	Dropped   int // refused by the server or dropped as invalid
	LastError string
}

// ValidationError names the first invalid event of a call, by its index.
type ValidationError struct {
	Index  int
	Field  string
	Reason string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("ravensight: server event %d: %s %s", e.Index, e.Field, e.Reason)
}

var (
	serverKeyPattern   = regexp.MustCompile(`^gt_srv_[A-Za-z0-9_-]{43}$`)
	opaqueIDPattern    = regexp.MustCompile(`^[A-Za-z0-9_.:-]{8,128}$`)
	serverNamePattern  = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_.-]{0,79}$`)
	propertyKeyPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,63}$`)
	reservedPropKey    = regexp.MustCompile(`(?i)^(?:rs_|history_|ravensight_)|^(?:context|identity_kind|session_provenance|profile_provenance)$`)
	unsafeValue        = regexp.MustCompile(`(?i)@|%40|(?:bearer|password|secret|token|authorization)[=:]|[A-Za-z0-9_-]{80,}`)
	bearerValue        = regexp.MustCompile(`(?i)(?:^|\s)bearer\s`)
	jwtValue           = regexp.MustCompile(`^eyJ[A-Za-z0-9_-]+\.`)
	keyValue           = regexp.MustCompile(`\bgt_[a-z]+_[A-Za-z0-9_-]{8,}`)
	badEventIndex      = regexp.MustCompile(`events\[(\d+)\]`)
)

const (
	serverMaxProps     = 24
	serverMaxPropBytes = 4096
	serverMaxString    = 200
	serverFuture       = 5 * time.Minute
	serverPast         = 90 * 24 * time.Hour
	serverDefaultQueue = 10000
)

// Server sends server events. Make one with NewServer and keep it; every
// method is safe from any goroutine.
type Server struct {
	cfg        ServerConfig
	api        string
	httpc      *http.Client
	logger     *slog.Logger
	inert      Status // "" when it has a well-formed key
	flushEvery time.Duration
	timeout    time.Duration
	maxQueue   int
	now        func() time.Time
	random     func() float64
	counter    atomic.Uint64
	nonce      string

	mu          sync.Mutex
	queue       []ServerEvent
	stopped     string
	backoff     time.Duration
	nextAttempt time.Time // the backoff: a flush or a close cuts it short
	retryUntil  time.Time // the server's Retry-After or a fixed pause: never cut
	batchDue    time.Time
	batchLimit  int
	stats       ServerStats
	closing     bool

	wake      chan struct{}
	flushReq  chan chan struct{}
	stopReq   chan struct{}
	done      chan struct{}
	runCtx    context.Context
	cancelRun context.CancelFunc
}

// NewServer makes a Server and starts its goroutine. It never fails: with
// no key, or a key that is not a gt_srv_ server key, it is inert and every
// call returns a *RefusalError.
func NewServer(cfg ServerConfig) *Server {
	return newServer(cfg, time.Now, rand.Float64)
}

func newServer(cfg ServerConfig, now func() time.Time, random func() float64) *Server {
	s := &Server{
		cfg: cfg, api: normalizeAPIURL(cfg.APIURL), now: now, random: random,
		nonce: randomHex(4), batchLimit: maxBatch,
		wake: make(chan struct{}, 1), flushReq: make(chan chan struct{}),
		stopReq: make(chan struct{}), done: make(chan struct{}),
	}
	s.logger = cfg.Logger
	if s.logger == nil {
		s.logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	}
	s.httpc = cfg.HTTPClient
	if s.httpc == nil {
		s.httpc = &http.Client{}
	}
	s.flushEvery = cfg.FlushInterval
	if s.flushEvery == 0 {
		s.flushEvery = defaultFlush
	}
	s.timeout = cfg.RequestTimeout
	if s.timeout <= 0 {
		s.timeout = defaultTimeout
	}
	s.maxQueue = cfg.MaxQueueSize
	if s.maxQueue <= 0 {
		s.maxQueue = serverDefaultQueue
	}
	key := strings.TrimSpace(cfg.ServerKey)
	switch {
	case key == "":
		s.inert = StatusNoKey
		if !cfg.Optional {
			s.logger.Warn("Ravensight: no server key, so server events are not sent. Set ServerConfig.ServerKey, or Optional to silence this.")
		}
	case !serverKeyPattern.MatchString(key):
		s.inert = "invalid_server_key"
		s.logger.Warn("Ravensight: ServerConfig.ServerKey is not a gt_srv_ server key, so server events are not sent. A game's gt_live_ or gt_pub_ key belongs in Config.IngestKey.")
	}
	if s.inert != "" {
		close(s.done)
		return s
	}
	s.cfg.ServerKey = key
	s.runCtx, s.cancelRun = context.WithCancel(context.Background())
	go s.run()
	return s
}

// validateServerEvent checks one event the way the server does, so one bad
// event never costs a whole batch. ev is completed in place: an id and a
// timestamp when missing.
func (s *Server) prepareEvent(ev *ServerEvent, index int, now time.Time) error {
	if ev.EventID == "" {
		ev.EventID = fmt.Sprintf("srv-%s-%d-%d", s.nonce, now.UnixMilli(), s.counter.Add(1))
	}
	if ev.Timestamp.IsZero() {
		ev.Timestamp = now
	}
	fail := func(field, reason string) error { return &ValidationError{Index: index, Field: field, Reason: reason} }
	switch {
	case !opaqueIDPattern.MatchString(ev.EventID):
		return fail("EventID", "must be 8 to 128 of A-Z a-z 0-9 _ . : -")
	case !serverNamePattern.MatchString(ev.Name):
		return fail("Name", "must start with a letter, then up to 79 of letters, digits, _ . -")
	case ev.ActorID != "" && !opaqueIDPattern.MatchString(ev.ActorID):
		return fail("ActorID", "must be 8 to 128 of A-Z a-z 0-9 _ . : -")
	case ev.Timestamp.After(now.Add(serverFuture)) || ev.Timestamp.Before(now.Add(-serverPast)):
		return fail("Timestamp", "must be at most 5 minutes ahead and 90 days back")
	}
	if err := validProperties(ev.Properties); err != "" {
		return fail("Properties", err)
	}
	// A copy: the caller may reuse its map, and a resend must carry exactly
	// the content its id was first sent with. The values are scalars.
	ev.Properties = maps.Clone(ev.Properties)
	return nil
}

func validProperties(props map[string]any) string {
	if props == nil {
		return ""
	}
	if len(props) > serverMaxProps {
		return "has more than 24 keys"
	}
	raw, err := marshalJSON(props)
	if err != nil {
		return "cannot be written as JSON"
	}
	if len(raw) > serverMaxPropBytes {
		return "is over 4,096 bytes as JSON"
	}
	for k, v := range props {
		if !propertyKeyPattern.MatchString(k) || reservedPropKey.MatchString(k) {
			return fmt.Sprintf("key %q is not allowed", k)
		}
		if !scalarOK(v) {
			return fmt.Sprintf("value of %q is not allowed", k)
		}
	}
	return ""
}

func scalarOK(v any) bool {
	switch x := v.(type) {
	case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	case float32:
		return !math.IsInf(float64(x), 0) && !math.IsNaN(float64(x))
	case float64:
		return !math.IsInf(x, 0) && !math.IsNaN(x)
	case json.Number:
		f, err := x.Float64()
		return err == nil && !math.IsInf(f, 0)
	case string:
		return utf16Len(x) <= serverMaxString && !unsafeValue.MatchString(x) && !bearerValue.MatchString(x) &&
			!jwtValue.MatchString(x) && !keyValue.MatchString(x)
	}
	return false
}

func (s *Server) refusal() error { return refused(string(s.inert)) }

// Send validates the events, sends them now in batches of at most 50, and
// waits for the answers. A 429, a 5xx or a timeout is retried with a
// backoff (a Retry-After exactly), and a 409 site_changed after a second,
// until ctx ends; a pause
// for the studio's balance is not retried, its ids are in RefusedIDs. It
// returns a *ValidationError naming the first invalid event before
// anything is sent, or a *RefusalError when the server refuses the key.
func (s *Server) Send(ctx context.Context, events ...ServerEvent) (ServerResult, error) {
	var result ServerResult
	if s == nil || s.inert != "" {
		if s == nil {
			return result, refused(string(StatusNoKey))
		}
		return result, s.refusal()
	}
	now := s.now()
	batch := make([]ServerEvent, len(events))
	copy(batch, events)
	for i := range batch {
		if err := s.prepareEvent(&batch[i], i, now); err != nil {
			return result, err
		}
	}
	backoff := time.Duration(0)
	limit := maxBatch
	for len(batch) > 0 {
		if reason := s.stoppedReason(); reason != "" {
			return result, refused(reason)
		}
		n := min(len(batch), limit)
		out := s.post(ctx, batch[:n])
		switch out.kind {
		case outcomeDone:
			result.add(out.result)
			batch = batch[n:]
			backoff, limit = 0, maxBatch
		case outcomeDropOne:
			result.Invalid++
			result.InvalidIDs = append(result.InvalidIDs, batch[out.index].EventID)
			batch = append(batch[:out.index:out.index], batch[out.index+1:]...)
		case outcomeSplit:
			if n == 1 {
				result.Invalid++
				result.InvalidIDs = append(result.InvalidIDs, batch[0].EventID)
				batch = batch[1:]
				continue
			}
			limit = n / 2
		case outcomeStop, outcomeDrop:
			return result, &RefusalError{Code: out.code, Status: out.status}
		case outcomeRetry:
			wait := out.wait
			if !out.exact {
				backoff = nextBackoff(backoff)
				wait = jitter(backoff, s.random)
			}
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return result, ctx.Err()
			case <-timer.C:
			}
		}
	}
	return result, nil
}

func (r *ServerResult) add(o ServerResult) {
	r.Accepted += o.Accepted
	r.Duplicates += o.Duplicates
	r.Conflicts += o.Conflicts
	r.Refused += o.Refused
	r.AcceptedIDs = append(r.AcceptedIDs, o.AcceptedIDs...)
	r.ConflictIDs = append(r.ConflictIDs, o.ConflictIDs...)
	r.RefusedIDs = append(r.RefusedIDs, o.RefusedIDs...)
	r.Invalid += o.Invalid
	r.InvalidIDs = append(r.InvalidIDs, o.InvalidIDs...)
	if o.Paused {
		r.Paused, r.Reason = true, o.Reason
	}
}

func nextBackoff(d time.Duration) time.Duration {
	if d == 0 {
		return backoffBase
	}
	if d*2 > backoffMax {
		return backoffMax
	}
	return d * 2
}

func jitter(d time.Duration, random func() float64) time.Duration {
	f := 0.5 + random()*0.5
	if f >= 1 {
		f = 0.999
	}
	return time.Duration(float64(d) * f)
}

type outcomeKind int

const (
	outcomeDone    outcomeKind = iota // answered: accepted, duplicate, conflict or paused
	outcomeRetry                      // keep the events, wait and send again
	outcomeSplit                      // too large: halve
	outcomeDropOne                    // the server named one bad event
	outcomeDrop                       // the batch was refused as invalid
	outcomeStop                       // the key or the feature is refused: stop
)

type outcome struct {
	kind   outcomeKind
	result ServerResult
	index  int
	wait   time.Duration
	exact  bool // wait is the server's Retry-After or a fixed pause, not the backoff
	code   string
	status int
}

type wireServerEvent struct {
	EventID    string         `json:"eventId"`
	Name       string         `json:"name"`
	Timestamp  int64          `json:"timestamp"`
	ActorID    string         `json:"actorId,omitempty"`
	Properties map[string]any `json:"properties,omitempty"`
}

// post sends one batch and reads the answer. It never sets an Origin
// header: a server key is refused from a browser.
func (s *Server) post(ctx context.Context, batch []ServerEvent) outcome {
	wire := make([]wireServerEvent, len(batch))
	for i, ev := range batch {
		wire[i] = wireServerEvent{EventID: ev.EventID, Name: ev.Name, Timestamp: ev.Timestamp.UnixMilli(), ActorID: ev.ActorID, Properties: ev.Properties}
	}
	body, err := marshalJSON(map[string]any{"events": wire})
	if err != nil {
		return outcome{kind: outcomeDrop, code: "invalid_events"}
	}
	rctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	sent := body
	gz := len(body) >= gzipFrom
	if gz {
		sent = gzipped(body)
	}
	req, err := http.NewRequestWithContext(context.WithValue(rctx, sdkRequestKey{}, true), "POST", s.api+"/site/server-events", bytes.NewReader(sent))
	if err != nil {
		return outcome{kind: outcomeStop, code: "invalid_api_url"}
	}
	req.Header.Set("Authorization", "Bearer "+s.cfg.ServerKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "ravensight-go/"+Version)
	if gz {
		req.Header.Set("Content-Encoding", "gzip")
	}
	resp, err := s.httpc.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return outcome{kind: outcomeRetry, code: "cancelled"}
		}
		s.setLastError("offline")
		return outcome{kind: outcomeRetry, code: "offline"}
	}
	defer resp.Body.Close()
	a := answer{status: resp.StatusCode}
	a.body, _ = io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var probe struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	_ = json.Unmarshal(a.body, &probe)
	a.code = probe.Error
	if a.code != "" {
		s.setLastError(a.code)
	}
	wait, hasWait := parseRetryAfter(resp.Header.Get("Retry-After"), s.now())

	switch {
	case a.status == 200 || (a.status == 429 && a.code == "collection_paused"):
		var r struct {
			Accepted    int      `json:"accepted"`
			Duplicates  int      `json:"duplicates"`
			Conflicts   int      `json:"conflicts"`
			Refused     int      `json:"refused"`
			AcceptedIDs []string `json:"acceptedIds"`
			ConflictIDs []string `json:"conflictIds"`
			RefusedIDs  []string `json:"refusedIds"`
			Paused      bool     `json:"paused"`
			Reason      string   `json:"reason"`
		}
		_ = json.Unmarshal(a.body, &r)
		res := ServerResult{Accepted: r.Accepted, Duplicates: r.Duplicates, Conflicts: r.Conflicts, Refused: r.Refused,
			AcceptedIDs: r.AcceptedIDs, ConflictIDs: r.ConflictIDs, RefusedIDs: r.RefusedIDs, Paused: r.Paused, Reason: r.Reason}
		if a.status == 429 {
			res.Paused = true
			if res.Reason == "" {
				res.Reason = "needs_funds"
			}
			if len(res.RefusedIDs) == 0 {
				for _, ev := range batch {
					res.RefusedIDs = append(res.RefusedIDs, ev.EventID)
				}
				res.Refused = len(res.RefusedIDs)
			}
		}
		return outcome{kind: outcomeDone, result: res}
	case a.status == 400:
		if m := badEventIndex.FindStringSubmatch(probe.Message); m != nil {
			if i, err := strconv.Atoi(m[1]); err == nil && i >= 0 && i < len(batch) {
				return outcome{kind: outcomeDropOne, index: i, code: a.code, status: a.status}
			}
		}
		return outcome{kind: outcomeDrop, code: a.code, status: a.status}
	case a.status == 413:
		return outcome{kind: outcomeSplit, code: "payload_too_large", status: a.status}
	case a.status == 401 || a.status == 403 || a.status == 404:
		code := a.code
		if code == "" {
			code = "http_" + strconv.Itoa(a.status)
		}
		s.stop(code, a.status)
		return outcome{kind: outcomeStop, code: code, status: a.status}
	case a.status == 429 || a.status == 503:
		if hasWait {
			return outcome{kind: outcomeRetry, wait: wait, exact: true, code: a.code, status: a.status}
		}
		return outcome{kind: outcomeRetry, code: a.code, status: a.status}
	case a.status == 409:
		return outcome{kind: outcomeRetry, wait: time.Second, exact: true, code: a.code, status: a.status}
	default:
		return outcome{kind: outcomeRetry, code: a.code, status: a.status}
	}
}

func (s *Server) setLastError(code string) {
	s.mu.Lock()
	s.stats.LastError = code
	s.mu.Unlock()
}

func (s *Server) stoppedReason() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stopped
}

func (s *Server) stop(code string, status int) {
	s.mu.Lock()
	first := s.stopped == ""
	if first {
		s.stopped = code
		s.stats.Dropped += len(s.queue)
		s.queue = nil
	}
	s.mu.Unlock()
	if first {
		s.logger.Warn(fmt.Sprintf("Ravensight: server events refused (%s, HTTP %d), sending stopped. Check the website's server key and that website analytics is on.", code, status))
	}
}

// Track validates an event and queues it to be sent in the background. It
// never waits on the network. It returns a *ValidationError for an invalid
// event, or a *RefusalError ("queue_full", the stop reason, or "closed").
func (s *Server) Track(ev ServerEvent) error {
	if s == nil {
		return refused(string(StatusNoKey))
	}
	if s.inert != "" {
		return s.refusal()
	}
	if err := s.prepareEvent(&ev, 0, s.now()); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.stopped != "":
		return refused(s.stopped)
	case s.closing:
		return refused("closed")
	case len(s.queue) >= s.maxQueue:
		s.stats.Dropped++
		return refused("queue_full")
	}
	if len(s.queue) == 0 {
		s.batchDue = s.now().Add(s.flushEvery)
		s.signal()
	}
	s.queue = append(s.queue, ev)
	if len(s.queue) >= maxBatch {
		s.signal()
	}
	return nil
}

func (s *Server) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Server) run() {
	defer close(s.done)
	for {
		s.mu.Lock()
		wakeAt := s.nextWakeLocked()
		s.mu.Unlock()
		var timerC <-chan time.Time
		var timer *time.Timer
		if !wakeAt.IsZero() {
			timer = time.NewTimer(max(0, wakeAt.Sub(s.now())))
			timerC = timer.C
		}
		var replies []chan struct{}
		urgent, stopping := false, false
		select {
		case <-s.wake:
		case <-timerC:
		case r := <-s.flushReq:
			replies, urgent = append(replies, r), true
		case <-s.stopReq:
			stopping, urgent = true, true
		}
		if timer != nil {
			timer.Stop()
		}
		s.drain(urgent)
		for _, r := range replies {
			close(r)
		}
		if stopping {
			return
		}
	}
}

func (s *Server) nextWakeLocked() time.Time {
	if len(s.queue) == 0 || s.stopped != "" {
		return time.Time{}
	}
	at := s.batchDue
	if s.flushEvery < 0 && len(s.queue) < maxBatch {
		at = time.Time{}
	}
	if len(s.queue) >= maxBatch {
		at = s.now()
	}
	if !at.IsZero() && s.nextAttempt.After(at) {
		at = s.nextAttempt
	}
	if !at.IsZero() && s.retryUntil.After(at) {
		at = s.retryUntil
	}
	return at
}

// drain sends queued events until the queue is empty, a send fails or the
// run ends. A flush or a close cuts the backoff short once.
func (s *Server) drain(urgent bool) {
	for s.runCtx.Err() == nil {
		s.mu.Lock()
		now := s.now()
		due := len(s.queue) > 0 && s.stopped == "" &&
			(urgent || s.closing || len(s.queue) >= maxBatch || (s.flushEvery > 0 && !now.Before(s.batchDue))) &&
			(urgent || !now.Before(s.nextAttempt)) && !now.Before(s.retryUntil)
		if !due {
			s.mu.Unlock()
			return
		}
		n := min(len(s.queue), s.batchLimit)
		batch := append([]ServerEvent(nil), s.queue[:n]...)
		s.mu.Unlock()

		out := s.post(s.runCtx, batch)

		s.mu.Lock()
		switch out.kind {
		case outcomeDone:
			s.removeLocked(n)
			s.stats.Sent += out.result.Accepted + out.result.Duplicates
			s.stats.Conflicts += out.result.Conflicts
			if out.result.Conflicts > 0 {
				s.logOnce("Ravensight: a server event id was reused with other content; that event was not stored.")
			}
			s.batchLimit = maxBatch
			if len(out.result.RefusedIDs) > 0 {
				// Paused for the studio's balance: free, kept and sent later.
				refusedIDs := map[string]bool{}
				for _, id := range out.result.RefusedIDs {
					refusedIDs[id] = true
				}
				var keep []ServerEvent
				for _, ev := range batch {
					if refusedIDs[ev.EventID] {
						keep = append(keep, ev)
					}
				}
				s.queue = append(keep, s.queue...)
				s.backoff = max(nextBackoff(s.backoff), time.Minute)
				s.nextAttempt = s.now().Add(jitter(s.backoff, s.random))
				urgent = false
			} else {
				s.backoff, s.nextAttempt = 0, time.Time{}
			}
			if len(s.queue) > 0 {
				s.batchDue = s.now().Add(s.flushEvery)
			}
		case outcomeDropOne:
			s.queue = append(s.queue[:out.index:out.index], s.queue[out.index+1:]...)
			s.stats.Dropped++
		case outcomeDrop:
			s.removeLocked(n)
			s.stats.Dropped += n
		case outcomeSplit:
			if n == 1 {
				s.removeLocked(1)
				s.stats.Dropped++
			} else {
				s.batchLimit = n / 2
			}
		case outcomeStop:
			// stop() already cleared the queue.
		case outcomeRetry:
			if out.exact {
				s.retryUntil = s.now().Add(out.wait)
			} else {
				s.backoff = nextBackoff(s.backoff)
				s.nextAttempt = s.now().Add(jitter(s.backoff, s.random))
			}
			urgent = false
		}
		s.mu.Unlock()
	}
}

func (s *Server) removeLocked(n int) {
	if n > len(s.queue) {
		n = len(s.queue)
	}
	s.queue = append([]ServerEvent(nil), s.queue[n:]...)
}

var serverWarned sync.Map

func (s *Server) logOnce(msg string) {
	if _, loaded := serverWarned.LoadOrStore(s.nonce+msg, true); !loaded {
		s.logger.Warn(msg)
	}
}

// Flush sends the queued events now, cutting a backoff short (never a
// Retry-After), and waits until the queue is empty, a send fails, or ctx
// ends. It returns nil when nothing is left, ErrNotSent when events remain,
// or ctx's error.
func (s *Server) Flush(ctx context.Context) error {
	if s == nil || s.inert != "" {
		return nil
	}
	reply := make(chan struct{})
	select {
	case s.flushReq <- reply:
	case <-s.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-reply:
	case <-s.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	s.mu.Lock()
	n := len(s.queue)
	s.mu.Unlock()
	if n > 0 {
		return fmt.Errorf("%w: %d server events left", ErrNotSent, n)
	}
	return nil
}

// Close sends what it can for at most about 1.5 seconds and stops. Events
// still queued after that are dropped: server mode keeps nothing on disk.
func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultCloseLimit)
	defer cancel()
	return s.Shutdown(ctx)
}

// Shutdown is Close with your own time limit.
func (s *Server) Shutdown(ctx context.Context) error {
	if s == nil || s.inert != "" {
		return nil
	}
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		select {
		case <-s.done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.closing = true
	s.mu.Unlock()
	close(s.stopReq)
	var err error
	select {
	case <-s.done:
	case <-ctx.Done():
		err = ctx.Err()
		s.cancelRun()
		<-s.done
	}
	s.cancelRun()
	s.mu.Lock()
	if left := len(s.queue); left > 0 {
		s.stats.Dropped += left
		s.queue = nil
		if err == nil {
			err = fmt.Errorf("%w: %d server events dropped at close", ErrNotSent, left)
		}
	}
	s.mu.Unlock()
	return err
}

// Stats returns the Server's counters.
func (s *Server) Stats() ServerStats {
	if s == nil {
		return ServerStats{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.stats
	st.Queued = len(s.queue)
	return st
}
