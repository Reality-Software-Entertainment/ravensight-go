package ravensight

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// The SDK's own requests carry this context key, so the request capture of
// WrapTransport never reports them.
type sdkRequestKey struct{}

type action int

const (
	actNone action = iota
	actSettings
	actSession
	actIdentify
	actConfig
	actFeedback
	actBatch
)

// run is the client's one goroutine. Every request and every queue file
// write happens here, never on the caller's goroutine.
func (c *Client) run() {
	defer close(c.done)
	for {
		c.mu.Lock()
		wakeAt := c.nextWakeLocked(c.now())
		c.unlock()
		var timer *time.Timer
		var timerC <-chan time.Time
		if !wakeAt.IsZero() {
			d := wakeAt.Sub(c.now())
			if d < 0 {
				d = 0
			}
			timer = time.NewTimer(d)
			timerC = timer.C
		}
		var replies []chan struct{}
		urgent := false
		stopping := false
		select {
		case <-c.wake:
		case <-timerC:
		case r := <-c.flushReq:
			replies = append(replies, r)
			urgent = true
		case <-c.stopReq:
			stopping = true
			urgent = true
		}
		if timer != nil {
			timer.Stop()
		}
		c.cycle(urgent)
		if stopping {
			c.persistForce()
		} else {
			c.persistIfDue()
		}
		for _, r := range replies {
			close(r)
		}
		if stopping {
			return
		}
	}
}

// cycle does every due action in turn. A flush or a close cuts a backoff
// short once; a failure inside the cycle ends that.
func (c *Client) cycle(urgent bool) {
	for i := 0; i < 200; i++ {
		if c.runCtx.Err() != nil {
			return
		}
		c.mu.Lock()
		act := c.nextActionLocked(c.now(), urgent)
		c.unlock()
		if act == actNone {
			return
		}
		var ok bool
		switch act {
		case actSettings:
			ok = c.readSettings()
		case actSession:
			ok = c.openSession()
		case actIdentify:
			ok = c.sendIdentify()
		case actConfig:
			ok = c.fetchConfig()
		case actFeedback:
			ok = c.sendFeedback()
		case actBatch:
			ok = c.sendBatch()
		}
		if !ok {
			urgent = false
		}
	}
}

// nextActionLocked decides what the worker does next.
func (c *Client) nextActionLocked(now time.Time, urgent bool) action {
	if !c.activeLocked() || now.Before(c.retryUntil) || (!urgent && now.Before(c.nextAttempt)) {
		return actNone
	}
	sendable := c.sendableLocked()
	eventsDue := sendable > 0 && (urgent || c.closing || sendable >= maxBatch ||
		(c.flushEvery > 0 && !now.Before(c.batchDue)))
	feedbackDue := len(c.jobs) > 0
	identifyWants := c.identifyWantsSessionLocked()
	configDue := c.rc.wanted
	if !c.settingsRead {
		if eventsDue || feedbackDue || identifyWants || configDue || sendable > 0 {
			return actSettings
		}
		return actNone
	}
	needSession := eventsDue || feedbackDue || identifyWants
	if needSession && !c.sessionValidLocked(now) {
		return actSession
	}
	switch {
	case c.identifyReadyLocked():
		return actIdentify
	case configDue:
		return actConfig
	case feedbackDue:
		return actFeedback
	case eventsDue:
		return actBatch
	}
	return actNone
}

// nextWakeLocked is when the worker should look again without being woken.
func (c *Client) nextWakeLocked(now time.Time) time.Time {
	var at time.Time
	consider := func(t time.Time) {
		if !t.IsZero() && (at.IsZero() || t.Before(at)) {
			at = t
		}
	}
	if c.dirty && c.persist {
		consider(c.dirtyAt.Add(time.Second))
	}
	if !c.activeLocked() {
		return at
	}
	block := c.nextAttempt
	if c.retryUntil.After(block) {
		block = c.retryUntil
	}
	later := func(t time.Time) time.Time {
		if block.After(t) {
			return block
		}
		return t
	}
	if c.sendableLocked() > 0 && c.flushEvery > 0 {
		consider(later(c.batchDue))
	}
	if c.sendableLocked() >= maxBatch || len(c.jobs) > 0 || c.rc.wanted || c.identifyWantsSessionLocked() ||
		(!c.settingsRead && c.sendableLocked() > 0) {
		consider(later(now))
	}
	return at
}

func (c *Client) sessionValidLocked(now time.Time) bool {
	return c.sess.token != "" && now.Before(c.sess.expires)
}

func (c *Client) persistIfDue() {
	c.mu.Lock()
	due := c.dirty && !c.now().Before(c.dirtyAt.Add(time.Second))
	c.unlock()
	if due {
		c.persistNow()
	}
}

func (c *Client) persistForce() { c.persistNow() }

// --- HTTP ----------------------------------------------------------------------

type answer struct {
	status     int
	body       []byte
	code       string // the body's "error"
	retryAfter time.Duration
	hasRetry   bool
	etag       string
	err        error // no answer: offline, a timeout or a cancelled run
}

func (a answer) json(v any) bool { return json.Unmarshal(a.body, v) == nil }

// request sends one request with the SDK's headers and reads at most 1 MB
// of the answer.
func (c *Client) request(method, path string, body []byte, headers map[string]string) answer {
	ctx, cancel := context.WithTimeout(context.WithValue(c.runCtx, sdkRequestKey{}, true), c.timeout)
	defer cancel()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.api+path, reader)
	if err != nil {
		return answer{err: err}
	}
	req.Header.Set("User-Agent", "ravensight-go/"+Version)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.isPlaytest {
		req.Header.Set("X-Ravensight-Playtest", c.playtest.Token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.httpc.Do(req)
	if err != nil {
		return answer{err: err}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	a := answer{status: resp.StatusCode, body: raw, etag: resp.Header.Get("ETag")}
	var probe struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(raw, &probe) == nil {
		a.code = probe.Error
	}
	if d, ok := parseRetryAfter(resp.Header.Get("Retry-After"), c.now()); ok {
		a.retryAfter, a.hasRetry = d, true
	}
	return a
}

// parseRetryAfter reads delta seconds or an HTTP date.
func parseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	if n, err := strconv.ParseInt(value, 10, 64); err == nil && n >= 0 {
		return time.Duration(n) * time.Second, true
	}
	if t, err := http.ParseTime(value); err == nil {
		d := t.Sub(now)
		if d < 0 {
			d = 0
		}
		return d, true
	}
	return 0, false
}

func gzipped(body []byte) []byte {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_, _ = w.Write(body)
	_ = w.Close()
	return buf.Bytes()
}

// signature is the delivery contract's header form:
// lowercase hex HMAC-SHA256(secret, "v1:" + timestamp + ":" + body).
func signature(secret string, timestamp int64, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte("v1:" + strconv.FormatInt(timestamp, 10) + ":"))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// --- Waiting -----------------------------------------------------------------

// failLocked records a failure that keeps the work: a Retry-After wait when
// the server gave one on a 429 or 503, else the backoff.
func (c *Client) failLocked(a answer) {
	if a.err == nil && (a.status == 429 || a.status == 503) && a.hasRetry {
		c.retryUntil = c.now().Add(a.retryAfter)
		c.transient = StatusRateLimited
	} else {
		c.scheduleBackoffLocked()
		if c.transient != StatusSessionFailed {
			c.transient = StatusOffline
		}
	}
	if a.code != "" {
		c.stats.LastError = a.code
	} else if a.err != nil {
		c.stats.LastError = "offline"
	}
	c.statusChangedLocked()
}

// scheduleBackoffLocked waits 10 seconds, doubling to 5 minutes, each wait
// multiplied by a random factor in [0.5, 1) so devices that failed together
// do not retry together.
func (c *Client) scheduleBackoffLocked() {
	if c.backoff == 0 {
		c.backoff = backoffBase
	} else {
		c.backoff *= 2
		if c.backoff > backoffMax {
			c.backoff = backoffMax
		}
	}
	factor := 0.5 + c.random()*0.5
	if factor >= 1 {
		factor = 0.999
	}
	c.nextAttempt = c.now().Add(time.Duration(float64(c.backoff) * factor))
}

func (c *Client) succeededLocked() {
	c.backoff = 0
	c.nextAttempt = time.Time{}
	c.transient = StatusOK
	c.statusChangedLocked()
}

// stopLocked ends sending for the run.
func (c *Client) stopLocked(s Status, msg string) {
	if c.stopped == "" {
		c.stopped = s
		c.stats.LastError = string(s)
		c.logLocked(slog.LevelWarn, msg)
	}
	c.failJobsLocked(string(s))
	c.statusChangedLocked()
}

const invalidKeyMessage = "Ravensight: ingest key refused (invalid_key), sending stopped for this run. Check your gt_live_ key."

// --- Settings ----------------------------------------------------------------

func (c *Client) readSettings() bool {
	a := c.request("GET", "/settings", nil, map[string]string{"X-API-Key": c.cfg.IngestKey})
	c.mu.Lock()
	defer c.unlock()
	switch {
	case a.err == nil && a.status == 200:
		var body struct {
			TrackingEnabled *bool           `json:"trackingEnabled"`
			ErrorCapture    json.RawMessage `json:"errorCapture"`
		}
		a.json(&body)
		c.settingsRead = true
		c.applyCaptureAnswerLocked(body.ErrorCapture)
		if body.TrackingEnabled != nil && !*body.TrackingEnabled {
			c.events = nil
			c.markDirtyLocked()
			c.stopLocked(StatusTrackingDisabled, "Ravensight: tracking is switched off for this game on the server, so nothing is sent this run.")
			return false
		}
		c.succeededLocked()
		return true
	case a.err == nil && (a.status == 401 || a.status == 403):
		c.stopForRefusalLocked(a)
		return false
	default:
		c.failLocked(a)
		return false
	}
}

// stopForRefusalLocked maps a refused settings or session request to the
// reason it ends the run with.
func (c *Client) stopForRefusalLocked(a answer) {
	switch a.code {
	case "tracking_disabled":
		c.stopLocked(StatusTrackingDisabled, "Ravensight: tracking is switched off for this game on the server, so nothing is sent this run.")
	case "playtest_job_closed":
		c.events = nil
		c.stopLocked(StatusPlaytestJobClosed, "Ravensight: the playtest job is closed, so nothing more is sent this run.")
	case "invalid_playtest_token":
		c.events = nil
		c.stopLocked(StatusSessionFailed, "Ravensight: the playtest token was refused, so nothing is sent this run.")
	default:
		c.stopLocked(StatusInvalidKey, invalidKeyMessage)
	}
}

// --- Session -----------------------------------------------------------------

func (c *Client) openSession() bool {
	c.mu.Lock()
	gen := c.generation
	body := map[string]any{
		"deviceId":    c.deviceID,
		"gameVersion": c.gameVersion,
		"platform":    c.platform,
		"sdk":         "go/" + Version,
	}
	join := c.joinToken
	if join != "" {
		body["joinToken"] = join
	}
	if c.distribution != "" {
		body["distribution"] = c.distribution
	}
	if len(c.device) > 0 {
		body["device"] = c.device
	}
	link := ""
	if join == "" && !c.isPlaytest && c.campaignLink != "" {
		link = c.campaignLink
		body["campaignLink"] = link
	}
	c.unlock()
	raw, _ := marshalJSON(body)
	a := c.request("POST", "/session", raw, map[string]string{"X-API-Key": c.cfg.IngestKey})

	c.mu.Lock()
	defer c.unlock()
	if a.err == nil && (a.status == 200 || a.status == 201) {
		var ans struct {
			Token     string          `json:"token"`
			ExpiresIn float64         `json:"expiresIn"`
			ExpiresAt json.RawMessage `json:"expiresAt"`
			Origin    json.RawMessage `json:"origin"`
			Signing   *struct {
				Algorithm  string  `json:"algorithm"`
				Secret     string  `json:"secret"`
				ServerTime float64 `json:"serverTime"`
			} `json:"signing"`
			ErrorCapture json.RawMessage `json:"errorCapture"`
			Identify     *struct {
				Salt string `json:"salt"`
			} `json:"identify"`
		}
		if !a.json(&ans) || ans.Token == "" {
			c.logLocked(slog.LevelInfo, "Ravensight: the session answer had no token.")
			c.transient = StatusSessionFailed
			c.failLocked(answer{status: a.status})
			return false
		}
		if !c.activeLocked() {
			return false // opted out or stopped while the request was out
		}
		if gen != c.generation {
			c.logLocked(slog.LevelInfo, "Ravensight: discarded a session opened for the previous device id.")
			return true
		}
		now := c.now()
		s := session{token: ans.Token, expires: sessionExpiry(now, ans.ExpiresIn, ans.ExpiresAt)}
		if isJSONObject(ans.Origin) {
			s.origin = append(json.RawMessage(nil), ans.Origin...)
			s.originSession = originSession(s.origin)
		}
		if ans.Signing != nil && ans.Signing.Algorithm == "HMAC-SHA256" && ans.Signing.Secret != "" {
			s.secret = ans.Signing.Secret
			if ans.Signing.ServerTime > 0 {
				s.offset = int64(ans.Signing.ServerTime) - now.Unix()
			}
		}
		c.sess = s
		c.stats.Sessions++
		if c.joinToken == join {
			c.joinToken = ""
		}
		if link != "" && c.campaignLink == link {
			c.campaignLink = ""
		}
		// Events tracked before this session belong to it.
		for _, e := range c.events {
			if e.origin == nil && s.origin != nil {
				e.origin, e.session = s.origin, s.originSession
				c.markDirtyLocked()
			}
		}
		c.applyCaptureAnswerLocked(ans.ErrorCapture)
		salt := ""
		if ans.Identify != nil {
			salt = ans.Identify.Salt
		}
		c.identifySessionAnswerLocked(salt)
		c.rc.wanted = true
		c.succeededLocked()
		return true
	}
	if a.err == nil && a.status == 401 && a.code == "invalid_join_token" {
		c.joinToken = ""
		c.stats.LastError = "invalid_join_token"
		c.logLocked(slog.LevelInfo, "Ravensight: the join token was refused; opening a plain session.")
		return true
	}
	if a.err == nil && (a.status == 401 || a.status == 403) {
		c.stopForRefusalLocked(a)
		return false
	}
	if a.err == nil && a.status >= 400 && a.status < 500 && a.status != 429 {
		c.transient = StatusSessionFailed
		c.warnOnceLocked("session_"+a.code, "Ravensight: the session was refused ("+a.code+"); retrying with a backoff.")
	}
	c.failLocked(a)
	return false
}

func sessionExpiry(now time.Time, expiresIn float64, expiresAt json.RawMessage) time.Time {
	const margin = 60 * time.Second
	if expiresIn > 0 {
		return now.Add(time.Duration(expiresIn*float64(time.Second)) - margin)
	}
	var at string
	if json.Unmarshal(expiresAt, &at) == nil {
		if t, err := time.Parse(time.RFC3339, at); err == nil {
			return t.Add(-margin)
		}
	}
	var ms float64
	if json.Unmarshal(expiresAt, &ms) == nil && ms > 0 {
		return time.UnixMilli(int64(ms)).Add(-margin)
	}
	return now.Add(24*time.Hour - margin)
}

// --- Batches -----------------------------------------------------------------

type wireEvent struct {
	Event     string          `json:"event"`
	Timestamp int64           `json:"timestamp"`
	Data      json.RawMessage `json:"data"`
	Origin    json.RawMessage `json:"origin,omitempty"`
}

// pickBatchLocked takes up to limit events that are not in flight:
// gameplay first, in order, then captured errors.
func (c *Client) pickBatchLocked(limit int) []*qevent {
	var batch []*qevent
	for _, capture := range []bool{false, true} {
		for _, e := range c.events {
			if len(batch) >= limit {
				return batch
			}
			if e.capture == capture && !e.inFlight {
				batch = append(batch, e)
			}
		}
	}
	return batch
}

func (c *Client) sendBatch() bool {
	c.mu.Lock()
	batch := c.pickBatchLocked(c.batchLimit)
	if len(batch) == 0 {
		c.unlock()
		return true
	}
	wire := make([]wireEvent, len(batch))
	for i, e := range batch {
		e.inFlight = true
		wire[i] = wireEvent{Event: e.name, Timestamp: e.timestamp, Data: e.data}
		if e.origin != nil && e.session != c.sess.originSession {
			wire[i].Origin = e.origin
		}
	}
	token, secret, offset := c.sess.token, c.sess.secret, c.sess.offset
	useGzip := !c.noGzip
	c.unlock()

	body, _ := marshalJSON(map[string]any{"events": wire})
	headers := map[string]string{"X-Session-Token": token}
	if secret != "" {
		ts := c.now().Unix() + offset
		headers["X-Ravensight-Timestamp"] = strconv.FormatInt(ts, 10)
		headers["X-Ravensight-Signature"] = "v1=" + signature(secret, ts, body)
	}
	sent := body
	compressed := false
	if useGzip && len(body) >= gzipFrom {
		sent = gzipped(body)
		compressed = true
		headers["Content-Encoding"] = "gzip"
	}
	a := c.request("POST", "/track/batch", sent, headers)

	c.mu.Lock()
	for _, e := range batch {
		e.inFlight = false
	}
	ok := c.batchAnswerLocked(batch, a, compressed)
	c.statusChangedLocked()
	c.unlock()
	c.persistNow() // a removal is saved before anything else happens
	return ok
}

// batchAnswerLocked applies the delivery contract's answer table.
func (c *Client) batchAnswerLocked(batch []*qevent, a answer, compressed bool) bool {
	if a.err != nil {
		if errors.Is(c.runCtx.Err(), context.Canceled) {
			return false
		}
		c.failLocked(a)
		return false
	}
	switch {
	case a.status >= 200 && a.status < 300:
		c.removeLocked(batch)
		c.stats.Sent += len(batch)
		c.stats.Batches++
		c.batchLimit = maxBatch
		c.reauthed = false
		if c.sendableLocked() > 0 {
			c.batchDue = c.now().Add(c.flushEvery)
		}
		c.succeededLocked()
		return true
	case a.status == 400:
		var body struct {
			Index *int `json:"index"`
		}
		a.json(&body)
		switch {
		case body.Index != nil && *body.Index >= 0 && *body.Index < len(batch):
			c.removeLocked(batch[*body.Index : *body.Index+1])
			c.dropLocked(1, a.code, "Ravensight: the server refused one event ("+a.code+"); it was dropped.")
		case a.code == "batch_too_large":
			return c.splitLocked(batch, a.code)
		default:
			c.removeLocked(batch)
			c.dropLocked(len(batch), a.code, "Ravensight: the server refused a batch ("+a.code+"); its events were dropped.")
		}
		return true
	case a.status == 413:
		return c.splitLocked(batch, "payload_too_large")
	case a.status == 415 && compressed:
		c.noGzip = true
		return true
	case a.status == 415:
		c.removeLocked(batch)
		c.dropLocked(len(batch), "unsupported_media_type", "Ravensight: the server refused a batch as unsupported; its events were dropped.")
		return true
	case a.status == 401:
		c.sess = session{}
		if !c.reauthed {
			c.reauthed = true // open a new session once and resend the same ids
			return true
		}
		c.reauthed = false
		c.failLocked(a)
		return false
	case a.status == 403:
		// Keep the events in the file for the next launch.
		c.markDirtyLocked()
		switch a.code {
		case "tracking_disabled":
			c.stopLocked(StatusTrackingDisabled, "Ravensight: tracking is switched off for this game on the server, so nothing more is sent this run.")
		case "playtest_job_closed":
			c.events = nil
			c.stopLocked(StatusPlaytestJobClosed, "Ravensight: the playtest job is closed, so nothing more is sent this run.")
		default:
			c.stopLocked(StatusInvalidKey, invalidKeyMessage)
		}
		return false
	default:
		c.failLocked(a)
		return false
	}
}

// splitLocked halves the batch size; a single event still too large is
// dropped.
func (c *Client) splitLocked(batch []*qevent, code string) bool {
	if len(batch) <= 1 {
		c.removeLocked(batch)
		c.dropLocked(len(batch), code, "Ravensight: the server refused an event as too large; it was dropped.")
		return true
	}
	c.batchLimit = len(batch) / 2
	return true
}

func (c *Client) dropLocked(n int, code, msg string) {
	c.stats.Dropped += n
	if code != "" {
		c.stats.LastError = code
	}
	c.logLocked(slog.LevelInfo, msg)
}

// removeLocked removes the given events if they are still queued. A late
// answer after a reset or an opt-out finds nothing to remove.
func (c *Client) removeLocked(batch []*qevent) {
	if len(batch) == 0 {
		return
	}
	gone := make(map[uint64]bool, len(batch))
	for _, e := range batch {
		gone[e.seq] = true
	}
	kept := c.events[:0]
	for _, e := range c.events {
		if !gone[e.seq] {
			kept = append(kept, e)
		}
	}
	for i := len(kept); i < len(c.events); i++ {
		c.events[i] = nil
	}
	c.events = kept
	c.markDirtyLocked()
}
