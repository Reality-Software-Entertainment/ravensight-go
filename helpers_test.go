package ravensight

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// recorded is one request the fake API received, with its body decoded.
type recorded struct {
	Method string
	Path   string
	Query  url.Values
	Header http.Header
	Body   []byte
	Gzip   bool
	Raw    []byte // as sent, before gzip was undone
}

func (r recorded) json(t *testing.T) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(r.Body, &m); err != nil {
		t.Fatalf("%s %s: body is not JSON: %v: %s", r.Method, r.Path, err, r.Body)
	}
	return m
}

// events returns a batch request's events.
func (r recorded) events(t *testing.T) []map[string]any {
	t.Helper()
	var body struct {
		Events []map[string]any `json:"events"`
	}
	if err := json.Unmarshal(r.Body, &body); err != nil {
		t.Fatalf("batch body: %v", err)
	}
	return body.Events
}

type handlerFunc func(w http.ResponseWriter, r recorded, n int)

// fakeAPI is a scripted stand-in for the Ravensight API. Nothing in the
// tests touches a real network.
type fakeAPI struct {
	t        *testing.T
	srv      *httptest.Server
	mu       sync.Mutex
	reqs     []recorded
	counts   map[string]int
	handlers map[string]handlerFunc
	salt     string
	capture  string // the errorCapture object every answer carries, as JSON
}

const testSalt = "a3f1c9e07b2d48c6915e2f7a0d4b8c3e6f1a9d2c7e5b0f4a8d3c6e9b1f2a7d40"

func newFakeAPI(t *testing.T) *fakeAPI {
	f := &fakeAPI{t: t, counts: map[string]int{}, handlers: map[string]handlerFunc{}, salt: testSalt,
		capture: `{"enabled":true,"trail":false}`}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeAPI) url() string { return f.srv.URL }

func (f *fakeAPI) serve(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	body := raw
	gz := r.Header.Get("Content-Encoding") == "gzip"
	if gz {
		zr, err := gzip.NewReader(bytes.NewReader(raw))
		if err == nil {
			body, _ = io.ReadAll(zr)
		}
	}
	path := strings.TrimPrefix(r.URL.Path, "/api/v1")
	rec := recorded{Method: r.Method, Path: path, Query: r.URL.Query(), Header: r.Header.Clone(), Body: body, Gzip: gz, Raw: raw}
	key := r.Method + " " + path
	f.mu.Lock()
	f.reqs = append(f.reqs, rec)
	f.counts[key]++
	n := f.counts[key]
	h := f.handlers[key]
	f.mu.Unlock()
	if h != nil {
		h(w, rec, n)
		return
	}
	f.defaultAnswer(w, rec, n)
}

func (f *fakeAPI) defaultAnswer(w http.ResponseWriter, r recorded, n int) {
	switch r.Method + " " + r.Path {
	case "GET /settings":
		writeJSON(w, 200, fmt.Sprintf(`{"trackingEnabled":true,"errorCapture":%s}`, f.captureJSON()))
	case "POST /session":
		writeJSON(w, 201, f.sessionAnswer(n, true))
	case "POST /track/batch":
		var body struct {
			Events []any `json:"events"`
		}
		_ = json.Unmarshal(r.Body, &body)
		writeJSON(w, 202, fmt.Sprintf(`{"accepted":%d,"duplicates":0}`, len(body.Events)))
	case "POST /identify":
		writeJSON(w, 200, `{"playerId":"px_x","linked":true,"verified":false,"repeat":false}`)
	case "GET /ingest/config":
		writeJSON(w, 200, `{"values":{},"experiments":[],"etag":"\"e0\""}`)
	case "POST /feedback":
		writeJSON(w, 201, `{"id":"f1"}`)
	default:
		writeJSON(w, 404, `{"error":"not_found"}`)
	}
}

func (f *fakeAPI) captureJSON() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.capture
}

func (f *fakeAPI) setCapture(js string) {
	f.mu.Lock()
	f.capture = js
	f.mu.Unlock()
}

// sessionAnswer is session n's answer: token tok-n and origin ses_n.
func (f *fakeAPI) sessionAnswer(n int, withSalt bool) string {
	f.mu.Lock()
	salt := f.salt
	f.mu.Unlock()
	identify := ""
	if withSalt && salt != "" {
		identify = fmt.Sprintf(`,"identify":{"salt":%q}`, salt)
	}
	return fmt.Sprintf(`{"token":"tok-%d","expiresIn":3600,"origin":{"session":"ses_%d","build":"1.0.0","platform":"test","stamp":"o1.a%d.b"},"errorCapture":%s%s}`,
		n, n, n, f.captureJSON(), identify)
}

func (f *fakeAPI) on(key string, h handlerFunc) {
	f.mu.Lock()
	f.handlers[key] = h
	f.mu.Unlock()
}

func (f *fakeAPI) requests() []recorded {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recorded(nil), f.reqs...)
}

func (f *fakeAPI) only(key string) []recorded {
	var out []recorded
	for _, r := range f.requests() {
		if r.Method+" "+r.Path == key {
			out = append(out, r)
		}
	}
	return out
}

func (f *fakeAPI) count(key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.counts[key]
}

// batchEvents returns every event of every batch request, in order.
func (f *fakeAPI) batchEvents(t *testing.T) []map[string]any {
	var out []map[string]any
	for _, r := range f.only("POST /track/batch") {
		out = append(out, r.events(t)...)
	}
	return out
}

func eventNames(events []map[string]any) []string {
	var out []string
	for _, e := range events {
		out = append(out, fmt.Sprint(e["event"]))
	}
	return out
}

func writeJSON(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}

// logSink keeps the SDK's log lines for the test.
type logSink struct {
	mu    sync.Mutex
	lines []string
}

func (s *logSink) Enabled(context.Context, slog.Level) bool { return true }
func (s *logSink) Handle(_ context.Context, r slog.Record) error {
	s.mu.Lock()
	s.lines = append(s.lines, r.Level.String()+" "+r.Message)
	s.mu.Unlock()
	return nil
}
func (s *logSink) WithAttrs([]slog.Attr) slog.Handler { return s }
func (s *logSink) WithGroup(string) slog.Handler      { return s }

func (s *logSink) all() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.lines...)
}

func (s *logSink) countContaining(sub string) int {
	n := 0
	for _, l := range s.all() {
		if strings.Contains(l, sub) {
			n++
		}
	}
	return n
}

// fakeClock is a settable clock for the retry tests.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Now()} }

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

type testOpts struct {
	cfg   func(*Config)
	args  []string
	clock *fakeClock
	dir   string
}

// newTestClient makes a client against the fake API with its own storage
// directory and manual flushing, and closes it at the end of the test.
func newTestClient(t *testing.T, api *fakeAPI, o testOpts) (*Client, *logSink) {
	t.Helper()
	sink := &logSink{}
	dir := o.dir
	if dir == "" {
		dir = t.TempDir()
	}
	cfg := Config{
		IngestKey:     "gt_live_testkey123",
		StorageDir:    dir,
		FlushInterval: -1,
		Logger:        slog.New(sink),
		Platform:      "test",
	}
	if api != nil {
		cfg.APIURL = api.url()
	}
	if o.cfg != nil {
		o.cfg(&cfg)
	}
	now := time.Now
	if o.clock != nil {
		now = o.clock.now
	}
	c := newClient(cfg, o.args, now, func() float64 { return 0.5 })
	c.crashGrace = time.Hour // a recorded panic counts as a crash unless a test says otherwise
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = c.Shutdown(ctx)
	})
	return c, sink
}

func flush(t *testing.T, c *Client) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.Flush(ctx)
}

func mustFlush(t *testing.T, c *Client) {
	t.Helper()
	if err := flush(t, c); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

func closeNow(t *testing.T, c *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func files(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out
}

func hasFile(t *testing.T, dir, name string) bool {
	_, err := os.Stat(filepath.Join(dir, name))
	return err == nil
}

func readQueueFile(t *testing.T, dir, key string) queueFile {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, queueFileName(key)))
	if err != nil {
		t.Fatalf("queue file: %v", err)
	}
	var q queueFile
	if err := json.Unmarshal(raw, &q); err != nil {
		t.Fatal(err)
	}
	return q
}

func dataOf(t *testing.T, e map[string]any) map[string]any {
	t.Helper()
	d, ok := e["data"].(map[string]any)
	if !ok {
		t.Fatalf("event has no data object: %v", e)
	}
	return d
}
