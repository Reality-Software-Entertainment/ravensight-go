package ravensight

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func capturesOf(t *testing.T, api *fakeAPI) []map[string]any {
	var out []map[string]any
	for _, e := range api.batchEvents(t) {
		d := dataOf(t, e)
		if d["rs_kind"] == "error" && d["rs_source"] != nil && d["rs_source"] != "manual" {
			out = append(out, d)
		}
	}
	return out
}

func TestALogErrorBecomesOneScrubbedWarningEvent(t *testing.T) {
	api := newFakeAPI(t)
	c, _ := newTestClient(t, api, testOpts{})
	mustFlush(t, c) // the switch is known
	logger := slog.New(c.LogHandler(slog.NewTextHandler(discardWriter{}, nil)))
	logger.Error("save failed for jane@example.com", "err", errors.New("disk full at /Users/jane/save.dat"))
	logger.Info("an info line is not an error")
	mustFlush(t, c)
	caps := capturesOf(t, api)
	if len(caps) != 1 {
		t.Fatalf("%d captures", len(caps))
	}
	d := caps[0]
	if d["rs_source"] != "log" || d["rs_severity"] != "warning" || d["rs_error_type"] != "*errors.errorString" ||
		d["rs_message"] != "save failed for [email]: disk full at ~/save.dat" {
		t.Fatalf("capture %v", d)
	}
	if _, has := d["rs_trail"]; has {
		t.Fatal("a trail while the switch is off")
	}
	if _, has := d["rs_fp"]; has {
		t.Fatal("the SDK sent a fingerprint")
	}
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

func TestTheTrailGoesOnlyWhileTheGameTurnsItOn(t *testing.T) {
	api := newFakeAPI(t)
	api.setCapture(`{"enabled":true,"trail":true}`)
	dir := t.TempDir()
	api.on("POST /track/batch", func(w http.ResponseWriter, r recorded, n int) {
		if n == 2 {
			writeJSON(w, 503, `{}`) // keep the capture queued so the file is written
			return
		}
		api.defaultAnswer(w, r, n)
	})
	c, _ := newTestClient(t, api, testOpts{dir: dir})
	mustFlush(t, c)
	c.Log(slog.LevelInfo, "loaded level 3 for bob@example.com")
	c.Track("door_opened", nil)
	c.Log(slog.LevelError, "boss script failed")
	_ = flush(t, c)
	c.persistNow()
	q := readQueueFile(t, dir, "gt_live_testkey123")
	raw := fmt.Sprint(q)
	if strings.Contains(raw, "rs_trail") {
		t.Fatal("rs_trail was written to the queue file")
	}
	mustFlush(t, c)
	caps := capturesOf(t, api)
	trail, _ := caps[len(caps)-1]["rs_trail"].(string)
	lines := strings.Split(trail, "\n")
	if len(lines) != 2 || !strings.Contains(lines[0], " info loaded level 3 for [email]") || !strings.Contains(lines[1], " event door_opened") || !strings.HasPrefix(lines[0], "+") {
		t.Fatalf("trail %q", trail)
	}

	off := newFakeAPI(t)
	c2, _ := newTestClient(t, off, testOpts{})
	mustFlush(t, c2)
	c2.Log(slog.LevelInfo, "x")
	c2.Log(slog.LevelError, "y")
	mustFlush(t, c2)
	if _, has := capturesOf(t, off)[0]["rs_trail"]; has {
		t.Fatal("a trail while the switch is off")
	}
}

func TestTheTrailIsBounded(t *testing.T) {
	c := &Client{}
	c.cap.trailOn = true
	for i := 0; i < 50; i++ {
		c.pushTrailLocked(trailLine(time.Duration(i)*time.Millisecond, "info", strings.Repeat("w", 200)))
	}
	if len(c.cap.trail) != 20 || utf16Len(c.cap.trail[0]) != 120 {
		t.Fatalf("%d lines, first %d", len(c.cap.trail), utf16Len(c.cap.trail[0]))
	}
	if text := c.trailTextLocked(); utf16Len(text) > 1500 || !strings.HasSuffix(text, c.cap.trail[19]) {
		t.Fatalf("trail of %d characters", utf16Len(text))
	}
}

func TestTheGameSwitchOffSendsNoCapturesButManualErrorsPass(t *testing.T) {
	api := newFakeAPI(t)
	api.setCapture(`{"enabled":false,"trail":true}`)
	c, _ := newTestClient(t, api, testOpts{})
	mustFlush(t, c)
	if c.IsErrorCaptureActive() {
		t.Fatal("capture active with the switch off")
	}
	c.Log(slog.LevelError, "x")
	c.TrackError(ErrorReport{Severity: "error", Message: "manual"})
	mustFlush(t, c)
	if len(capturesOf(t, api)) != 0 {
		t.Fatal("captured with the switch off")
	}
	found := false
	for _, e := range api.batchEvents(t) {
		if dataOf(t, e)["rs_message"] == "manual" {
			found = true
		}
	}
	if !found {
		t.Fatal("a manual error was held back")
	}
}

func TestBootErrorsAreHeldThenSentOrDiscarded(t *testing.T) {
	for _, on := range []bool{true, false} {
		api := newFakeAPI(t)
		api.setCapture(fmt.Sprintf(`{"enabled":%v}`, on))
		release := make(chan struct{})
		api.on("GET /settings", func(w http.ResponseWriter, r recorded, n int) {
			<-release
			api.defaultAnswer(w, r, n)
		})
		c, _ := newTestClient(t, api, testOpts{})
		waitFor(t, "the settings request", func() bool { return api.count("GET /settings") == 1 })
		if !c.IsErrorCaptureActive() {
			t.Fatal("capture is not live before the answer")
		}
		for i := 0; i < 8; i++ {
			c.Log(slog.LevelError, fmt.Sprintf("boot problem %c", 'a'+i))
		}
		c.mu.Lock()
		held, queued := len(c.cap.hold), len(c.events)
		c.mu.Unlock()
		if held != 5 || queued != 1 {
			t.Fatalf("held %d queued %d", held, queued)
		}
		close(release)
		mustFlush(t, c)
		want := 0
		if on {
			want = 5
		}
		if got := len(capturesOf(t, api)); got != want {
			t.Fatalf("on=%v: %d captures sent", on, got)
		}
	}
}

func TestAnAnswerWithoutTheSwitchMeansOn(t *testing.T) {
	api := newFakeAPI(t)
	api.on("GET /settings", func(w http.ResponseWriter, r recorded, n int) { writeJSON(w, 200, `{"trackingEnabled":true}`) })
	api.on("POST /session", func(w http.ResponseWriter, r recorded, n int) { writeJSON(w, 201, `{"token":"t","expiresIn":3600}`) })
	c, _ := newTestClient(t, api, testOpts{})
	mustFlush(t, c)
	c.Log(slog.LevelError, "x")
	mustFlush(t, c)
	if len(capturesOf(t, api)) != 1 {
		t.Fatal("no capture")
	}
}

func TestNothingIsCapturedWithoutAKeyConsentOrWhenTurnedOff(t *testing.T) {
	inert := New(Config{Optional: true})
	inert.Log(slog.LevelError, "x")
	if inert.IsErrorCaptureActive() {
		t.Fatal("inert capture")
	}
	api := newFakeAPI(t)
	c, _ := newTestClient(t, api, testOpts{cfg: func(cfg *Config) { cfg.Consent = ConsentOptIn }})
	c.Log(slog.LevelError, "x")
	if c.IsErrorCaptureActive() || c.Stats().Queued != 0 {
		t.Fatal("capture before consent")
	}
	c2, _ := newTestClient(t, api, testOpts{cfg: func(cfg *Config) { cfg.NoErrorCapture = true }})
	if c2.IsErrorCaptureActive() {
		t.Fatal("capture with NoErrorCapture")
	}
	c3, _ := newTestClient(t, newFakeAPI(t), testOpts{})
	c3.SetErrorCapture(false)
	c3.Log(slog.LevelError, "x")
	c3.SetErrorCapture(true)
	if !c3.IsErrorCaptureActive() {
		t.Fatal("SetErrorCapture(true)")
	}
	c3.mu.Lock()
	n := len(c3.cap.hold) + len(c3.events)
	c3.mu.Unlock()
	if n != 1 { // game_started only
		t.Fatalf("%d queued or held", n)
	}
}

func TestTheSDKsOwnLogLinesAreNeverCaptured(t *testing.T) {
	api := newFakeAPI(t)
	sink := &logSink{}
	var c *Client
	// The game routes everything, the SDK's own logger included, through the capture handler.
	handler := (&Client{}).LogHandler(sink)
	c, _ = newTestClient(t, api, testOpts{cfg: func(cfg *Config) { cfg.Logger = slog.New(handler) }})
	handler.(*logHandler).c = c
	api.on("POST /track/batch", func(w http.ResponseWriter, r recorded, n int) {
		writeJSON(w, 400, `{"error":"bad"}`) // makes the SDK log a line
	})
	c.Track("x", nil)
	_ = flush(t, c)
	c.Log(slog.LevelError, "Ravensight: something")
	c.mu.Lock()
	n := 0
	for _, e := range c.events {
		if e.capture {
			n++
		}
	}
	n += len(c.cap.hold)
	c.mu.Unlock()
	if n != 0 {
		t.Fatalf("%d of the SDK's own lines captured", n)
	}
}

func TestDeviceCapsBoundAnErrorLoop(t *testing.T) {
	clock := newFakeClock()
	api := newFakeAPI(t)
	c, _ := newTestClient(t, api, testOpts{clock: clock})
	mustFlush(t, c)
	count := func() int {
		c.mu.Lock()
		defer c.mu.Unlock()
		n := 0
		for _, e := range c.events {
			if e.capture {
				n++
			}
		}
		return n
	}
	for i := 0; i < 100; i++ {
		c.Log(slog.LevelError, fmt.Sprintf("enemy %d fell through the floor", i))
	}
	if got := count(); got != 3 {
		t.Fatalf("one dedupe key gave %d captures", got)
	}
	for i := 0; i < 100; i++ {
		c.Log(slog.LevelError, fmt.Sprintf("distinct problem %c", 'A'+i%26)+strings.Repeat("!", i/26))
	}
	if got := count(); got != 6 {
		t.Fatalf("the bucket let %d through in a minute", got)
	}
	for m := 0; m < 10; m++ {
		clock.advance(time.Minute)
		for i := 0; i < 10; i++ {
			c.Log(slog.LevelError, fmt.Sprintf("minute %c problem %c", 'a'+m, 'a'+i))
		}
	}
	if got := count(); got != 30 {
		t.Fatalf("the run cap let %d through", got)
	}
}

func TestCapturesNeverJumpOrEvictGameplayEvents(t *testing.T) {
	api := newFakeAPI(t)
	c, _ := newTestClient(t, api, testOpts{cfg: func(cfg *Config) { cfg.MaxQueueSize = 5 }})
	mustFlush(t, c) // game_started goes and the switch is known
	api.on("POST /track/batch", func(w http.ResponseWriter, r recorded, n int) { writeJSON(w, 503, `{}`) })
	c.Log(slog.LevelError, "first capture")
	c.Track("g1", nil)
	c.Log(slog.LevelError, "second capture")
	c.Track("g2", nil)
	c.Track("g3", nil)
	c.Track("g4", nil)                      // at the cap: evicts the oldest capture
	c.Log(slog.LevelError, "third capture") // evicts the other capture
	c.mu.Lock()
	var names []string
	for _, e := range c.pickBatchLocked(50) {
		names = append(names, e.name+fmt.Sprint(e.capture))
	}
	c.mu.Unlock()
	if strings.Join(names, ",") != "g1false,g2false,g3false,g4false,errortrue" {
		t.Fatalf("batch order %v", names)
	}
	c.Track("g5", nil) // no capture left but the newest: it goes
	c.Log(slog.LevelError, "fourth capture")
	c.mu.Lock()
	gameplay := 0
	for _, e := range c.events {
		if !e.capture {
			gameplay++
		}
	}
	c.mu.Unlock()
	if gameplay != 5 {
		t.Fatalf("a capture evicted gameplay: %d left", gameplay)
	}
}

func crashes(dir string) []string {
	m, _ := filepath.Glob(filepath.Join(dir, crashPrefix+"*"))
	return m
}

func panicWith(c *Client, v any) (recovered any) {
	defer func() { recovered = recover() }()
	defer c.Recover()
	panic(v)
}

func TestAPanicIsRecordedAndSentOnceOnTheNextLaunch(t *testing.T) {
	api := newFakeAPI(t)
	dir := t.TempDir()
	c, _ := newTestClient(t, api, testOpts{dir: dir})
	if got := panicWith(c, "boss loop broke for jane@example.com"); got != "boss loop broke for jane@example.com" {
		t.Fatalf("Recover did not panic again with the same value: %v", got)
	}
	if len(crashes(dir)) != 1 {
		t.Fatalf("crash records %v", files(t, dir))
	}
	closeNow(t, c)
	c2, _ := newTestClient(t, api, testOpts{dir: dir})
	mustFlush(t, c2)
	caps := capturesOf(t, api)
	if len(caps) != 1 {
		t.Fatalf("%d captures", len(caps))
	}
	d := caps[0]
	if d["rs_source"] != "crash" || d["rs_severity"] != "critical" || d["rs_message"] != "boss loop broke for [email]" || d["rs_error_type"] != "string" {
		t.Fatalf("crash %v", d)
	}
	stack, _ := d["rs_stack"].(string)
	if !strings.Contains(stack, "panicWith") || strings.Contains(stack, "runtime/debug.Stack") || strings.Contains(stack, "+0x") {
		t.Fatalf("stack %q", stack)
	}
	if len(crashes(dir)) != 0 {
		t.Fatal("the record was not deleted once queued")
	}
	closeNow(t, c2)
	c3, _ := newTestClient(t, api, testOpts{dir: dir})
	mustFlush(t, c3)
	if len(capturesOf(t, api)) != 1 {
		t.Fatal("a crash was sent twice")
	}
}

func TestRecoverWithoutAPanicDoesNothingAndANilClientStillPanics(t *testing.T) {
	var c *Client
	func() { defer c.Recover() }()
	if got := panicWith(nil, errors.New("e")); got == nil {
		t.Fatal("a nil client swallowed the panic")
	}
	inert := New(Config{Optional: true})
	if got := panicWith(inert, 7); got != 7 {
		t.Fatal("an inert client changed the panic")
	}
}

func TestSavedCrashesOldResetAndSwitchOff(t *testing.T) {
	// Older than 30 days: deleted unsent.
	api := newFakeAPI(t)
	dir := t.TempDir()
	old := time.Now().Add(-31 * 24 * time.Hour).UnixMilli()
	os.WriteFile(filepath.Join(dir, fmt.Sprintf("%s%d-aaa.json", crashPrefix, old)), []byte(fmt.Sprintf(`{"timestamp":%d,"message":"old"}`, old)), 0o600)
	c, _ := newTestClient(t, api, testOpts{dir: dir})
	mustFlush(t, c)
	if len(capturesOf(t, api)) != 0 || len(crashes(dir)) != 0 {
		t.Fatal("an old crash was sent or kept")
	}
	// A reset deletes a saved crash unsent.
	panicWith(c, "x")
	if len(crashes(dir)) != 1 {
		t.Fatal("no record")
	}
	c.ResetDeviceID()
	if len(crashes(dir)) != 0 {
		t.Fatal("the reset kept the record")
	}
	// The game's switch off: discarded unsent.
	panicWith(c, "y")
	closeNow(t, c)
	off := newFakeAPI(t)
	off.setCapture(`{"enabled":false}`)
	c2, _ := newTestClient(t, off, testOpts{dir: dir})
	mustFlush(t, c2)
	if len(capturesOf(t, off)) != 0 || len(crashes(dir)) != 0 {
		t.Fatal("a crash was sent or kept with the switch off")
	}
	// At most 5 records are kept.
	c3, _ := newTestClient(t, newFakeAPI(t), testOpts{dir: t.TempDir()})
	for i := 0; i < 8; i++ {
		panicWith(c3, i)
	}
	if n := len(crashes(c3.st.dir)); n != 5 {
		t.Fatalf("%d records kept", n)
	}
}

func TestPanicStackKeepsTheGamesFrames(t *testing.T) {
	stack := "goroutine 1 [running]:\nruntime/debug.Stack()\n\t/go/src/runtime/debug/stack.go:26 +0x5e\ngithub.com/x/ravensight-go.(*Client).Recover(0x1)\n\t/src/capture.go:1 +0x2\npanic({0x1, 0x2})\n\t/go/src/runtime/panic.go:787 +0x124\nmain.level(...)\n\t/home/sam/game/main.go:12 +0x10\nmain.main()\n\t/home/sam/game/main.go:5 +0x20\n"
	got := panicStack(stack)
	if got != "main.level(...)\n\t/home/sam/game/main.go:12\nmain.main()\n\t/home/sam/game/main.go:5" {
		t.Fatalf("%q", got)
	}
}

func TestWrapTransportCapturesTheGamesFailedRequests(t *testing.T) {
	api := newFakeAPI(t)
	game := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/items/"):
			w.WriteHeader(500)
		case r.URL.Path == "/missing":
			w.WriteHeader(404)
		default:
			w.WriteHeader(200)
		}
	}))
	defer game.Close()
	c, _ := newTestClient(t, api, testOpts{})
	mustFlush(t, c)
	hc := &http.Client{Transport: c.WrapTransport(nil)}
	for _, p := range []string{"/items/12345?token=secret#x", "/missing", "/ok"} {
		resp, err := hc.Get(game.URL + p)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	deadURL := dead.URL
	dead.Close()
	if _, err := hc.Get(deadURL + "/v1/kickoff/3f2a9c1e-7b4d-4e8a-9c3f-1a2b3c4d5e6f"); err == nil {
		t.Fatal("expected a network error")
	}
	// The SDK's own API is never captured, even through a wrapped client.
	if resp, err := hc.Get(api.url() + "/api/v1/nothing"); err == nil {
		resp.Body.Close()
	}
	mustFlush(t, c)
	caps := capturesOf(t, api)
	host := strings.TrimPrefix(game.URL, "http://")
	deadHost := strings.TrimPrefix(deadURL, "http://")
	want := []string{
		"GET " + host + "/items/:id 500|error",
		"GET " + host + "/missing 404|warning",
		"GET " + deadHost + "/v1/kickoff/:id network|error",
	}
	if len(caps) != len(want) {
		t.Fatalf("%d captures: %v", len(caps), caps)
	}
	for i, w := range want {
		if got := fmt.Sprint(caps[i]["rs_message"], "|", caps[i]["rs_severity"]); got != scrub(w) && got != w {
			t.Errorf("capture %d: %q want %q", i, got, w)
		}
		if caps[i]["rs_source"] != "request" {
			t.Errorf("source %v", caps[i]["rs_source"])
		}
	}
}
