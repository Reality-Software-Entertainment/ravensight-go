package ravensight

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestNoKeyIsInert(t *testing.T) {
	dir := t.TempDir()
	sink := &logSink{}
	c := New(Config{StorageDir: dir, Logger: slog.New(sink)})
	if c.Track("x", nil) {
		t.Fatal("an inert client queued an event")
	}
	if c.Status() != StatusNoKey || c.IsCollecting() || c.DeviceID() != "" {
		t.Fatalf("status %q collecting %v", c.Status(), c.IsCollecting())
	}
	if err := c.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := c.Identify("abc123"); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if got := files(t, dir); len(got) != 0 {
		t.Fatalf("an inert client wrote %v", got)
	}
	if sink.countContaining("no ingest key") != 1 {
		t.Fatalf("want one warning, got %v", sink.all())
	}
	quiet := &logSink{}
	New(Config{Optional: true, Logger: slog.New(quiet)}).Close()
	if len(quiet.all()) != 0 {
		t.Fatalf("Optional did not silence: %v", quiet.all())
	}
}

func TestNilClientIsSafe(t *testing.T) {
	var c *Client
	if c.Track("x", nil) || c.IsCollecting() || c.TrackError(ErrorReport{Message: "m"}) {
		t.Fatal("nil client collected")
	}
	c.SetEnabled(true)
	c.ResetIdentity()
	c.FetchConfig()
	_ = c.ConfigBool("k", true)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if Default() != nil && Default().cfg.IngestKey == "" {
		t.Fatal("unexpected default")
	}
}

func TestHappyPathSendsSettingsSessionThenBatch(t *testing.T) {
	api := newFakeAPI(t)
	c, _ := newTestClient(t, api, testOpts{cfg: func(cfg *Config) {
		cfg.GameVersion = "2.3.4"
		cfg.Distribution = "itch-demo"
		cfg.Device = Device{Model: "Desk 1", OSVersion: " 14.2 ", GPU: "Card\x01 X"}
	}})
	if !c.Track("level_completed", map[string]any{"level": 3, "seconds": 41.7, "boss": "crab"}) {
		t.Fatal("track refused")
	}
	mustFlush(t, c)
	reqs := api.requests()
	var order []string
	for _, r := range reqs {
		order = append(order, r.Method+" "+r.Path)
	}
	want := []string{"GET /settings", "POST /session", "GET /ingest/config", "POST /track/batch"}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Fatalf("requests %v, want %v", order, want)
	}
	if reqs[0].Header.Get("X-API-Key") != "gt_live_testkey123" {
		t.Fatal("settings without the key")
	}
	session := reqs[1].json(t)
	if !strings.HasPrefix(session["deviceId"].(string), "dev_") || session["sdk"] != "go/"+Version ||
		session["gameVersion"] != "2.3.4" || session["platform"] != "test" || session["distribution"] != "itch-demo" {
		t.Fatalf("session body %v", session)
	}
	device := session["device"].(map[string]any)
	if device["model"] != "Desk 1" || device["osVersion"] != "14.2" || device["gpu"] != "Card X" {
		t.Fatalf("device %v", device)
	}
	batch := reqs[3]
	if batch.Header.Get("X-Session-Token") != "tok-1" {
		t.Fatalf("batch token %q", batch.Header.Get("X-Session-Token"))
	}
	events := batch.events(t)
	if got := eventNames(events); strings.Join(got, ",") != "game_started,level_completed" {
		t.Fatalf("events %v", got)
	}
	data := dataOf(t, events[1])
	if data["level"].(float64) != 3 || data["boss"] != "crab" || data["rs_event_id"] == nil {
		t.Fatalf("data %v", data)
	}
	if _, has := events[1]["origin"]; has {
		t.Fatal("an event of the current session carried an origin")
	}
	if ts := int64(events[1]["timestamp"].(float64)); time.Since(time.UnixMilli(ts)) > time.Minute {
		t.Fatalf("timestamp %d is not unix ms", ts)
	}
	closeNow(t, c)
	last := api.only("POST /track/batch")
	if got := eventNames(last[len(last)-1].events(t)); got[len(got)-1] != "game_exited" {
		t.Fatalf("close did not send game_exited: %v", got)
	}
	if c.Track("late", nil) {
		t.Fatal("a closed client queued an event")
	}
}

func TestAPIURLForms(t *testing.T) {
	for in, want := range map[string]string{
		"":                                   "https://api.ravensight.io/api/v1",
		"https://api.example.test":           "https://api.example.test/api/v1",
		"https://api.example.test/":          "https://api.example.test/api/v1",
		"https://api.example.test/api/v1":    "https://api.example.test/api/v1",
		" https://api.example.test/api/v1/ ": "https://api.example.test/api/v1",
	} {
		if got := normalizeAPIURL(in); got != want {
			t.Errorf("%q: got %q want %q", in, got, want)
		}
	}
}

func TestDataIsCopiedWhenTracked(t *testing.T) {
	api := newFakeAPI(t)
	c, _ := newTestClient(t, api, testOpts{})
	data := map[string]any{"score": 1}
	c.Track("x", data)
	data["score"] = 99
	mustFlush(t, c)
	events := api.batchEvents(t)
	if got := dataOf(t, events[len(events)-1])["score"].(float64); got != 1 {
		t.Fatalf("score %v: the map was not copied", got)
	}
	if c.Track("bad", map[string]any{"f": func() {}}) || c.Track("", nil) || c.Track(strings.Repeat("n", 201), nil) {
		t.Fatal("an invalid event was queued")
	}
}

func TestOptInBeforeTheYesDoesNothing(t *testing.T) {
	api := newFakeAPI(t)
	dir := t.TempDir()
	c, _ := newTestClient(t, api, testOpts{dir: dir, cfg: func(cfg *Config) { cfg.Consent = ConsentOptIn }})
	if c.Track("x", nil) || c.DeviceID() != "" || c.Status() != StatusDisabled {
		t.Fatal("collected before the yes")
	}
	_ = flush(t, c)
	if n := len(api.requests()); n != 0 {
		t.Fatalf("%d requests before the yes", n)
	}
	if hasFile(t, dir, deviceIDFile) {
		t.Fatal("a device id was made before the yes")
	}
	if err := c.SetTrackingEnabled(true); err != nil {
		t.Fatal(err)
	}
	if !c.Track("after_yes", nil) || c.DeviceID() == "" {
		t.Fatal("not collecting after the yes")
	}
	mustFlush(t, c)
	if got := eventNames(api.batchEvents(t)); strings.Join(got, ",") != "game_started,after_yes" {
		t.Fatalf("events %v", got)
	}
	if st := (store{dir: dir}).loadState(); !st.OptedIn || st.OptedOut {
		t.Fatalf("state %+v", st)
	}
	// A saved yes starts the next launch at once.
	closeNow(t, c)
	c2, _ := newTestClient(t, api, testOpts{dir: dir, cfg: func(cfg *Config) { cfg.Consent = ConsentOptIn }})
	if !c2.IsCollecting() {
		t.Fatal("the saved yes was not read")
	}
}

func TestSavedOptOutMakesNoRequestsAcrossClients(t *testing.T) {
	api := newFakeAPI(t)
	dir := t.TempDir()
	c, _ := newTestClient(t, api, testOpts{dir: dir})
	c.Track("x", nil)
	mustFlush(t, c)
	id := c.DeviceID()
	if err := c.SetTrackingEnabled(false); err != nil {
		t.Fatal(err)
	}
	if c.Track("y", nil) || c.Status() != StatusDisabled {
		t.Fatal("collecting after opt-out")
	}
	closeNow(t, c)
	if hasFile(t, dir, queueFileName("gt_live_testkey123")) {
		t.Fatal("the queue file survived the opt-out")
	}
	before := len(api.requests())
	c2, _ := newTestClient(t, api, testOpts{dir: dir})
	c2.Track("z", nil)
	_ = flush(t, c2)
	closeNow(t, c2)
	if after := len(api.requests()); after != before {
		t.Fatalf("an opted out client made %d requests", after-before)
	}
	if c2.DeviceID() != id {
		t.Fatal("opting out lost the device id")
	}
	// Back on: one game_started for the run.
	if err := c2.SetTrackingEnabled(true); err != nil {
		t.Fatal(err)
	}
}

func TestOptOutBeforeTheClientExists(t *testing.T) {
	api := newFakeAPI(t)
	dir := t.TempDir()
	cfg := Config{IngestKey: "gt_live_testkey123", StorageDir: dir}
	if err := SetTrackingEnabled(cfg, false); err != nil {
		t.Fatal(err)
	}
	c, _ := newTestClient(t, api, testOpts{dir: dir})
	c.Track("x", nil)
	_ = flush(t, c)
	if len(api.requests()) != 0 || c.TrackingEnabled() {
		t.Fatal("the saved choice was not honoured")
	}
}

func TestResetDeviceIDMintsSavesAndDropsQueueAndSession(t *testing.T) {
	api := newFakeAPI(t)
	dir := t.TempDir()
	api.on("POST /track/batch", func(w http.ResponseWriter, r recorded, n int) {
		if n == 1 {
			writeJSON(w, 503, `{"error":"unavailable"}`)
			return
		}
		api.defaultAnswer(w, r, n)
	})
	c, _ := newTestClient(t, api, testOpts{dir: dir})
	c.Track("before_reset", nil)
	_ = flush(t, c) // fails: the queue file keeps it
	c.persistNow()
	old := c.DeviceID()
	if !hasFile(t, dir, queueFileName("gt_live_testkey123")) {
		t.Fatal("expected a queue file")
	}
	fresh := c.ResetDeviceID()
	if fresh == old || !strings.HasPrefix(fresh, "dev_") {
		t.Fatalf("reset gave %q (old %q)", fresh, old)
	}
	if raw, _ := os.ReadFile(filepath.Join(dir, deviceIDFile)); string(raw) != fresh {
		t.Fatal("the new id was not saved")
	}
	if hasFile(t, dir, queueFileName("gt_live_testkey123")) || c.Stats().Queued != 0 || c.SessionToken() != "" {
		t.Fatal("reset kept the queue or the session")
	}
	c.Track("after_reset", nil)
	mustFlush(t, c)
	sessions := api.only("POST /session")
	if got := sessions[len(sessions)-1].json(t)["deviceId"]; got != fresh {
		t.Fatalf("the next session used %v", got)
	}
	for _, r := range api.only("POST /track/batch")[1:] {
		for _, e := range r.events(t) {
			if e["event"] == "before_reset" {
				t.Fatal("an event of the old id was sent after the reset")
			}
		}
	}
	// A reset while opted out never brings the old id back.
	_ = c.SetTrackingEnabled(false)
	c.ResetDeviceID()
	if hasFile(t, dir, deviceIDFile) {
		t.Fatal("a reset while opted out left a device id file")
	}
	_ = c.SetTrackingEnabled(true)
	if c.DeviceID() == fresh || c.DeviceID() == "" {
		t.Fatalf("after the reset and the yes: %q", c.DeviceID())
	}
}

func TestSessionRefusedStopsWithOneWarning(t *testing.T) {
	for _, tc := range []struct {
		status int
		code   string
		want   Status
	}{
		{401, "invalid_key", StatusInvalidKey},
		{403, "key_paused", StatusInvalidKey},
		{403, "tracking_disabled", StatusTrackingDisabled},
	} {
		t.Run(fmt.Sprint(tc.status, tc.code), func(t *testing.T) {
			api := newFakeAPI(t)
			api.on("POST /session", func(w http.ResponseWriter, r recorded, n int) {
				writeJSON(w, tc.status, fmt.Sprintf(`{"error":%q}`, tc.code))
			})
			var seen []Status
			var mu = make(chan struct{}, 1)
			mu <- struct{}{}
			c, sink := newTestClient(t, api, testOpts{cfg: func(cfg *Config) {
				cfg.OnStatus = func(s Status) { <-mu; seen = append(seen, s); mu <- struct{}{} }
			}})
			c.Track("x", nil)
			_ = flush(t, c)
			_ = flush(t, c)
			if c.Status() != tc.want {
				t.Fatalf("status %q", c.Status())
			}
			if api.count("POST /session") != 1 {
				t.Fatalf("%d session requests", api.count("POST /session"))
			}
			if tc.want == StatusInvalidKey && sink.countContaining("ingest key refused (invalid_key), sending stopped for this run. Check your gt_live_ key.") != 1 {
				t.Fatalf("warning: %v", sink.all())
			}
			if c.Track("y", nil) {
				t.Fatal("tracked after the run stopped")
			}
			<-mu
			if len(seen) == 0 || seen[len(seen)-1] != tc.want {
				t.Fatalf("OnStatus saw %v", seen)
			}
			mu <- struct{}{}
		})
	}
}

func TestSettingsKillSwitchStopsAndClears(t *testing.T) {
	api := newFakeAPI(t)
	api.on("GET /settings", func(w http.ResponseWriter, r recorded, n int) {
		writeJSON(w, 200, `{"trackingEnabled":false}`)
	})
	dir := t.TempDir()
	c, _ := newTestClient(t, api, testOpts{dir: dir})
	c.Track("x", nil)
	_ = flush(t, c)
	if c.Status() != StatusTrackingDisabled || c.Stats().Queued != 0 {
		t.Fatalf("status %q queued %d", c.Status(), c.Stats().Queued)
	}
	if api.count("POST /session") != 0 || api.count("POST /track/batch") != 0 {
		t.Fatal("sent after the kill switch")
	}
	closeNow(t, c)
	if hasFile(t, dir, queueFileName("gt_live_testkey123")) {
		t.Fatal("kept a queue file after the kill switch")
	}
}

func TestBatch401ReopensTheSessionOnceThenBacksOff(t *testing.T) {
	api := newFakeAPI(t)
	api.on("POST /track/batch", func(w http.ResponseWriter, r recorded, n int) {
		writeJSON(w, 401, `{"error":"session_expired"}`)
	})
	c, _ := newTestClient(t, api, testOpts{})
	c.Track("x", nil)
	_ = flush(t, c)
	if api.count("POST /session") != 2 || api.count("POST /track/batch") != 2 {
		t.Fatalf("sessions %d batches %d", api.count("POST /session"), api.count("POST /track/batch"))
	}
	if c.Status() != StatusOffline || c.Stats().Queued != 2 {
		t.Fatalf("status %q queued %d", c.Status(), c.Stats().Queued)
	}
	batches := api.only("POST /track/batch")
	a, b := batches[0].events(t), batches[1].events(t)
	if dataOf(t, a[1])["rs_event_id"] != dataOf(t, b[1])["rs_event_id"] {
		t.Fatal("a resend changed the event id")
	}
}

func TestBatch403KeepsTheEventsForTheNextLaunch(t *testing.T) {
	api := newFakeAPI(t)
	dir := t.TempDir()
	api.on("POST /track/batch", func(w http.ResponseWriter, r recorded, n int) {
		writeJSON(w, 403, `{"error":"key_paused"}`)
	})
	c, _ := newTestClient(t, api, testOpts{dir: dir})
	c.Track("kept", nil)
	_ = flush(t, c)
	if c.Status() != StatusInvalidKey {
		t.Fatalf("status %q", c.Status())
	}
	closeNow(t, c)
	q := readQueueFile(t, dir, "gt_live_testkey123")
	if q.Version != 2 || len(q.Groups) != 1 || len(q.Groups[0].Events) != 2 {
		t.Fatalf("queue file %+v", q)
	}
	// The next launch sends them under their own origin.
	api.on("POST /track/batch", nil)
	c2, _ := newTestClient(t, api, testOpts{dir: dir})
	mustFlush(t, c2)
	batches := api.only("POST /track/batch")
	events := batches[len(batches)-1].events(t)
	var keptOrigin map[string]any
	for _, e := range events {
		if e["event"] == "kept" {
			keptOrigin, _ = e["origin"].(map[string]any)
		}
	}
	if keptOrigin == nil || keptOrigin["session"] != "ses_1" || keptOrigin["stamp"] != "o1.a1.b" {
		t.Fatalf("the replayed event's origin: %v", keptOrigin)
	}
}

func TestRetryAfterIsWaitedExactlyEvenByFlush(t *testing.T) {
	api := newFakeAPI(t)
	clock := newFakeClock()
	api.on("POST /track/batch", func(w http.ResponseWriter, r recorded, n int) {
		if n == 1 {
			w.Header().Set("Retry-After", "120")
			writeJSON(w, 429, `{"error":"rate_limited"}`)
			return
		}
		api.defaultAnswer(w, r, n)
	})
	c, _ := newTestClient(t, api, testOpts{clock: clock})
	c.Track("x", nil)
	_ = flush(t, c)
	if c.Status() != StatusRateLimited {
		t.Fatalf("status %q", c.Status())
	}
	clock.advance(119 * time.Second)
	_ = flush(t, c)
	if api.count("POST /track/batch") != 1 {
		t.Fatal("flush cut a Retry-After short")
	}
	clock.advance(2 * time.Second)
	mustFlush(t, c)
	if api.count("POST /track/batch") != 2 || c.Status() != StatusOK {
		t.Fatalf("batches %d status %q", api.count("POST /track/batch"), c.Status())
	}
}

func TestBackoffDoublesWithJitterAndFlushCutsIt(t *testing.T) {
	c := &Client{now: time.Now, random: func() float64 { return 0 }}
	var waits []time.Duration
	for i := 0; i < 7; i++ {
		start := time.Now()
		c.scheduleBackoffLocked()
		waits = append(waits, c.nextAttempt.Sub(start).Round(time.Second))
	}
	want := []time.Duration{5, 10, 20, 40, 80, 150, 150}
	for i, w := range want {
		if waits[i] != w*time.Second {
			t.Fatalf("waits %v", waits)
		}
	}
	c.random = func() float64 { return 0.9999999 }
	c.backoff = 0
	c.scheduleBackoffLocked()
	if d := time.Until(c.nextAttempt); d >= 10*time.Second || d < 9*time.Second {
		t.Fatalf("jitter bound: %v", d)
	}

	api := newFakeAPI(t)
	api.on("POST /track/batch", func(w http.ResponseWriter, r recorded, n int) {
		if n == 1 {
			writeJSON(w, 500, `{"error":"internal_error"}`)
			return
		}
		api.defaultAnswer(w, r, n)
	})
	cl, _ := newTestClient(t, api, testOpts{})
	cl.Track("x", nil)
	_ = flush(t, cl)
	if cl.Status() != StatusOffline {
		t.Fatalf("status %q", cl.Status())
	}
	mustFlush(t, cl) // a flush cuts the backoff short
}

func TestA400WithAnIndexDropsThatEventOnly(t *testing.T) {
	api := newFakeAPI(t)
	api.on("POST /track/batch", func(w http.ResponseWriter, r recorded, n int) {
		if n == 1 {
			writeJSON(w, 400, `{"error":"invalid_event","index":1}`)
			return
		}
		api.defaultAnswer(w, r, n)
	})
	c, _ := newTestClient(t, api, testOpts{cfg: func(cfg *Config) { cfg.NoLifecycleEvents = true }})
	c.Track("a", nil)
	c.Track("bad", nil)
	c.Track("c", nil)
	mustFlush(t, c)
	batches := api.only("POST /track/batch")
	if got := eventNames(batches[1].events(t)); strings.Join(got, ",") != "a,c" {
		t.Fatalf("resent %v", got)
	}
	if c.Stats().Dropped != 1 {
		t.Fatalf("dropped %d", c.Stats().Dropped)
	}
}

func TestA400WithoutAnIndexDropsTheBatchAndTooLargeSplits(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"batch_too_large", 400, `{"error":"batch_too_large"}`},
		{"413", 413, `{"error":"payload_too_large"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := newFakeAPI(t)
			api.on("POST /track/batch", func(w http.ResponseWriter, r recorded, n int) {
				if n == 1 {
					writeJSON(w, tc.status, tc.body)
					return
				}
				api.defaultAnswer(w, r, n)
			})
			c, _ := newTestClient(t, api, testOpts{cfg: func(cfg *Config) { cfg.NoLifecycleEvents = true }})
			for i := 0; i < 4; i++ {
				c.Track(fmt.Sprintf("e%d", i), nil)
			}
			mustFlush(t, c)
			batches := api.only("POST /track/batch")
			if len(batches) != 3 || len(batches[1].events(t)) != 2 || len(batches[2].events(t)) != 2 {
				t.Fatalf("%d batches", len(batches))
			}
		})
	}
	api := newFakeAPI(t)
	api.on("POST /track/batch", func(w http.ResponseWriter, r recorded, n int) {
		writeJSON(w, 400, `{"error":"invalid_json"}`)
	})
	c, _ := newTestClient(t, api, testOpts{cfg: func(cfg *Config) { cfg.NoLifecycleEvents = true }})
	c.Track("a", nil)
	c.Track("b", nil)
	mustFlush(t, c)
	if api.count("POST /track/batch") != 1 || c.Stats().Dropped != 2 {
		t.Fatalf("batches %d dropped %d", api.count("POST /track/batch"), c.Stats().Dropped)
	}
	// A single event still too large is dropped, never retried as is.
	api2 := newFakeAPI(t)
	api2.on("POST /track/batch", func(w http.ResponseWriter, r recorded, n int) { writeJSON(w, 413, `{}`) })
	c2, _ := newTestClient(t, api2, testOpts{cfg: func(cfg *Config) { cfg.NoLifecycleEvents = true }})
	c2.Track("huge", nil)
	mustFlush(t, c2)
	if api2.count("POST /track/batch") != 1 {
		t.Fatalf("%d batches", api2.count("POST /track/batch"))
	}
}

func TestFiftyQueuedEventsSendWithoutWaitingForTheTimer(t *testing.T) {
	api := newFakeAPI(t)
	c, _ := newTestClient(t, api, testOpts{cfg: func(cfg *Config) { cfg.FlushInterval = time.Hour; cfg.NoLifecycleEvents = true }})
	for i := 0; i < 49; i++ {
		c.Track("e", nil)
	}
	time.Sleep(50 * time.Millisecond)
	if api.count("POST /track/batch") != 0 {
		t.Fatal("sent before 50")
	}
	c.Track("e", nil)
	waitFor(t, "a batch of 50", func() bool { return api.count("POST /track/batch") == 1 })
	if n := len(api.only("POST /track/batch")[0].events(t)); n != 50 {
		t.Fatalf("batch of %d", n)
	}
}

func TestTheTimerSendsAQueuedEvent(t *testing.T) {
	api := newFakeAPI(t)
	c, _ := newTestClient(t, api, testOpts{cfg: func(cfg *Config) { cfg.FlushInterval = 20 * time.Millisecond }})
	c.Track("e", nil)
	waitFor(t, "the timer", func() bool { return len(api.batchEvents(t)) == 2 })
}

func TestQueueCapOf500DropsTheOldestWhileOffline(t *testing.T) {
	api := newFakeAPI(t)
	api.on("GET /settings", func(w http.ResponseWriter, r recorded, n int) { writeJSON(w, 503, `{}`) })
	c, _ := newTestClient(t, api, testOpts{cfg: func(cfg *Config) { cfg.NoLifecycleEvents = true }})
	for i := 0; i < 600; i++ {
		c.Track(fmt.Sprintf("e%d", i), nil)
	}
	st := c.Stats()
	if st.Queued != 500 || st.Dropped != 100 {
		t.Fatalf("queued %d dropped %d", st.Queued, st.Dropped)
	}
	c.mu.Lock()
	first := c.events[0].name
	c.mu.Unlock()
	if first != "e100" {
		t.Fatalf("oldest kept is %s", first)
	}
}

var eventIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

func TestEventIDShapeAndResendAndRelaunch(t *testing.T) {
	api := newFakeAPI(t)
	dir := t.TempDir()
	api.on("POST /track/batch", func(w http.ResponseWriter, r recorded, n int) {
		if n == 1 {
			writeJSON(w, 500, `{}`)
			return
		}
		api.defaultAnswer(w, r, n)
	})
	c, _ := newTestClient(t, api, testOpts{dir: dir})
	c.Track("x", nil)
	_ = flush(t, c)
	mustFlush(t, c)
	b := api.only("POST /track/batch")
	id1, id2 := dataOf(t, b[0].events(t)[1])["rs_event_id"].(string), dataOf(t, b[1].events(t)[1])["rs_event_id"].(string)
	if id1 != id2 || !eventIDPattern.MatchString(id1) {
		t.Fatalf("ids %q %q", id1, id2)
	}
	if !strings.HasPrefix(id1, c.DeviceID()[:16]+"-") {
		t.Fatalf("id %q does not start with the device prefix", id1)
	}
	gs := dataOf(t, b[0].events(t)[0])["rs_event_id"].(string)
	if gs == id1 {
		t.Fatal("two events share an id")
	}
	closeNow(t, c)
	c2, _ := newTestClient(t, api, testOpts{dir: dir})
	c2.Track("y", nil)
	mustFlush(t, c2)
	all := api.batchEvents(t)
	id3 := dataOf(t, all[len(all)-1])["rs_event_id"].(string)
	if !strings.HasPrefix(id3, c.DeviceID()[:16]+"-") || id3 == id1 {
		t.Fatalf("relaunch id %q", id3)
	}
}

func TestAnEventOver8KBIsDroppedWithOneWarning(t *testing.T) {
	api := newFakeAPI(t)
	c, sink := newTestClient(t, api, testOpts{})
	if c.Track("big", map[string]any{"blob": strings.Repeat("x", 8200)}) {
		t.Fatal("queued an event over 8 KB")
	}
	c.Track("big", map[string]any{"blob": strings.Repeat("x", 8200)})
	st := c.Stats()
	if st.Dropped != 2 || st.LastError != "event_too_large" || sink.countContaining("over 8192 bytes") != 1 {
		t.Fatalf("stats %+v log %v", st, sink.all())
	}
	if !c.Track("fits", map[string]any{"blob": strings.Repeat("x", 7900)}) {
		t.Fatal("refused an event under 8 KB")
	}
	// A typed error of control characters fits: its text counts one byte a character.
	if !c.TrackError(ErrorReport{Severity: "error", Message: strings.Repeat("\x01", 200), Stack: strings.Repeat("\x01", 2000)}) {
		t.Fatal("refused a typed error within its cuts")
	}
}

func TestBodiesOf1KBOrMoreAreGzippedAndSignedUncompressed(t *testing.T) {
	api := newFakeAPI(t)
	secret := "dGVzdC1zZWNyZXQtZm9yLWRlbGl2ZXJ5LWNvbnRyYWN0"
	serverTime := time.Now().Unix() + 1000
	api.on("POST /session", func(w http.ResponseWriter, r recorded, n int) {
		writeJSON(w, 201, fmt.Sprintf(`{"token":"tok","expiresIn":3600,"signing":{"algorithm":"HMAC-SHA256","version":1,"secret":%q,"serverTime":%d}}`, secret, serverTime))
	})
	c, _ := newTestClient(t, api, testOpts{})
	c.Track("big", map[string]any{"pad": strings.Repeat("p", 1500)})
	mustFlush(t, c)
	b := api.only("POST /track/batch")[0]
	if !b.Gzip {
		t.Fatal("a body over 1 KB was not compressed")
	}
	ts := b.Header.Get("X-Ravensight-Timestamp")
	var tsn int64
	fmt.Sscan(ts, &tsn)
	if d := tsn - serverTime; d < -5 || d > 5 {
		t.Fatalf("timestamp %d is not on the server's clock (%d)", tsn, serverTime)
	}
	if want := "v1=" + signature(secret, tsn, b.Body); b.Header.Get("X-Ravensight-Signature") != want {
		t.Fatal("the signature is not over the uncompressed body")
	}

	// A session without signing sends no signature, and a small body goes plain.
	api2 := newFakeAPI(t)
	c2, _ := newTestClient(t, api2, testOpts{})
	c2.Track("small", nil)
	mustFlush(t, c2)
	b2 := api2.only("POST /track/batch")[0]
	if b2.Gzip || b2.Header.Get("X-Ravensight-Signature") != "" {
		t.Fatal("unexpected gzip or signature")
	}
}

func TestA415OnGzipResendsUncompressed(t *testing.T) {
	api := newFakeAPI(t)
	api.on("POST /track/batch", func(w http.ResponseWriter, r recorded, n int) {
		if r.Gzip {
			writeJSON(w, 415, `{"error":"unsupported_encoding"}`)
			return
		}
		api.defaultAnswer(w, r, n)
	})
	c, _ := newTestClient(t, api, testOpts{})
	c.Track("big", map[string]any{"pad": strings.Repeat("p", 1500)})
	mustFlush(t, c)
	b := api.only("POST /track/batch")
	if len(b) != 2 || b[1].Gzip {
		t.Fatalf("%d batches", len(b))
	}
}

func TestAnOfflineRunIsSentNextLaunchUnderItsOwnOrigin(t *testing.T) {
	api := newFakeAPI(t)
	dir := t.TempDir()
	var down atomic.Bool
	down.Store(true)
	api.on("POST /track/batch", func(w http.ResponseWriter, r recorded, n int) {
		if down.Load() {
			writeJSON(w, 503, `{}`)
			return
		}
		api.defaultAnswer(w, r, n)
	})
	c, _ := newTestClient(t, api, testOpts{dir: dir, cfg: func(cfg *Config) { cfg.GameVersion = "1.0.0" }})
	c.Track("offline_1", map[string]any{"n": 1})
	_ = flush(t, c)
	closeNow(t, c)
	q := readQueueFile(t, dir, "gt_live_testkey123")
	if len(q.Groups) != 1 || string(q.Groups[0].Origin) == "" {
		t.Fatalf("queue file %+v", q)
	}
	for _, e := range q.Groups[0].Events {
		if e.Timestamp == 0 {
			t.Fatal("a saved event lost its time")
		}
	}
	down.Store(false)
	c2, _ := newTestClient(t, api, testOpts{dir: dir, cfg: func(cfg *Config) { cfg.GameVersion = "1.1.0" }})
	c2.Track("online", nil)
	mustFlush(t, c2)
	last := api.only("POST /track/batch")
	events := last[len(last)-1].events(t)
	got := map[string]any{}
	for _, e := range events {
		got[e["event"].(string)] = e["origin"]
	}
	if o, _ := got["offline_1"].(map[string]any); o == nil || o["session"] != "ses_1" {
		t.Fatalf("offline event origin %v", got["offline_1"])
	}
	if got["online"] != nil {
		t.Fatal("a current event carried an origin")
	}
	closeNow(t, c2)
	if hasFile(t, dir, queueFileName("gt_live_testkey123")) {
		t.Fatal("an empty queue left its file")
	}
}

func TestQueueFileReadingRules(t *testing.T) {
	now := time.Now()
	old := now.Add(-90 * 24 * time.Hour).UnixMilli()
	v1 := fmt.Sprintf(`{"version":1,"groups":[{"events":[{"event":"a","timestamp":%d,"data":{"rs_event_id":"x1"}},{"event":"old","timestamp":%d,"data":{}}]}]}`, now.UnixMilli(), old)
	events, tooOld, _ := decodeQueue([]byte(v1), now, 500)
	if len(events) != 1 || tooOld != 1 || events[0].origin != nil {
		t.Fatalf("v1: %d events, %d too old", len(events), tooOld)
	}
	for _, bad := range []string{``, `{`, `{"version":3,"groups":[]}`, `[]`} {
		if ev, _, _ := decodeQueue([]byte(bad), now, 500); len(ev) != 0 {
			t.Fatalf("%q read as events", bad)
		}
	}
	var b strings.Builder
	b.WriteString(`{"version":2,"groups":[{"origin":{"session":"s"},"events":[`)
	for i := 0; i < 600; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, `{"event":"e%d","timestamp":%d,"data":{"rs_kind":"error","rs_source":"log","rs_trail":"x"}}`, i, now.UnixMilli())
	}
	b.WriteString(`]}]}`)
	events, _, overflow := decodeQueue([]byte(b.String()), now, 500)
	if len(events) != 500 || overflow != 100 || events[0].name != "e100" || !events[0].capture || events[0].session != "s" {
		t.Fatalf("v2: %d events, overflow %d", len(events), overflow)
	}
	if strings.Contains(string(events[0].data), "rs_trail") {
		t.Fatal("a trail was read back from the queue file")
	}

	// Leftover temporary files are deleted on load.
	dir := t.TempDir()
	name := queueFileName("gt_live_testkey123")
	os.WriteFile(filepath.Join(dir, name+".tmp-123"), []byte("partial"), 0o600)
	api := newFakeAPI(t)
	newTestClient(t, api, testOpts{dir: dir})
	if hasFile(t, dir, name+".tmp-123") {
		t.Fatal("a temporary file survived the load")
	}
}

func TestMemoryOnlyQueueWritesNoQueueFile(t *testing.T) {
	api := newFakeAPI(t)
	api.on("POST /track/batch", func(w http.ResponseWriter, r recorded, n int) { writeJSON(w, 503, `{}`) })
	dir := t.TempDir()
	c, _ := newTestClient(t, api, testOpts{dir: dir, cfg: func(cfg *Config) { cfg.MemoryOnlyQueue = true }})
	c.Track("x", nil)
	_ = flush(t, c)
	closeNow(t, c)
	if hasFile(t, dir, queueFileName("gt_live_testkey123")) {
		t.Fatal("wrote a queue file")
	}
}

func TestOptingOutDeletesTheQueueFile(t *testing.T) {
	api := newFakeAPI(t)
	api.on("POST /track/batch", func(w http.ResponseWriter, r recorded, n int) { writeJSON(w, 503, `{}`) })
	dir := t.TempDir()
	c, _ := newTestClient(t, api, testOpts{dir: dir})
	c.Track("x", nil)
	_ = flush(t, c)
	c.persistNow()
	if !hasFile(t, dir, queueFileName("gt_live_testkey123")) {
		t.Fatal("no queue file to delete")
	}
	_ = c.SetTrackingEnabled(false)
	closeNow(t, c)
	if hasFile(t, dir, queueFileName("gt_live_testkey123")) {
		t.Fatal("the queue file survived the opt-out")
	}
}

func TestPlaytestRunFromTheEnvironmentAndCommandLine(t *testing.T) {
	api := newFakeAPI(t)
	dir := t.TempDir()
	(store{dir: dir}).saveState(savedState{OptedOut: true})
	t.Setenv("RAVENSIGHT_PLAYTEST_TOKEN", "pt_token_1")
	t.Setenv("RAVENSIGHT_PLAYTEST_JOB_ID", "job_7")
	c, _ := newTestClient(t, api, testOpts{dir: dir, args: []string{"game", "--ravensight-playtest-run-id=run_42"}})
	if !c.IsPlaytest() || c.DeviceID() != "pt-run_42" || !c.IsCollecting() {
		t.Fatalf("playtest %v device %q collecting %v", c.IsPlaytest(), c.DeviceID(), c.IsCollecting())
	}
	c.Track("x", nil)
	mustFlush(t, c)
	for _, r := range api.requests() {
		if r.Header.Get("X-Ravensight-Playtest") != "pt_token_1" {
			t.Fatalf("%s %s without the playtest header", r.Method, r.Path)
		}
	}
	d := dataOf(t, api.batchEvents(t)[1])
	if d["synthetic"] != true || d["pt_run"] != "run_42" || d["pt_job"] != "job_7" {
		t.Fatalf("tags %v", d)
	}
	_ = c.SetTrackingEnabled(false)
	if c.ResetDeviceID() != "pt-run_42" || !c.IsCollecting() {
		t.Fatal("a playtest honoured the choice or the reset")
	}
	closeNow(t, c)
	for _, f := range files(t, dir) {
		if f != stateFile {
			t.Fatalf("a playtest run wrote %s", f)
		}
	}
}

func TestPlaytestTokenWithoutRunIDNeverRunsAsAPlayer(t *testing.T) {
	api := newFakeAPI(t)
	c, sink := newTestClient(t, api, testOpts{cfg: func(cfg *Config) { cfg.Playtest = Playtest{Token: "tok"} }})
	c.Track("x", nil)
	_ = flush(t, c)
	if len(api.requests()) != 0 || c.Status() != StatusSessionFailed || sink.countContaining("without a run id") != 1 {
		t.Fatalf("requests %d status %q", len(api.requests()), c.Status())
	}
}

func TestPlaytestJobClosedStopsForTheRun(t *testing.T) {
	for code, want := range map[string]Status{"playtest_job_closed": StatusPlaytestJobClosed, "invalid_playtest_token": StatusSessionFailed} {
		api := newFakeAPI(t)
		api.on("POST /session", func(w http.ResponseWriter, r recorded, n int) {
			writeJSON(w, 403, fmt.Sprintf(`{"error":%q}`, code))
		})
		c, _ := newTestClient(t, api, testOpts{cfg: func(cfg *Config) { cfg.Playtest = Playtest{Token: "t", RunID: "r"} }})
		c.Track("x", nil)
		_ = flush(t, c)
		_ = flush(t, c)
		if c.Status() != want || c.Stats().Queued != 0 || api.count("POST /session") != 1 {
			t.Fatalf("%s: status %q queued %d", code, c.Status(), c.Stats().Queued)
		}
	}
}

func TestJoinTokenIsSentOnceAndTheSessionTokenIsReadable(t *testing.T) {
	api := newFakeAPI(t)
	api.on("POST /track/batch", func(w http.ResponseWriter, r recorded, n int) {
		if n == 1 {
			writeJSON(w, 401, `{"error":"session_expired"}`)
			return
		}
		api.defaultAnswer(w, r, n)
	})
	c, _ := newTestClient(t, api, testOpts{cfg: func(cfg *Config) { cfg.JoinToken = "player-token" }})
	if c.SessionToken() != "" {
		t.Fatal("a token before any session")
	}
	c.Track("x", nil)
	mustFlush(t, c)
	s := api.only("POST /session")
	if len(s) != 2 || s[0].json(t)["joinToken"] != "player-token" || s[1].json(t)["joinToken"] != nil {
		t.Fatalf("join token not sent exactly once")
	}
	if c.SessionToken() != "tok-2" {
		t.Fatalf("token %q", c.SessionToken())
	}
}

func TestARefusedJoinTokenFallsBackToAPlainSession(t *testing.T) {
	api := newFakeAPI(t)
	api.on("POST /session", func(w http.ResponseWriter, r recorded, n int) {
		if r.json(t)["joinToken"] != nil {
			writeJSON(w, 401, `{"error":"invalid_join_token"}`)
			return
		}
		api.defaultAnswer(w, r, n)
	})
	c, _ := newTestClient(t, api, testOpts{cfg: func(cfg *Config) { cfg.JoinToken = "dead" }})
	c.Track("x", nil)
	mustFlush(t, c)
	if c.Status() != StatusOK || c.Stats().LastError != "invalid_join_token" || api.count("POST /session") != 2 {
		t.Fatalf("status %q last %q", c.Status(), c.Stats().LastError)
	}
}

func TestShutdownHoldsTheQuitForAtMostItsLimit(t *testing.T) {
	api := newFakeAPI(t)
	release := make(chan struct{})
	api.on("POST /track/batch", func(w http.ResponseWriter, r recorded, n int) {
		select {
		case <-release:
		case <-time.After(10 * time.Second):
		}
		writeJSON(w, 202, `{}`)
	})
	defer close(release)
	c, _ := newTestClient(t, api, testOpts{})
	c.Track("x", nil)
	start := time.Now()
	err := c.Close()
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("close took %v", d)
	}
	if err == nil {
		t.Fatal("close past its limit returned nil")
	}
}

func TestRuntimeSwitchSendsNothingWhileOff(t *testing.T) {
	api := newFakeAPI(t)
	c, _ := newTestClient(t, api, testOpts{cfg: func(cfg *Config) { cfg.StartDisabled = true }})
	if c.Track("x", nil) || c.Status() != StatusDisabled {
		t.Fatal("collecting while off")
	}
	_ = flush(t, c)
	if len(api.requests()) != 0 {
		t.Fatal("requests while off")
	}
	c.SetEnabled(true)
	c.Track("y", nil)
	mustFlush(t, c)
	if got := eventNames(api.batchEvents(t)); strings.Join(got, ",") != "game_started,y" {
		t.Fatalf("events %v", got)
	}
	c.SetEnabled(false)
	c.SetEnabled(true)
	mustFlush(t, c)
	n := 0
	for _, e := range api.batchEvents(t) {
		if e["event"] == "game_started" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("game_started %d times", n)
	}
}

func TestDefaultConvenience(t *testing.T) {
	api := newFakeAPI(t)
	if Track("x", nil) {
		t.Fatal("tracked with no default client")
	}
	c := Init(Config{IngestKey: "gt_live_testkey123", APIURL: api.url(), StorageDir: t.TempDir(), FlushInterval: -1, Logger: slog.New(&logSink{})})
	if Default() != c || !Track("x", nil) {
		t.Fatal("the default client")
	}
	if err := Close(); err != nil {
		t.Fatal(err)
	}
	defaultClient.Store(nil)
}

func TestLateAckAfterAResetKeepsNewEvents(t *testing.T) {
	api := newFakeAPI(t)
	inFlight := make(chan struct{})
	release := make(chan struct{})
	api.on("POST /track/batch", func(w http.ResponseWriter, r recorded, n int) {
		if n == 1 {
			close(inFlight)
			<-release
		}
		api.defaultAnswer(w, r, n)
	})
	c, _ := newTestClient(t, api, testOpts{cfg: func(cfg *Config) { cfg.NoLifecycleEvents = true }})
	c.Track("old", nil)
	done := make(chan struct{})
	go func() { _ = flush(t, c); close(done) }()
	<-inFlight
	c.ResetDeviceID()
	c.Track("new", nil)
	close(release)
	<-done
	mustFlush(t, c)
	if got := eventNames(api.batchEvents(t)); strings.Join(got, ",") != "old,new" {
		t.Fatalf("events %v", got)
	}
}

func TestStatsAndJSONOfTheQueueFile(t *testing.T) {
	e := &qevent{name: "a", timestamp: 1, data: json.RawMessage(`{"rs_trail":"t","x":1}`), saved: json.RawMessage(`{"x":1}`)}
	raw := encodeQueue([]*qevent{e})
	if strings.Contains(string(raw), "rs_trail") {
		t.Fatal("a trail was written to the queue file")
	}
}

func TestTrackNeverWaitsOnTheNetworkAndIsSafeAcrossGoroutines(t *testing.T) {
	api := newFakeAPI(t)
	hang := make(chan struct{})
	api.on("GET /settings", func(w http.ResponseWriter, r recorded, n int) {
		<-hang
		api.defaultAnswer(w, r, n)
	})
	c, _ := newTestClient(t, api, testOpts{cfg: func(cfg *Config) { cfg.FlushInterval = time.Millisecond }})
	waitFor(t, "the hanging settings request", func() bool { return api.count("GET /settings") == 1 })
	start := time.Now()
	done := make(chan struct{})
	for g := 0; g < 8; g++ {
		go func(g int) {
			for i := 0; i < 200; i++ {
				c.Track("tick", map[string]any{"g": g, "i": i})
				c.ConfigNumber("speed", 1)
				_ = c.Status()
			}
			done <- struct{}{}
		}(g)
	}
	for g := 0; g < 8; g++ {
		<-done
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("1,600 tracks took %v while the network hung", d)
	}
	if q := c.Stats().Queued; q != 500 {
		t.Fatalf("queued %d", q)
	}
	close(hang)
	mustFlush(t, c)
}
