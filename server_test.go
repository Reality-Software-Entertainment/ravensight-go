package ravensight

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const testServerKey = "gt_srv_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

// serverAnswer accepts every event the way the server does.
func serverAnswer(w http.ResponseWriter, r recorded) {
	var body struct {
		Events []struct {
			EventID string `json:"eventId"`
		} `json:"events"`
	}
	json.Unmarshal(r.Body, &body)
	var ids []string
	for _, e := range body.Events {
		ids = append(ids, e.EventID)
	}
	raw, _ := json.Marshal(ids)
	writeJSON(w, 200, fmt.Sprintf(`{"accepted":%d,"duplicates":0,"conflicts":0,"refused":0,"acceptedIds":%s,"conflictIds":[],"refusedIds":[],"paused":false}`, len(ids), raw))
}

func newTestServer(t *testing.T, api *fakeAPI, mutate func(*ServerConfig)) (*Server, *logSink) {
	t.Helper()
	sink := &logSink{}
	cfg := ServerConfig{ServerKey: testServerKey, APIURL: api.url(), FlushInterval: -1, Logger: slog.New(sink)}
	if mutate != nil {
		mutate(&cfg)
	}
	s := newServer(cfg, time.Now, func() float64 { return 0 })
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		s.Shutdown(ctx)
	})
	return s, sink
}

func serverEvents(t *testing.T, r recorded) []map[string]any {
	return r.events(t)
}

func TestServerSendsWithTheKeyAndNoOriginOrDeviceID(t *testing.T) {
	api := newFakeAPI(t)
	api.on("POST /site/server-events", func(w http.ResponseWriter, r recorded, n int) { serverAnswer(w, r) })
	s, _ := newTestServer(t, api, nil)
	ctx := context.Background()
	at := time.Now().Add(-time.Hour)
	res, err := s.Send(ctx,
		ServerEvent{Name: "signup_completed", ActorID: "acct_0001", Timestamp: at, Properties: map[string]any{"plan": "pro", "seats": 3, "trial": false, "ref": nil}},
		ServerEvent{Name: "nightly.job-ran", EventID: "job:2026-11-20"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Accepted != 2 || len(res.AcceptedIDs) != 2 || res.AcceptedIDs[1] != "job:2026-11-20" {
		t.Fatalf("result %+v", res)
	}
	r := api.only("POST /site/server-events")[0]
	if r.Header.Get("Authorization") != "Bearer "+testServerKey || r.Header.Get("Origin") != "" || r.Header.Get("X-API-Key") != "" {
		t.Fatal("headers")
	}
	ev := serverEvents(t, r)
	if ev[0]["actorId"] != "acct_0001" || int64(ev[0]["timestamp"].(float64)) != at.UnixMilli() || !opaqueIDPattern.MatchString(ev[0]["eventId"].(string)) {
		t.Fatalf("event %v", ev[0])
	}
	if _, has := ev[1]["actorId"]; has {
		t.Fatal("a backend event carried an actor")
	}
	for _, e := range ev {
		for k := range e {
			if k != "eventId" && k != "name" && k != "timestamp" && k != "actorId" && k != "properties" {
				t.Fatalf("unexpected field %q", k)
			}
		}
		if strings.Contains(string(r.Body), "deviceId") {
			t.Fatal("a device id was sent")
		}
	}
}

func TestServerValidatesEveryEventBeforeSending(t *testing.T) {
	api := newFakeAPI(t)
	s, _ := newTestServer(t, api, nil)
	long := strings.Repeat("a", 201)
	cases := []struct {
		ev    ServerEvent
		field string
	}{
		{ServerEvent{Name: "1bad"}, "Name"},
		{ServerEvent{Name: "ok", EventID: "short"}, "EventID"},
		{ServerEvent{Name: "ok", ActorID: "jane@example.com"}, "ActorID"},
		{ServerEvent{Name: "ok", Timestamp: time.Now().Add(10 * time.Minute)}, "Timestamp"},
		{ServerEvent{Name: "ok", Timestamp: time.Now().Add(-91 * 24 * time.Hour)}, "Timestamp"},
		{ServerEvent{Name: "ok", Properties: map[string]any{"rs_kind": "x"}}, "Properties"},
		{ServerEvent{Name: "ok", Properties: map[string]any{"Context": "x"}}, "Properties"},
		{ServerEvent{Name: "ok", Properties: map[string]any{"email": "a@b.co"}}, "Properties"},
		{ServerEvent{Name: "ok", Properties: map[string]any{"auth": "Bearer abc"}}, "Properties"},
		{ServerEvent{Name: "ok", Properties: map[string]any{"cfg": "password=hunter2"}}, "Properties"},
		{ServerEvent{Name: "ok", Properties: map[string]any{"t": "eyJhbGciOiJIUzI1NiJ9.x"}}, "Properties"},
		{ServerEvent{Name: "ok", Properties: map[string]any{"k": "uses gt_live_abcdefgh123"}}, "Properties"},
		{ServerEvent{Name: "ok", Properties: map[string]any{"run": strings.Repeat("Ab1", 30)}}, "Properties"},
		{ServerEvent{Name: "ok", Properties: map[string]any{"s": long}}, "Properties"},
		{ServerEvent{Name: "ok", Properties: map[string]any{"nested": map[string]any{"a": 1}}}, "Properties"},
		{ServerEvent{Name: "ok", Properties: map[string]any{"f": 1.0 / zero()}}, "Properties"},
	}
	many := map[string]any{}
	for i := 0; i < 25; i++ {
		many[fmt.Sprintf("k%d", i)] = i
	}
	cases = append(cases, struct {
		ev    ServerEvent
		field string
	}{ServerEvent{Name: "ok", Properties: many}, "Properties"})
	for i, tc := range cases {
		_, err := s.Send(context.Background(), ServerEvent{Name: "fine"}, tc.ev)
		var ve *ValidationError
		if !errors.As(err, &ve) || ve.Index != 1 || ve.Field != tc.field {
			t.Errorf("case %d: %v", i, err)
		}
		if err := s.Track(tc.ev); !errors.As(err, &ve) {
			t.Errorf("case %d: Track accepted it", i)
		}
	}
	if len(api.requests()) != 0 {
		t.Fatal("an invalid batch was sent")
	}
}

func zero() float64 { return 0 }

func TestServerRetriesWithTheSameIDsAndWaitsRetryAfter(t *testing.T) {
	api := newFakeAPI(t)
	api.on("POST /site/server-events", func(w http.ResponseWriter, r recorded, n int) {
		switch n {
		case 1:
			w.Header().Set("Retry-After", "0")
			writeJSON(w, 429, `{"error":"rate_limited"}`)
		case 2:
			writeJSON(w, 409, `{"error":"site_changed"}`)
		default:
			serverAnswer(w, r)
		}
	})
	s, _ := newTestServer(t, api, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := s.Send(ctx, ServerEvent{Name: "x"})
	if err != nil || res.Accepted != 1 {
		t.Fatalf("%+v %v", res, err)
	}
	reqs := api.only("POST /site/server-events")
	if len(reqs) != 3 || serverEvents(t, reqs[0])[0]["eventId"] != serverEvents(t, reqs[2])[0]["eventId"] {
		t.Fatal("a retry changed the event id")
	}
}

func TestServerStopsOnARefusedKeyOrAMissingFeature(t *testing.T) {
	for _, tc := range []struct {
		status int
		code   string
	}{{401, "invalid_server_key"}, {404, "not_found"}, {403, "server_only"}} {
		api := newFakeAPI(t)
		api.on("POST /site/server-events", func(w http.ResponseWriter, r recorded, n int) {
			writeJSON(w, tc.status, fmt.Sprintf(`{"error":%q}`, tc.code))
		})
		s, sink := newTestServer(t, api, nil)
		_, err := s.Send(context.Background(), ServerEvent{Name: "x"})
		var re *RefusalError
		if !errors.As(err, &re) || re.Code != tc.code || re.Status != tc.status {
			t.Fatalf("%v", err)
		}
		if err := s.Track(ServerEvent{Name: "y"}); !errors.As(err, &re) || re.Code != tc.code {
			t.Fatalf("Track after a stop: %v", err)
		}
		if sink.countContaining("sending stopped") != 1 {
			t.Fatalf("log %v", sink.all())
		}
	}
}

func TestServerDropsTheEventA400NamesAndSplitsOn413(t *testing.T) {
	api := newFakeAPI(t)
	api.on("POST /site/server-events", func(w http.ResponseWriter, r recorded, n int) {
		switch n {
		case 1:
			writeJSON(w, 400, `{"error":"before_cutover","message":"events[1] (e2): This event predates native collection."}`)
		case 2:
			writeJSON(w, 413, `{"error":"payload_too_large"}`)
		default:
			serverAnswer(w, r)
		}
	})
	s, _ := newTestServer(t, api, nil)
	res, err := s.Send(context.Background(),
		ServerEvent{Name: "a", EventID: "event-01"}, ServerEvent{Name: "b", EventID: "event-02"}, ServerEvent{Name: "c", EventID: "event-03"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Invalid != 1 || res.InvalidIDs[0] != "event-02" || res.Accepted != 2 {
		t.Fatalf("%+v", res)
	}
	reqs := api.only("POST /site/server-events")
	if len(reqs) != 4 || len(serverEvents(t, reqs[2])) != 1 {
		t.Fatalf("%d requests", len(reqs))
	}
}

func TestServerTrackBatchesInTheBackgroundAndKeepsPausedEvents(t *testing.T) {
	api := newFakeAPI(t)
	var paused atomic.Bool
	paused.Store(true)
	api.on("POST /site/server-events", func(w http.ResponseWriter, r recorded, n int) {
		if paused.Load() {
			var body struct {
				Events []struct {
					EventID string `json:"eventId"`
				} `json:"events"`
			}
			json.Unmarshal(r.Body, &body)
			ids, _ := json.Marshal([]string{body.Events[1].EventID})
			first, _ := json.Marshal([]string{body.Events[0].EventID})
			writeJSON(w, 200, fmt.Sprintf(`{"accepted":1,"duplicates":0,"conflicts":0,"refused":1,"acceptedIds":%s,"conflictIds":[],"refusedIds":%s,"paused":true,"reason":"needs_funds"}`, first, ids))
			return
		}
		serverAnswer(w, r)
	})
	s, _ := newTestServer(t, api, nil)
	if err := s.Track(ServerEvent{Name: "a"}); err != nil {
		t.Fatal(err)
	}
	s.Track(ServerEvent{Name: "b"})
	if err := s.Flush(context.Background()); !errors.Is(err, ErrNotSent) {
		t.Fatalf("flush with a paused event: %v", err)
	}
	if st := s.Stats(); st.Queued != 1 || st.Sent != 1 {
		t.Fatalf("stats %+v", st)
	}
	paused.Store(false)
	if err := s.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	reqs := api.only("POST /site/server-events")
	if serverEvents(t, reqs[0])[1]["eventId"] != serverEvents(t, reqs[1])[0]["eventId"] {
		t.Fatal("the paused event was not resent with its id")
	}
	// 50 waiting events send without a flush.
	for i := 0; i < 50; i++ {
		s.Track(ServerEvent{Name: "bulk"})
	}
	waitFor(t, "a background batch", func() bool { return api.count("POST /site/server-events") == 3 })
}

func TestServerGzipsLargeBodiesAndCapsItsQueue(t *testing.T) {
	api := newFakeAPI(t)
	api.on("POST /site/server-events", func(w http.ResponseWriter, r recorded, n int) { serverAnswer(w, r) })
	s, _ := newTestServer(t, api, func(c *ServerConfig) { c.MaxQueueSize = 2; c.FlushInterval = time.Hour })
	var evs []ServerEvent
	for i := 0; i < 20; i++ {
		evs = append(evs, ServerEvent{Name: "x", Properties: map[string]any{"note": strings.Repeat("n ", 50)}})
	}
	if _, err := s.Send(context.Background(), evs...); err != nil {
		t.Fatal(err)
	}
	if !api.only("POST /site/server-events")[0].Gzip {
		t.Fatal("a large body was not compressed")
	}
	s.Track(ServerEvent{Name: "a"})
	s.Track(ServerEvent{Name: "b"})
	var re *RefusalError
	if err := s.Track(ServerEvent{Name: "c"}); !errors.As(err, &re) || re.Code != "queue_full" {
		t.Fatalf("got %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Track(ServerEvent{Name: "d"}); !errors.As(err, &re) || re.Code != "closed" {
		t.Fatalf("after close: %v", err)
	}
	if api.count("POST /site/server-events") != 2 {
		t.Fatal("close did not send the queue")
	}
}

func TestServerWithoutAServerKeyIsInert(t *testing.T) {
	sink := &logSink{}
	for _, key := range []string{"", "gt_live_abc123", "gt_srv_short"} {
		s := NewServer(ServerConfig{ServerKey: key, Logger: slog.New(sink)})
		var re *RefusalError
		if _, err := s.Send(context.Background(), ServerEvent{Name: "x"}); !errors.As(err, &re) {
			t.Fatalf("%q: %v", key, err)
		}
		if err := s.Track(ServerEvent{Name: "x"}); err == nil || s.Close() != nil || s.Flush(context.Background()) != nil {
			t.Fatalf("%q: not inert", key)
		}
	}
	if sink.countContaining("not a gt_srv_ server key") != 2 || sink.countContaining("no server key") != 1 {
		t.Fatalf("log %v", sink.all())
	}
	var nilServer *Server
	if nilServer.Track(ServerEvent{}) == nil || nilServer.Close() != nil {
		t.Fatal("nil server")
	}
}

func TestServerSendRespectsItsContext(t *testing.T) {
	api := newFakeAPI(t)
	api.on("POST /site/server-events", func(w http.ResponseWriter, r recorded, n int) { writeJSON(w, 503, `{}`) })
	s, _ := newTestServer(t, api, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := s.Send(ctx, ServerEvent{Name: "x"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("Send outlived its context")
	}
}

func TestServerFlushNeverCutsARetryAfterAndCopiesProperties(t *testing.T) {
	api := newFakeAPI(t)
	api.on("POST /site/server-events", func(w http.ResponseWriter, r recorded, n int) {
		if n == 1 {
			w.Header().Set("Retry-After", "30")
			writeJSON(w, 429, `{"error":"rate_limited"}`)
			return
		}
		serverAnswer(w, r)
	})
	s, _ := newTestServer(t, api, nil)
	props := map[string]any{"mode": "ranked"}
	if err := s.Track(ServerEvent{Name: "x", Properties: props}); err != nil {
		t.Fatal(err)
	}
	props["mode"] = "casual" // the caller reuses its map
	props["extra"] = 1
	_ = s.Flush(context.Background())
	_ = s.Flush(context.Background())
	if n := api.count("POST /site/server-events"); n != 1 {
		t.Fatalf("flush cut a Retry-After: %d requests", n)
	}
	s.mu.Lock()
	s.retryUntil = time.Time{}
	s.mu.Unlock()
	if err := s.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	reqs := api.only("POST /site/server-events")
	for _, r := range reqs {
		p := serverEvents(t, r)[0]["properties"].(map[string]any)
		if p["mode"] != "ranked" || p["extra"] != nil {
			t.Fatalf("sent %v: the caller's later change leaked in", p)
		}
	}
}
