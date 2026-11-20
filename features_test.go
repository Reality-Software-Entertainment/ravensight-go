package ravensight

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
)

// --- Delivery vectors ----------------------------------------------------------

type deliveryVectors struct {
	Signing struct {
		Secret string `json:"secret"`
		Header []struct {
			Name      string `json:"name"`
			Timestamp int64  `json:"timestamp"`
			Body      string `json:"body"`
			Header    string `json:"header"`
		} `json:"header"`
	} `json:"signing"`
	AnyKeyEventSize struct {
		Cases []struct {
			Name   string                     `json:"name"`
			Data   map[string]json.RawMessage `json:"data"`
			Expect *string                    `json:"expect"`
		} `json:"cases"`
	} `json:"anyKeyEventSize"`
}

func loadDeliveryVectors(t *testing.T) deliveryVectors {
	raw, err := os.ReadFile("testdata/delivery-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v deliveryVectors
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestDeliveryVectorsHeaderSignatures(t *testing.T) {
	v := loadDeliveryVectors(t)
	if len(v.Signing.Header) < 4 {
		t.Fatal("no header vectors")
	}
	for _, tc := range v.Signing.Header {
		if got := "v1=" + signature(v.Signing.Secret, tc.Timestamp, []byte(tc.Body)); got != tc.Header {
			t.Errorf("%s: got %s", tc.Name, got)
		}
	}
}

// expand turns {repeat, count} into the repeated string.
func expand(t *testing.T, raw json.RawMessage) any {
	var rep struct {
		Repeat *string `json:"repeat"`
		Count  int     `json:"count"`
	}
	if json.Unmarshal(raw, &rep) == nil && rep.Repeat != nil {
		return strings.Repeat(*rep.Repeat, rep.Count)
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestDeliveryVectorsEventSize(t *testing.T) {
	v := loadDeliveryVectors(t)
	if len(v.AnyKeyEventSize.Cases) < 5 {
		t.Fatal("no size vectors")
	}
	for _, tc := range v.AnyKeyEventSize.Cases {
		data := map[string]any{}
		texts := map[string]string{}
		for k, raw := range tc.Data {
			data[k] = expand(t, raw)
			if s, ok := data[k].(string); ok {
				texts[k] = s
			}
		}
		encoded, _ := marshalJSON(data)
		kind, _ := data["rs_kind"].(string)
		over := countedBytes(kind, texts, encoded) > maxEventBytes
		want := tc.Expect != nil && *tc.Expect == "event_too_large"
		if over != want {
			t.Errorf("%s: over=%v want %v (%d bytes)", tc.Name, over, want, countedBytes(kind, texts, encoded))
		}
	}
}

// --- Remote config and exposure ------------------------------------------------

func configAPI(t *testing.T, doc *atomic.Value) *fakeAPI {
	api := newFakeAPI(t)
	api.on("GET /ingest/config", func(w http.ResponseWriter, r recorded, n int) {
		body := doc.Load().(string)
		var parsed struct {
			ETag string `json:"etag"`
		}
		json.Unmarshal([]byte(body), &parsed)
		if parsed.ETag != "" && r.Header.Get("If-None-Match") == parsed.ETag {
			w.WriteHeader(304)
			return
		}
		w.Header().Set("ETag", parsed.ETag)
		writeJSON(w, 200, body)
	})
	return api
}

func TestExposureTagsFollowTheFirstTypedRead(t *testing.T) {
	var doc atomic.Value
	doc.Store(`{"values":{"speed":2.5,"hint":true,"title":"Hi","layout":{"a":1}},"experiments":[{"key":"boss_hp","variant":"hard"}],"etag":"\"e1\""}`)
	api := configAPI(t, &doc)
	var ready atomic.Int32
	c, _ := newTestClient(t, api, testOpts{cfg: func(cfg *Config) { cfg.OnConfigReady = func(fresh bool) { ready.Add(1) } }})
	c.Track("before_read", nil)
	mustFlush(t, c)
	if ready.Load() != 1 {
		t.Fatal("OnConfigReady")
	}
	q := api.only("GET /ingest/config")[0].Query
	if q.Get("deviceId") != c.DeviceID() || q.Get("platform") != "test" || q.Get("gameVersion") != "1.0.0" {
		t.Fatalf("config query %v", q)
	}
	// A fetch alone tags nothing.
	for _, e := range api.batchEvents(t) {
		if _, has := dataOf(t, e)["rs_exp_boss_hp"]; has {
			t.Fatal("tagged before a read")
		}
	}
	if c.ConfigNumber("speed", 1) != 2.5 || !c.ConfigBool("hint", false) || c.ConfigString("title", "") != "Hi" ||
		string(c.ConfigJSON("layout", nil)) != `{"a":1}` {
		t.Fatal("typed reads")
	}
	// Types are never converted.
	if c.ConfigBool("title", true) != true || c.ConfigNumber("hint", 7) != 7 || c.ConfigString("speed", "f") != "f" ||
		string(c.ConfigJSON("speed", json.RawMessage(`[]`))) != `[]` || c.ConfigNumber("missing", 3) != 3 {
		t.Fatal("a value was converted")
	}
	c.Track("after_read", map[string]any{"rs_exp_mine": "on", "rs_exp_Bad": "x"})
	mustFlush(t, c)
	var exposures int
	for _, e := range api.batchEvents(t) {
		d := dataOf(t, e)
		switch e["event"] {
		case "rs_exposure":
			exposures++
			if len(d) != 2 || d["rs_exp_boss_hp"] != "hard" {
				t.Fatalf("rs_exposure data %v", d)
			}
		case "after_read":
			if d["rs_exp_boss_hp"] != "hard" || d["rs_exp_mine"] != "on" || d["rs_exp_Bad"] != nil {
				t.Fatalf("after_read tags %v", d)
			}
		}
	}
	if exposures != 1 {
		t.Fatalf("%d rs_exposure events", exposures)
	}
	// A stopped experiment is not tagged after the next fetch, and a 304 keeps the cache.
	doc.Store(`{"values":{"speed":3},"experiments":[],"etag":"\"e2\""}`)
	c.FetchConfig()
	mustFlush(t, c)
	c.Track("after_stop", nil)
	mustFlush(t, c)
	events := api.batchEvents(t)
	if _, has := dataOf(t, events[len(events)-1])["rs_exp_boss_hp"]; has {
		t.Fatal("a stopped experiment was tagged")
	}
	if c.ConfigNumber("speed", 0) != 3 {
		t.Fatal("the new value")
	}
	c.FetchConfig()
	mustFlush(t, c)
	reqs := api.only("GET /ingest/config")
	if reqs[len(reqs)-1].Header.Get("If-None-Match") != `"e2"` || ready.Load() != 3 {
		t.Fatalf("conditional fetch: %q, ready %d", reqs[len(reqs)-1].Header.Get("If-None-Match"), ready.Load())
	}
}

func TestAnOfflineEventKeepsItsOriginalTag(t *testing.T) {
	var doc atomic.Value
	doc.Store(`{"values":{"k":1},"experiments":[{"key":"exp","variant":"a"}],"etag":"\"e1\""}`)
	api := configAPI(t, &doc)
	dir := t.TempDir()
	c, _ := newTestClient(t, api, testOpts{dir: dir})
	mustFlush(t, c)
	c.ConfigNumber("k", 0)
	api.on("POST /track/batch", func(w http.ResponseWriter, r recorded, n int) { writeJSON(w, 503, `{}`) })
	c.Track("offline", nil)
	_ = flush(t, c)
	closeNow(t, c)
	api.on("POST /track/batch", nil)
	doc.Store(`{"values":{"k":1},"experiments":[{"key":"exp","variant":"b"}],"etag":"\"e2\""}`)
	c2, _ := newTestClient(t, api, testOpts{dir: dir})
	mustFlush(t, c2)
	for _, e := range api.batchEvents(t) {
		if e["event"] == "offline" && dataOf(t, e)["rs_exp_exp"] != "a" {
			t.Fatalf("the offline event's tag: %v", dataOf(t, e))
		}
	}
}

func TestConfigCacheIsScopedAndMalformedDocumentsKeepTheLast(t *testing.T) {
	var doc atomic.Value
	doc.Store(`{"values":{"k":"cached"},"experiments":[],"etag":"\"e1\""}`)
	api := configAPI(t, &doc)
	dir := t.TempDir()
	c, _ := newTestClient(t, api, testOpts{dir: dir})
	mustFlush(t, c)
	closeNow(t, c)
	// Offline next launch: the cache answers.
	api.on("GET /settings", func(w http.ResponseWriter, r recorded, n int) { writeJSON(w, 503, `{}`) })
	c2, _ := newTestClient(t, api, testOpts{dir: dir})
	if c2.ConfigString("k", "fallback") != "cached" {
		t.Fatal("the cache was not read")
	}
	closeNow(t, c2)
	// Another build never reuses it.
	c3, _ := newTestClient(t, api, testOpts{dir: dir, cfg: func(cfg *Config) { cfg.GameVersion = "9.9.9" }})
	if c3.ConfigString("k", "fallback") != "fallback" {
		t.Fatal("another build reused the cache")
	}
	closeNow(t, c3)
	api.on("GET /settings", nil)
	// A malformed document keeps the last valid one.
	var keys []string
	for i := 0; i < 51; i++ {
		keys = append(keys, fmt.Sprintf(`"k%d":1`, i))
	}
	doc.Store(`{"values":{` + strings.Join(keys, ",") + `},"experiments":[],"etag":"\"e9\""}`)
	c4, _ := newTestClient(t, api, testOpts{dir: dir})
	mustFlush(t, c4)
	if c4.ConfigString("k", "fallback") != "cached" {
		t.Fatal("a malformed document replaced the last valid one")
	}
	// Opt-out clears the values and the cache.
	c4.SetTrackingEnabled(false)
	if hasFile(t, dir, configFile) || c4.ConfigString("k", "fallback") != "fallback" {
		t.Fatal("opt-out kept the config")
	}
}

// --- Campaign links ------------------------------------------------------------

func TestCampaignLinkIsSentOnceLowercasedAndClearedOnlyByAToken(t *testing.T) {
	api := newFakeAPI(t)
	api.on("POST /session", func(w http.ResponseWriter, r recorded, n int) {
		if n == 1 {
			writeJSON(w, 500, `{}`)
			return
		}
		api.defaultAnswer(w, r, n)
	})
	api.on("POST /track/batch", func(w http.ResponseWriter, r recorded, n int) {
		if n == 1 {
			writeJSON(w, 401, `{}`) // a second session
			return
		}
		api.defaultAnswer(w, r, n)
	})
	c, _ := newTestClient(t, api, testOpts{cfg: func(cfg *Config) { cfg.CampaignLink = "64B0000000000000000000A1" }})
	var re *RefusalError
	if err := c.SetCampaignLink("not-a-link"); !errors.As(err, &re) || re.Code != "invalid_link" {
		t.Fatalf("got %v", err)
	}
	c.Track("x", nil)
	_ = flush(t, c)
	mustFlush(t, c)
	s := api.only("POST /session")
	if len(s) != 3 {
		t.Fatalf("%d sessions", len(s))
	}
	if s[0].json(t)["campaignLink"] != "64b0000000000000000000a1" || s[1].json(t)["campaignLink"] != "64b0000000000000000000a1" {
		t.Fatal("not kept across a failed session")
	}
	if s[2].json(t)["campaignLink"] != nil {
		t.Fatal("sent twice")
	}
}

func TestCampaignLinkNeverGoesInAPlaytestOrBeforeConsent(t *testing.T) {
	api := newFakeAPI(t)
	c, _ := newTestClient(t, api, testOpts{cfg: func(cfg *Config) { cfg.Playtest = Playtest{Token: "t", RunID: "r"} }})
	c.SetCampaignLink("64b0000000000000000000a1")
	c.Track("x", nil)
	mustFlush(t, c)
	if api.only("POST /session")[0].json(t)["campaignLink"] != nil {
		t.Fatal("sent in a playtest")
	}
	api2 := newFakeAPI(t)
	c2, _ := newTestClient(t, api2, testOpts{cfg: func(cfg *Config) { cfg.Consent = ConsentOptIn }})
	c2.SetCampaignLink("64b0000000000000000000a1")
	_ = flush(t, c2)
	if len(api2.requests()) != 0 {
		t.Fatal("sent before consent")
	}
	c2.SetTrackingEnabled(true)
	mustFlush(t, c2)
	if api2.only("POST /session")[0].json(t)["campaignLink"] != "64b0000000000000000000a1" {
		t.Fatal("not sent after the yes")
	}
}

// --- Feedback ------------------------------------------------------------------

func TestFeedbackValidationRefusesLocallyWithoutARequest(t *testing.T) {
	api := newFakeAPI(t)
	c, _ := newTestClient(t, api, testOpts{})
	ctx := context.Background()
	for _, tc := range []struct {
		f    Feedback
		code string
	}{
		{Feedback{Message: "  "}, "empty_message"},
		{Feedback{Message: strings.Repeat("m", 2001)}, "message_too_long"},
		{Feedback{Message: "m", Category: "rant"}, "invalid_category"},
		{Feedback{Message: "m", Rating: 6}, "invalid_rating"},
		{Feedback{Message: "m", Rating: -1}, "invalid_rating"},
	} {
		var re *RefusalError
		if err := c.SubmitFeedback(ctx, tc.f); !errors.As(err, &re) || re.Code != tc.code {
			t.Errorf("%+v: %v", tc.f, err)
		}
	}
	if api.count("POST /feedback") != 0 {
		t.Fatal("a refused feedback made a request")
	}
	if err := c.SubmitFeedback(ctx, Feedback{Message: "The boss fight drags", Category: "suggestion"}); err != nil {
		t.Fatal(err)
	}
	f := api.only("POST /feedback")[0]
	body := f.json(t)
	if body["message"] != "The boss fight drags" || body["category"] != "suggestion" || body["rating"] != nil {
		t.Fatalf("body %v", body)
	}
	if f.Header.Get("X-Session-Token") == "" {
		t.Fatal("no session token")
	}
	c.SetEnabled(false)
	var re *RefusalError
	if err := c.SubmitFeedback(ctx, Feedback{Message: "m"}); !errors.As(err, &re) || re.Code != "disabled" {
		t.Fatalf("disabled: %v", err)
	}
	api.on("POST /feedback", func(w http.ResponseWriter, r recorded, n int) { writeJSON(w, 403, `{"error":"feedback_disabled"}`) })
	c.SetEnabled(true)
	if err := c.SubmitFeedback(ctx, Feedback{Message: "m", Rating: 5}); !errors.As(err, &re) || re.Code != "feedback_disabled" || re.Status != 403 {
		t.Fatalf("server refusal: %v", err)
	}
}

// --- Typed events --------------------------------------------------------------

func TestTypedEventsCarryTheReservedKeys(t *testing.T) {
	api := newFakeAPI(t)
	c, _ := newTestClient(t, api, testOpts{cfg: func(cfg *Config) { cfg.NoLifecycleEvents = true }})
	score := 1250.5
	c.TrackPurchase(Purchase{Amount: 499, Currency: "usd", ItemType: "pack", ItemID: "gems", TransactionID: "t9", Extra: map[string]any{"rs_kind": "mine", "shop": "main"}})
	c.TrackResource(Resource{Flow: "sink", Resource: "gold", Amount: 120})
	c.TrackProgression(Progression{Status: "complete", P1: "w1", Score: &score})
	c.TrackError(ErrorReport{Severity: "error", Message: strings.Repeat("m", 300), Type: "SaveError", Name: "save_error"})
	mustFlush(t, c)
	ev := api.batchEvents(t)
	if got := eventNames(ev); strings.Join(got, ",") != "purchase,resource_sink,progression_complete,save_error" {
		t.Fatalf("names %v", got)
	}
	p := dataOf(t, ev[0])
	if p["rs_kind"] != "purchase" || p["rs_amount"].(float64) != 499 || p["rs_currency"] != "USD" || p["shop"] != "main" || p["rs_transaction_id"] != "t9" || p["rs_cart"] != nil {
		t.Fatalf("purchase %v", p)
	}
	if r := dataOf(t, ev[1]); r["rs_flow"] != "sink" || r["rs_resource"] != "gold" {
		t.Fatalf("resource %v", r)
	}
	if g := dataOf(t, ev[2]); g["rs_status"] != "complete" || g["rs_score"].(float64) != 1250.5 || g["rs_p2"] != nil {
		t.Fatalf("progression %v", g)
	}
	if e := dataOf(t, ev[3]); len(e["rs_message"].(string)) != 200 || e["rs_error_type"] != "SaveError" || e["rs_source"] != nil {
		t.Fatalf("error %v", e)
	}
}
