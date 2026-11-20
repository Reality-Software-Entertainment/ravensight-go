package ravensight

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
)

// testdata/identify-vectors.json is a byte-for-byte copy of the platform's
// identify vectors (version 2). The not_text and integer cases are for
// languages that can pass a non-string; Identify takes a Go string.
type identifyVectors struct {
	Version int    `json:"version"`
	Salt    string `json:"salt"`
	Accept  []struct {
		Note      string `json:"note"`
		AccountID any    `json:"accountId"`
		Integer   bool   `json:"integer"`
		Wire      string `json:"wire"`
		PlayerID  string `json:"playerId"`
	} `json:"accept"`
	Refuse []struct {
		Note      string `json:"note"`
		AccountID any    `json:"accountId"`
		Reason    string `json:"reason"`
	} `json:"refuse"`
	Sequence []struct {
		Step   string `json:"step"`
		Expect struct {
			Sends []struct {
				Session     string `json:"session"`
				AccountHash string `json:"accountHash"`
			} `json:"sends"`
			Stored        *savedIdentity `json:"stored"`
			Refused       string         `json:"refused"`
			DeviceChanged *bool          `json:"device_changed"`
			NewSession    string         `json:"new_session"`
			HeldInMemory  bool           `json:"held_in_memory"`
		} `json:"expect"`
	} `json:"sequence"`
	SaltsDiffer struct {
		AccountID string `json:"accountId"`
		SaltA     string `json:"salt_a"`
		WireA     string `json:"wire_a"`
		SaltB     string `json:"salt_b"`
		WireB     string `json:"wire_b"`
	} `json:"salts_differ"`
}

func loadIdentifyVectors(t *testing.T) identifyVectors {
	t.Helper()
	raw, err := os.ReadFile("testdata/identify-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v identifyVectors
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	if v.Version != 2 {
		t.Fatalf("identify vectors version %d", v.Version)
	}
	return v
}

func TestIdentifyVectorsAcceptAndHash(t *testing.T) {
	v := loadIdentifyVectors(t)
	n := 0
	for _, tc := range v.Accept {
		id, ok := tc.AccountID.(string)
		if !ok || tc.Integer {
			continue // a Go caller always passes a string
		}
		n++
		if reason := identifyRefusal(id); reason != "" {
			t.Errorf("%s: refused %s", tc.Note, reason)
		}
		wire := accountHash(id, v.Salt)
		if wire != tc.Wire || "px_"+wire[:32] != tc.PlayerID {
			t.Errorf("%s: got %s", tc.Note, wire)
		}
	}
	if n < 10 {
		t.Fatalf("only %d accept cases ran", n)
	}
	sd := v.SaltsDiffer
	if accountHash(sd.AccountID, sd.SaltA) != sd.WireA || accountHash(sd.AccountID, sd.SaltB) != sd.WireB || sd.WireA == sd.WireB {
		t.Fatal("salts_differ")
	}
}

func TestIdentifyVectorsRefuseSendNothingAndKeepTheDevice(t *testing.T) {
	v := loadIdentifyVectors(t)
	api := newFakeAPI(t)
	c, _ := newTestClient(t, api, testOpts{})
	mustFlush(t, c)
	device := c.DeviceID()
	n := 0
	for _, tc := range v.Refuse {
		id, ok := tc.AccountID.(string)
		if !ok {
			continue // not_text: a Go string cannot be null or a number
		}
		n++
		err := c.Identify(id)
		var re *RefusalError
		if !errors.As(err, &re) || re.Code != tc.Reason {
			t.Errorf("%s: got %v, want %s", tc.Note, err, tc.Reason)
		}
	}
	if n != 7 {
		t.Fatalf("%d refuse cases ran", n)
	}
	mustFlush(t, c)
	if c.IsIdentified() || c.DeviceID() != device || api.count("POST /identify") != 0 {
		t.Fatal("a refusal changed something")
	}
}

// The vectors' sequence, step by step, against the fake API.
func TestIdentifySequence(t *testing.T) {
	v := loadIdentifyVectors(t)
	api := newFakeAPI(t)
	dir := t.TempDir()
	hold := make(chan struct{})
	api.on("POST /session", func(w http.ResponseWriter, r recorded, n int) {
		if n == 1 {
			<-hold // step 1 looks before session S1 answers
		}
		api.defaultAnswer(w, r, n)
	})
	opts := testOpts{dir: dir, cfg: func(cfg *Config) { cfg.NoLifecycleEvents = true }}
	identifies := func() []recorded { return api.only("POST /identify") }
	stored := func() savedIdentity { return (store{dir: dir}).loadIdentity() }
	step := func(i int) { t.Logf("step %d: %s", i, v.Sequence[i].Step) }
	hashOf := func(i int) string { return v.Sequence[i].Expect.Sends[0].AccountHash }

	// 1. identify("u1") before any session opens: held in memory, nothing stored or sent.
	step(0)
	c, _ := newTestClient(t, api, opts)
	if err := c.Identify("u1"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the first session request", func() bool { return api.count("POST /session") == 1 })
	if len(identifies()) != 0 || stored().AccountHash != "" || !c.IsIdentified() {
		t.Fatal("step 1")
	}
	// 2. S1 answers with the salt: hashed, stored, sent under S1.
	step(1)
	close(hold)
	waitFor(t, "identify under S1", func() bool { return len(identifies()) == 1 })
	r := identifies()[0]
	if r.Header.Get("X-Session-Token") != "tok-1" || r.Header.Get("X-API-Key") != "gt_live_testkey123" {
		t.Fatal("step 2 headers")
	}
	var body map[string]any
	json.Unmarshal(r.Body, &body)
	if !reflect.DeepEqual(body, map[string]any{"accountHash": hashOf(1)}) {
		t.Fatalf("step 2 body %s", r.Body)
	}
	if got := stored(); got != *v.Sequence[1].Expect.Stored {
		t.Fatalf("step 2 stored %+v", got)
	}
	// 3. identify("u1") again in S1: nothing.
	step(2)
	device := c.DeviceID()
	c.Identify("u1")
	mustFlush(t, c)
	if len(identifies()) != 1 || c.DeviceID() != device {
		t.Fatal("step 3")
	}
	// 4. Relaunch, S2 opens with the same salt: sent under S2 with no call.
	step(3)
	closeNow(t, c)
	c, _ = newTestClient(t, api, opts)
	waitFor(t, "identify under S2", func() bool { return len(identifies()) == 2 })
	if r := identifies()[1]; r.Header.Get("X-Session-Token") != "tok-2" || !strings.Contains(string(r.Body), hashOf(3)) {
		t.Fatal("step 4")
	}
	// 5. An email is refused; nothing changes.
	step(4)
	device = c.DeviceID()
	var re *RefusalError
	if err := c.Identify("player@example.com"); !errors.As(err, &re) || re.Code != "looks_like_email" {
		t.Fatalf("step 5: %v", err)
	}
	if stored() != *v.Sequence[4].Expect.Stored || c.DeviceID() != device || len(identifies()) != 2 {
		t.Fatal("step 5")
	}
	// 6. Another account: a fresh device, a new session S3, sent under it.
	step(5)
	c.Identify("u2")
	waitFor(t, "identify under S3", func() bool { return len(identifies()) == 3 })
	if c.DeviceID() == device {
		t.Fatal("step 6: the device did not change")
	}
	if r := identifies()[2]; r.Header.Get("X-Session-Token") != "tok-3" || !strings.Contains(string(r.Body), hashOf(5)) {
		t.Fatal("step 6 send")
	}
	if stored() != *v.Sequence[5].Expect.Stored {
		t.Fatal("step 6 stored")
	}
	// 7. resetIdentity: a fresh device and session, nothing stored or sent.
	step(6)
	device = c.DeviceID()
	c.ResetIdentity()
	if c.DeviceID() == device || stored().AccountHash != "" || c.IsIdentified() || c.SessionToken() != "" {
		t.Fatal("step 7")
	}
	// 8. Again with nothing stored: the device is kept.
	step(7)
	device = c.DeviceID()
	c.ResetIdentity()
	if c.DeviceID() != device {
		t.Fatal("step 8")
	}
	// 9. identify("u3"), answered 429 needs_funds: stored, tried again next session, never in a loop.
	step(8)
	api.on("POST /identify", func(w http.ResponseWriter, r recorded, n int) {
		writeJSON(w, 429, `{"error":"quota_exceeded","reason":"needs_funds"}`)
	})
	c.Identify("u3")
	waitFor(t, "identify u3", func() bool { return len(identifies()) == 4 })
	if r := identifies()[3]; r.Header.Get("X-Session-Token") != "tok-4" || !strings.Contains(string(r.Body), hashOf(8)) {
		t.Fatal("step 9 send")
	}
	mustFlush(t, c)
	c.Track("x", nil)
	mustFlush(t, c)
	if len(identifies()) != 4 || stored() != *v.Sequence[8].Expect.Stored {
		t.Fatal("step 9: retried in a loop or lost the identity")
	}
	// 10. Relaunch under a different salt: the stored hash is deleted, never sent.
	step(9)
	closeNow(t, c)
	api.on("POST /identify", nil)
	api.mu.Lock()
	api.salt = "0f9e8d7c6b5a49382716f5e4d3c2b1a0ffeeddccbbaa99887766554433221100"
	api.mu.Unlock()
	c, _ = newTestClient(t, api, opts)
	c.Track("x", nil)
	mustFlush(t, c)
	if len(identifies()) != 4 || stored().AccountHash != "" || c.IsIdentified() {
		t.Fatal("step 10")
	}
	// 11. identify("u4") with no salt in the answer: unavailable this launch, the id dropped.
	step(10)
	closeNow(t, c)
	api.mu.Lock()
	api.salt = ""
	api.mu.Unlock()
	c, _ = newTestClient(t, api, opts)
	c.Identify("u4")
	c.Track("x", nil)
	mustFlush(t, c)
	if len(identifies()) != 4 || stored().AccountHash != "" || c.IsIdentified() {
		t.Fatal("step 11")
	}
	// 12. Opt out: nothing stored, nothing sent.
	step(11)
	closeNow(t, c)
	api.mu.Lock()
	api.salt = testSalt
	api.mu.Unlock()
	c, _ = newTestClient(t, api, opts)
	c.Identify("u5")
	waitFor(t, "identify u5", func() bool { return len(identifies()) == 5 })
	if stored().AccountHash == "" {
		t.Fatal("step 12 setup")
	}
	c.SetTrackingEnabled(false)
	if stored().AccountHash != "" || c.IsIdentified() {
		t.Fatal("step 12: the opt-out kept the identity")
	}
}

func TestIdentifySendsNothingWithoutAKeyOrBeforeConsent(t *testing.T) {
	api := newFakeAPI(t)
	dir := t.TempDir()
	c, _ := newTestClient(t, api, testOpts{dir: dir, cfg: func(cfg *Config) { cfg.Consent = ConsentOptIn }})
	if err := c.Identify("acct-0001"); err != nil {
		t.Fatal(err)
	}
	_ = flush(t, c)
	if len(api.requests()) != 0 || hasFile(t, dir, identityFile) {
		t.Fatal("identify before consent")
	}
	c.SetTrackingEnabled(true)
	waitFor(t, "identify after the yes", func() bool { return api.count("POST /identify") == 1 })
	if !hasFile(t, dir, identityFile) {
		t.Fatal("not stored after the yes")
	}
	raw, _ := os.ReadFile(dir + "/" + identityFile)
	if strings.Contains(string(raw), "acct-0001") {
		t.Fatal("the raw id was written to disk")
	}
}

func TestIdentify404AndInvalidHashStopForTheLaunchAndNoSessionTwiceStops(t *testing.T) {
	for _, tc := range []struct {
		status int
		code   string
		reopen bool
	}{
		{404, "not_found", false},
		{400, "invalid_account_hash", false},
		{400, "no_session", true},
	} {
		api := newFakeAPI(t)
		api.on("POST /identify", func(w http.ResponseWriter, r recorded, n int) {
			writeJSON(w, tc.status, `{"error":"`+tc.code+`"}`)
		})
		api.on("POST /track/batch", func(w http.ResponseWriter, r recorded, n int) {
			writeJSON(w, 401, `{"error":"session_expired"}`) // forces new sessions
		})
		c, _ := newTestClient(t, api, testOpts{})
		c.Identify("acct-1")
		for i := 0; i < 4; i++ {
			c.Track("x", nil)
			_ = flush(t, c)
			c.mu.Lock()
			c.nextAttempt = c.now()
			c.unlock()
		}
		want := 1
		if tc.reopen {
			want = 2 // tried again at the next session once, then stopped
		}
		if got := api.count("POST /identify"); got != want {
			t.Fatalf("%d %s: %d identify requests, want %d", tc.status, tc.code, got, want)
		}
		if !c.IsIdentified() {
			t.Fatalf("%s: the stored identity was dropped", tc.code)
		}
	}
}

func TestResetDeviceIDAloneKeepsTheIdentity(t *testing.T) {
	api := newFakeAPI(t)
	c, _ := newTestClient(t, api, testOpts{})
	c.Identify("acct-1")
	waitFor(t, "identify", func() bool { return api.count("POST /identify") == 1 })
	c.ResetDeviceID()
	if !c.IsIdentified() {
		t.Fatal("reset dropped the identity")
	}
	c.Track("x", nil)
	mustFlush(t, c)
	if api.count("POST /identify") != 2 {
		t.Fatal("the next session was not identified again")
	}
}

func TestIdentifyInAPlaytestSendsNothing(t *testing.T) {
	api := newFakeAPI(t)
	c, _ := newTestClient(t, api, testOpts{cfg: func(cfg *Config) { cfg.Playtest = Playtest{Token: "t", RunID: "r"} }})
	c.Identify("acct-1")
	c.Track("x", nil)
	mustFlush(t, c)
	if api.count("POST /identify") != 0 {
		t.Fatal("a playtest run sent identify")
	}
}
