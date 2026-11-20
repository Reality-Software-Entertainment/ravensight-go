package ravensight

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"sort"
)

// Remote config documents are bounded on the device as on the server.
const (
	maxConfigKeys        = 50
	maxConfigValueBytes  = 8192
	maxConfigExperiments = 10
	maxConfigDepth       = 20
)

type assignment struct {
	Key     string `json:"key"`
	Variant string `json:"variant"`
}

type remoteState struct {
	values      map[string]json.RawMessage
	experiments []assignment
	etag        string
	scope       string
	wanted      bool              // a fetch is due
	generation  uint64            // bumped when values are cleared, so a late answer changes nothing
	exposed     map[string]string // experiments exposed this run: key to variant
	cacheRead   bool
}

// configCache is the scoped disk copy of the last good document.
type configCache struct {
	Scope       string                     `json:"scope"`
	ETag        string                     `json:"etag"`
	Values      map[string]json.RawMessage `json:"values"`
	Experiments []assignment               `json:"experiments"`
}

// configScopeLocked keys the cache on everything that decides the answer,
// so another key, device, platform or build never reuses these values.
func (c *Client) configScopeLocked() string {
	sum := sha256.Sum256([]byte(c.api + "\n" + c.cfg.IngestKey + "\n" + c.deviceID + "\n" + c.platform + "\n" + c.gameVersion))
	return hex.EncodeToString(sum[:])
}

func (c *Client) loadConfigCacheLocked() {
	if c.rc.cacheRead || c.isPlaytest {
		return
	}
	c.rc.cacheRead = true
	raw, err := c.st.read(configFile)
	if err != nil {
		return
	}
	var cache configCache
	if json.Unmarshal(raw, &cache) != nil || cache.Scope != c.configScopeLocked() {
		return
	}
	if values, experiments, ok := validConfig(cache.Values, cache.Experiments); ok {
		c.rc.values, c.rc.experiments, c.rc.etag, c.rc.scope = values, experiments, cache.ETag, cache.Scope
	}
}

// clearRemoteLocked drops the active values and invalidates a fetch in
// flight. Used by opt-out, the runtime switch and a device id reset.
func (c *Client) clearRemoteLocked() {
	c.rc.values = nil
	c.rc.experiments = nil
	c.rc.etag = ""
	c.rc.scope = ""
	c.rc.generation++
	c.rc.cacheRead = false
}

// FetchConfig asks for fresh remote config now. Every session start fetches
// it anyway; OnConfigReady says when an answer arrived.
func (c *Client) FetchConfig() {
	if c == nil || c.inert {
		return
	}
	c.mu.Lock()
	if c.activeLocked() {
		c.rc.wanted = true
		c.wakeWorker()
	}
	c.unlock()
}

func (c *Client) fetchConfig() bool {
	c.mu.Lock()
	c.rc.wanted = false
	gen := c.rc.generation
	scope := c.configScopeLocked()
	etag := ""
	if c.rc.values != nil && c.rc.scope == scope {
		etag = c.rc.etag
	}
	q := url.Values{}
	q.Set("deviceId", c.deviceID)
	q.Set("platform", c.platform)
	q.Set("gameVersion", c.gameVersion)
	c.unlock()
	headers := map[string]string{"X-API-Key": c.cfg.IngestKey}
	if etag != "" {
		headers["If-None-Match"] = etag
	}
	a := c.request("GET", "/ingest/config?"+q.Encode(), nil, headers)

	c.mu.Lock()
	fresh := false
	var cache []byte
	switch {
	case gen != c.rc.generation || !c.activeLocked():
		c.unlock()
		return true
	case a.err == nil && a.status == 304 && etag != "":
		fresh = true
	case a.err == nil && a.status == 200:
		var doc struct {
			Values      map[string]json.RawMessage `json:"values"`
			Experiments []assignment               `json:"experiments"`
			ETag        string                     `json:"etag"`
		}
		if a.json(&doc) {
			if values, experiments, ok := validConfig(doc.Values, doc.Experiments); ok {
				tag := a.etag
				if tag == "" {
					tag = doc.ETag
				}
				c.rc.values, c.rc.experiments, c.rc.etag, c.rc.scope = values, experiments, tag, scope
				c.pruneExposureLocked()
				fresh = true
				if !c.isPlaytest {
					cache, _ = marshalJSON(configCache{Scope: scope, ETag: tag, Values: values, Experiments: experiments})
				}
			}
		}
	}
	f := c.cfg.OnConfigReady
	c.unlock()
	if cache != nil {
		_ = c.st.write(configFile, cache)
	}
	if f != nil {
		f(fresh)
	}
	return true
}

// validConfig checks a document's bounds. A malformed document keeps the
// last valid one.
func validConfig(values map[string]json.RawMessage, experiments []assignment) (map[string]json.RawMessage, []assignment, bool) {
	if values == nil {
		values = map[string]json.RawMessage{}
	}
	if len(values) > maxConfigKeys || len(experiments) > maxConfigExperiments {
		return nil, nil, false
	}
	for _, v := range values {
		if len(v) > maxConfigValueBytes || jsonDepth(v) > maxConfigDepth {
			return nil, nil, false
		}
	}
	for _, e := range experiments {
		if !experimentKeyPattern.MatchString(e.Key) || !variantPattern.MatchString(e.Variant) {
			return nil, nil, false
		}
	}
	return values, experiments, true
}

func jsonDepth(raw json.RawMessage) int {
	depth, deepest := 0, 0
	inString, escaped := false, false
	for _, b := range raw {
		switch {
		case escaped:
			escaped = false
		case inString && b == '\\':
			escaped = true
		case b == '"':
			inString = !inString
		case inString:
		case b == '{' || b == '[':
			depth++
			if depth > deepest {
				deepest = depth
			}
		case b == '}' || b == ']':
			depth--
		}
	}
	return deepest
}

// pruneExposureLocked stops tagging an experiment the last fetch no longer
// assigns (stopped), from the next event on.
func (c *Client) pruneExposureLocked() {
	if len(c.rc.exposed) == 0 {
		return
	}
	current := map[string]string{}
	for _, e := range c.rc.experiments {
		current[e.Key] = e.Variant
	}
	for key, variant := range c.rc.exposed {
		if current[key] != variant {
			delete(c.rc.exposed, key)
		}
	}
}

// readConfig returns a value and marks exposure: a typed read marks every
// experiment the last fetch assigned as exposed, and the first time an
// experiment is exposed in a run one rs_exposure event is queued with its
// tags. A fetch alone is not an exposure.
func (c *Client) readConfig(key string) (json.RawMessage, bool) {
	if c == nil || c.inert {
		return nil, false
	}
	c.mu.Lock()
	defer c.unlock()
	if !c.activeLocked() {
		return nil, false
	}
	if c.rc.exposed == nil {
		c.rc.exposed = map[string]string{}
	}
	fresh := map[string]json.RawMessage{}
	for _, e := range c.rc.experiments {
		if c.rc.exposed[e.Key] != e.Variant {
			c.rc.exposed[e.Key] = e.Variant
			fresh["rs_exp_"+e.Key] = mustJSON(e.Variant)
		}
	}
	if len(fresh) > 0 {
		// Exactly the tags and an rs_event_id: the shape public keys accept.
		c.queueFieldsLocked("rs_exposure", fresh, false, nil, 0)
	}
	v, ok := c.rc.values[key]
	return v, ok
}

// tagExposureLocked keeps a well-formed rs_exp_ tag the game set itself,
// removes a malformed one, and adds every experiment exposed this run.
func (c *Client) tagExposureLocked(fields map[string]json.RawMessage) {
	for k, v := range fields {
		name, ok := cutPrefix(k, "rs_exp_")
		if !ok {
			continue
		}
		var variant string
		if !experimentKeyPattern.MatchString(name) || json.Unmarshal(v, &variant) != nil || !variantPattern.MatchString(variant) {
			delete(fields, k)
		}
	}
	keys := make([]string, 0, len(c.rc.exposed))
	for k := range c.rc.exposed {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if _, set := fields["rs_exp_"+k]; !set {
			fields["rs_exp_"+k] = mustJSON(c.rc.exposed[k])
		}
	}
}

func cutPrefix(s, prefix string) (string, bool) {
	if len(s) >= len(prefix) && s[:len(prefix)] == prefix {
		return s[len(prefix):], true
	}
	return s, false
}

// ConfigBool returns a boolean config value, or fallback when there is
// none, it is another type, or the client is not collecting. Values are
// never converted: the string "false" is not a boolean.
func (c *Client) ConfigBool(key string, fallback bool) bool {
	raw, ok := c.readConfig(key)
	if !ok {
		return fallback
	}
	switch string(bytes.TrimSpace(raw)) {
	case "true":
		return true
	case "false":
		return false
	}
	return fallback
}

// ConfigNumber returns a number config value, or fallback.
func (c *Client) ConfigNumber(key string, fallback float64) float64 {
	raw, ok := c.readConfig(key)
	if !ok {
		return fallback
	}
	t := bytes.TrimSpace(raw)
	if len(t) == 0 || !(t[0] == '-' || t[0] >= '0' && t[0] <= '9') {
		return fallback
	}
	var v float64
	if json.Unmarshal(t, &v) != nil {
		return fallback
	}
	return v
}

// ConfigString returns a string config value, or fallback.
func (c *Client) ConfigString(key, fallback string) string {
	raw, ok := c.readConfig(key)
	if !ok {
		return fallback
	}
	var v string
	t := bytes.TrimSpace(raw)
	if len(t) == 0 || t[0] != '"' || json.Unmarshal(t, &v) != nil {
		return fallback
	}
	return v
}

// ConfigJSON returns an object or array config value as JSON, or fallback
// for anything else.
func (c *Client) ConfigJSON(key string, fallback json.RawMessage) json.RawMessage {
	raw, ok := c.readConfig(key)
	if !ok {
		return fallback
	}
	t := bytes.TrimSpace(raw)
	if len(t) == 0 || (t[0] != '{' && t[0] != '[') {
		return fallback
	}
	return append(json.RawMessage(nil), t...)
}
