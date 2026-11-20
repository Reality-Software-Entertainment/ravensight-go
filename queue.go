package ravensight

import (
	"bytes"
	"encoding/json"
	"time"
)

// qevent is one queued event. Its data is serialized once, when it is
// tracked, so a later change to the caller's map changes nothing and a
// resend carries exactly the same bytes and rs_event_id.
type qevent struct {
	seq       uint64
	name      string
	timestamp int64           // unix milliseconds, when it was tracked
	data      json.RawMessage // the full data object
	saved     json.RawMessage // the data written to the queue file (no rs_trail)
	origin    json.RawMessage // the origin of the session it was tracked under
	session   string          // origin.session, "" when unknown
	capture   bool            // an automatic error capture
	inFlight  bool
}

// automaticSources are the rs_source values of automatic captures. A game's
// own TrackError is gameplay.
var automaticSources = map[string]bool{
	"uncaught": true, "unhandled_rejection": true, "request": true, "console": true,
	"log": true, "dead_click": true, "crash": true,
}

// queueFile is the persisted queue, version 2 of the delivery contract.
type queueFile struct {
	Version int          `json:"version"`
	Groups  []queueGroup `json:"groups"`
}

type queueGroup struct {
	Origin json.RawMessage `json:"origin,omitempty"`
	Events []queueEntry    `json:"events"`
}

type queueEntry struct {
	Event     string          `json:"event"`
	Timestamp int64           `json:"timestamp"`
	Data      json.RawMessage `json:"data,omitempty"`
}

// encodeQueue writes the events in order, one group per run of events that
// share an origin. rs_trail is never written.
func encodeQueue(events []*qevent) []byte {
	file := queueFile{Version: 2, Groups: []queueGroup{}}
	for _, e := range events {
		data := e.saved
		if data == nil {
			data = e.data
		}
		n := len(file.Groups)
		if n == 0 || !bytes.Equal(file.Groups[n-1].Origin, e.origin) {
			file.Groups = append(file.Groups, queueGroup{Origin: e.origin})
			n++
		}
		file.Groups[n-1].Events = append(file.Groups[n-1].Events, queueEntry{Event: e.name, Timestamp: e.timestamp, Data: data})
	}
	raw, _ := marshalJSON(file)
	return raw
}

// decodeQueue reads a version 1 or 2 file. A malformed or unknown file is
// an empty queue. Events older than 89 days are dropped, then the oldest
// past limit; both are counted.
func decodeQueue(raw []byte, now time.Time, limit int) (events []*qevent, tooOld, overflow int) {
	var file queueFile
	if json.Unmarshal(raw, &file) != nil || (file.Version != 1 && file.Version != 2) {
		return nil, 0, 0
	}
	oldest := now.Add(-maxEventAge).UnixMilli()
	for _, g := range file.Groups {
		origin := g.Origin
		if file.Version == 1 || !isJSONObject(origin) {
			origin = nil
		}
		session := originSession(origin)
		for _, entry := range g.Events {
			if entry.Event == "" || !isJSONObject(entry.Data) {
				continue
			}
			if entry.Timestamp < oldest {
				tooOld++
				continue
			}
			var probe struct {
				Kind   string `json:"rs_kind"`
				Source string `json:"rs_source"`
				Trail  any    `json:"rs_trail"`
			}
			_ = json.Unmarshal(entry.Data, &probe)
			data := entry.Data
			if probe.Trail != nil {
				data = stripKey(data, "rs_trail")
			}
			events = append(events, &qevent{
				name: entry.Event, timestamp: entry.Timestamp, data: data,
				origin: origin, session: session,
				capture: probe.Kind == "error" && automaticSources[probe.Source],
			})
		}
	}
	if len(events) > limit {
		overflow = len(events) - limit
		events = events[overflow:]
	}
	return events, tooOld, overflow
}

func isJSONObject(raw json.RawMessage) bool {
	raw = bytes.TrimSpace(raw)
	return len(raw) >= 2 && raw[0] == '{'
}

// originSession reads origin.session; the origin itself is opaque.
func originSession(origin json.RawMessage) string {
	if origin == nil {
		return ""
	}
	var o struct {
		Session string `json:"session"`
	}
	_ = json.Unmarshal(origin, &o)
	return o.Session
}

// stripKey returns the object without key.
func stripKey(data json.RawMessage, key string) json.RawMessage {
	var m map[string]json.RawMessage
	if json.Unmarshal(data, &m) != nil {
		return data
	}
	delete(m, key)
	out, err := marshalJSON(m)
	if err != nil {
		return data
	}
	return out
}

// marshalJSON is json.Marshal without HTML escaping, so sizes match what
// the server measures.
func marshalJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// errorTextLimits are an error event's text fields and their cuts. Within
// its cut each counts one byte a character toward the 8 KB ceiling, as the
// server counts it, so escaping never refuses text the errors contract
// allows.
var errorTextLimits = map[string]int{"rs_message": 200, "rs_stack": 2000, "rs_trail": 1500}

// countedBytes is an event's data size as the server counts it. kind is
// the data's rs_kind and texts its string values by key.
func countedBytes(kind string, texts map[string]string, encoded []byte) int {
	size := len(encoded)
	if kind != "error" {
		return size
	}
	for key, cut := range errorTextLimits {
		text, ok := texts[key]
		if !ok {
			continue
		}
		units := utf16Len(text)
		if units > cut {
			continue
		}
		if quoted, err := marshalJSON(text); err == nil {
			size -= len(quoted) - units
		}
	}
	return size
}
