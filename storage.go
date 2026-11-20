package ravensight

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash/fnv"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// store keeps the SDK's small files in one directory. An empty dir keeps
// nothing: every read misses and every write is skipped.
type store struct {
	dir string
}

const (
	deviceIDFile = "device_id"
	stateFile    = "state.json"
	identityFile = "identity.json"
	configFile   = "config.json"
	crashPrefix  = "crash-"
)

func (s store) enabled() bool { return s.dir != "" }

func (s store) read(name string) ([]byte, error) {
	if !s.enabled() {
		return nil, fs.ErrNotExist
	}
	return os.ReadFile(filepath.Join(s.dir, name))
}

// write replaces name atomically: a temporary file in the same directory,
// then a rename over the old one, so a partial file never replaces a good
// one. Files are readable by the player's account only.
func (s store) write(name string, data []byte) error {
	if !s.enabled() {
		return nil
	}
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(s.dir, name+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	_, werr := tmp.Write(data)
	if werr == nil {
		werr = tmp.Sync()
	}
	if cerr := tmp.Close(); werr == nil {
		werr = cerr
	}
	if werr == nil {
		werr = os.Rename(tmpName, filepath.Join(s.dir, name))
	}
	if werr != nil {
		_ = os.Remove(tmpName)
	}
	return werr
}

func (s store) remove(name string) {
	if s.enabled() {
		_ = os.Remove(filepath.Join(s.dir, name))
	}
}

// removeTemps deletes leftover temporary copies of name from a run that
// died mid-write.
func (s store) removeTemps(name string) {
	if !s.enabled() {
		return
	}
	matches, _ := filepath.Glob(filepath.Join(s.dir, name+".tmp-*"))
	for _, m := range matches {
		_ = os.Remove(m)
	}
}

// list returns the names in the directory that start with prefix.
func (s store) list(prefix string) []string {
	if !s.enabled() {
		return nil
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), prefix) && !strings.Contains(e.Name(), ".tmp-") {
			out = append(out, e.Name())
		}
	}
	return out
}

// savedState is the player's choice, kept across launches.
type savedState struct {
	OptedOut bool `json:"optedOut,omitempty"`
	OptedIn  bool `json:"optedIn,omitempty"`
}

func (s store) loadState() savedState {
	var st savedState
	if raw, err := s.read(stateFile); err == nil {
		_ = json.Unmarshal(raw, &st)
	}
	return st
}

func (s store) saveState(st savedState) error {
	raw, _ := json.Marshal(st)
	return s.write(stateFile, raw)
}

// savedIdentity is identify's stored link: the game's salt and the account
// hash made with it. Never the raw account id.
type savedIdentity struct {
	Salt        string `json:"salt"`
	AccountHash string `json:"accountHash"`
}

func (s store) loadIdentity() savedIdentity {
	var id savedIdentity
	if raw, err := s.read(identityFile); err == nil {
		if json.Unmarshal(raw, &id) != nil || !isLowerHex(id.AccountHash, 64) || id.Salt == "" {
			id = savedIdentity{}
		}
	}
	return id
}

func (s store) saveIdentity(id savedIdentity) error {
	if id.AccountHash == "" {
		s.remove(identityFile)
		return nil
	}
	raw, _ := json.Marshal(id)
	return s.write(identityFile, raw)
}

// queueFileName is queue-<16 hex>.json, the hex being 64-bit FNV-1a of the
// ingest key. The hash only keeps two keys apart; it is not a secret.
func queueFileName(ingestKey string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(ingestKey))
	return "queue-" + hex.EncodeToString(h.Sum(nil)) + ".json"
}

// newDeviceID is "dev_" and 32 hex characters from the system's random
// source.
func newDeviceID() string {
	return "dev_" + randomHex(16)
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand does not fail on supported platforms.
		panic(errors.New("ravensight: no system random source"))
	}
	return hex.EncodeToString(b)
}

func isLowerHex(text string, length int) bool {
	if len(text) != length {
		return false
	}
	for i := 0; i < len(text); i++ {
		c := text[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
