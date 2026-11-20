package ravensight

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"strings"
)

// identifyMaxBytes is the longest account id accepted, in UTF-8 bytes.
const identifyMaxBytes = 256

type identifyState struct {
	stored            savedIdentity // the salt and hash kept beside the device id
	held              string        // a raw id, in memory only, until it may be hashed and stored
	wantSession       bool          // open a session for a waiting identity
	launchSalt        string        // this launch's salt, from the session answer
	saltMissing       bool          // the answer had no salt: unavailable this launch
	stopped           bool          // 404 or invalid_account_hash: not sent again this launch
	identifiedSession string        // the session token identify was last sent under
	noSessionStreak   int
	noSessionStopped  bool
	generation        uint64 // bumped by every forget, so a late answer changes nothing
}

// identifyRefusal is the reason an account id is refused, or "".
func identifyRefusal(accountID string) string {
	switch {
	case accountID == "":
		return "empty"
	case len(accountID) > identifyMaxBytes:
		return "too_long"
	case strings.ContainsRune(accountID, '@'):
		return "looks_like_email"
	}
	return ""
}

// accountHash is lowercase hex HMAC-SHA256 keyed with the salt string
// exactly as the session answer gave it (not decoded), over the account id.
func accountHash(accountID, salt string) string {
	mac := hmac.New(sha256.New, []byte(salt))
	mac.Write([]byte(accountID))
	return hex.EncodeToString(mac.Sum(nil))
}

// Identify links this device to your game's own account, so one person on
// two devices, or before and after signing in, counts as one player. Call
// it after the player signs in. Only a salted hash of the id is sent (the
// game's salt arrives with the session), once per session; the raw id is
// never sent or saved. Pass a random, high-entropy id, such as a random
// UUID stored with the account, never a sequential key, an email, a name
// or a handle.
//
// It returns a *RefusalError for an empty id ("empty"), one over 256 bytes
// of UTF-8 ("too_long") or one containing '@' ("looks_like_email"); a
// refusal sends nothing and changes nothing. The same account again does
// nothing; a different account first runs ResetIdentity. Without a key,
// before consent or while switched off, the id is held in memory only.
func (c *Client) Identify(accountID string) error {
	if c == nil {
		return refused("no_key")
	}
	if reason := identifyRefusal(accountID); reason != "" {
		c.mu.Lock()
		c.logLocked(slog.LevelWarn, "Ravensight: identify refused ("+reason+"): pass an opaque account id, 1 to 256 bytes, with no @. Nothing was sent or changed.")
		c.unlock()
		return refused(reason)
	}
	c.mu.Lock()
	if c.inert || c.isPlaytest {
		c.id.held = accountID // memory only, never sent
		c.unlock()
		return nil
	}
	sameHeld := c.id.held != "" && c.id.held == accountID
	sameStored := c.id.stored.AccountHash != "" && accountHash(accountID, c.id.stored.Salt) == c.id.stored.AccountHash
	if sameHeld || (sameStored && c.id.launchSalt != "") {
		c.unlock()
		return nil
	}
	switchAccount := !sameStored && (c.id.stored.AccountHash != "" || c.id.held != "")
	c.unlock()
	if switchAccount {
		c.ResetIdentity() // a different account: a fresh device id first
	}
	c.mu.Lock()
	defer c.unlock()
	if c.id.saltMissing {
		c.logLocked(slog.LevelInfo, "Ravensight: identify is not offered by this server; nothing is kept this launch.")
		return nil
	}
	if c.identifyCanStoreLocked() && c.id.launchSalt != "" {
		c.identifyStoreLocked(accountHash(accountID, c.id.launchSalt))
	} else {
		c.id.held = accountID
	}
	c.id.wantSession = true
	c.wakeWorker()
	return nil
}

// ResetIdentity is for sign-out: it forgets the stored identity and any id
// held in memory, then switches to a fresh device id as ResetDeviceID does,
// so the next session is not linked to the account. With no identity it
// does nothing. It sends nothing.
func (c *Client) ResetIdentity() {
	if c == nil {
		return
	}
	c.mu.Lock()
	had := c.identifyForgetLocked()
	c.unlock()
	if had && !c.inert && !c.isPlaytest {
		c.ResetDeviceID()
	}
}

// IsIdentified reports whether an identity is stored or an id is held. The
// id itself is never readable back.
func (c *Client) IsIdentified() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.id.stored.AccountHash != "" || c.id.held != ""
}

func (c *Client) identifyCanStoreLocked() bool {
	return !c.inert && !c.isPlaytest && c.enabled && c.consentLocked() && !c.closed
}

func (c *Client) identifyStoreLocked(hash string) {
	c.id.stored = savedIdentity{Salt: c.id.launchSalt, AccountHash: hash}
	c.saveIdentityLater()
}

// identifyForgetLocked drops the stored identity and any held id, and
// reports whether there was one.
func (c *Client) identifyForgetLocked() bool {
	had := c.id.stored.AccountHash != "" || c.id.held != ""
	if c.id.stored.AccountHash != "" {
		c.id.stored = savedIdentity{}
		c.saveIdentityLater()
	}
	c.id.held = ""
	c.id.identifiedSession = ""
	c.id.noSessionStreak = 0
	c.id.noSessionStopped = false
	c.id.wantSession = false
	c.id.generation++
	return had
}

// identifySessionAnswerLocked learns the salt from a session answer and
// drops what it rules out.
func (c *Client) identifySessionAnswerLocked(salt string) {
	if c.id.saltMissing {
		return // unavailable until the next launch
	}
	if salt == "" {
		c.id.saltMissing = true
		c.id.held = ""
		return
	}
	c.id.launchSalt = salt
	if c.id.stored.AccountHash != "" && c.id.stored.Salt != salt {
		// Made under another game's salt: deleted, never sent.
		c.id.stored = savedIdentity{}
		c.saveIdentityLater()
	}
	if c.id.held != "" && c.identifyCanStoreLocked() {
		c.identifyStoreLocked(accountHash(c.id.held, salt))
		c.id.held = ""
	}
	c.id.wantSession = false
}

func (c *Client) identifyPendingLocked() bool {
	return !c.inert && !c.isPlaytest && c.activeLocked() && !c.id.stopped && !c.id.noSessionStopped &&
		!c.id.saltMissing && (c.id.held != "" || c.id.stored.AccountHash != "")
}

func (c *Client) identifyWantsSessionLocked() bool {
	return c.id.wantSession && c.identifyPendingLocked() && !c.sessionValidLocked(c.now())
}

func (c *Client) identifyReadyLocked() bool {
	return c.identifyPendingLocked() && c.sessionValidLocked(c.now()) && c.id.launchSalt != "" &&
		c.id.stored.AccountHash != "" && c.id.identifiedSession != c.sess.token
}

// sendIdentify sends the stored hash once for the current session. It never
// touches the event queue, and its failures never back the queue off.
func (c *Client) sendIdentify() bool {
	c.mu.Lock()
	token, hash, gen := c.sess.token, c.id.stored.AccountHash, c.id.generation
	c.id.identifiedSession = token // whatever the answer, not again this session
	c.unlock()
	body, _ := marshalJSON(map[string]string{"accountHash": hash})
	a := c.request("POST", "/identify", body, map[string]string{"X-API-Key": c.cfg.IngestKey, "X-Session-Token": token})

	c.mu.Lock()
	defer c.unlock()
	if gen != c.id.generation {
		return true // a sign-out happened meanwhile
	}
	if a.err == nil && a.status == 400 && a.code == "no_session" {
		c.id.noSessionStreak++
		if c.id.noSessionStreak >= 2 {
			c.id.noSessionStopped = true
			c.logLocked(slog.LevelInfo, "Ravensight: identify answered no_session twice in a row; not sent again until ResetIdentity.")
		}
		return true
	}
	c.id.noSessionStreak = 0
	switch {
	case a.err == nil && a.status >= 200 && a.status < 300:
		c.logLocked(slog.LevelInfo, "Ravensight: identified this session.")
	case a.err == nil && a.status == 404:
		c.id.stopped = true
		c.logLocked(slog.LevelInfo, "Ravensight: identify is not offered by this server; not sent again this launch.")
	case a.err == nil && a.status == 400 && a.code == "invalid_account_hash":
		c.id.stopped = true
		c.warnOnceLocked("identify_hash", "Ravensight: identify was refused as invalid_account_hash; not sent again this launch.")
	default:
		code := a.code
		if code == "" {
			code = "offline"
		}
		c.logLocked(slog.LevelInfo, "Ravensight: identify not accepted ("+code+"); tried again next session.")
	}
	return true
}

// SetCampaignLink passes on a Ravensight campaign link the game was opened
// with: the rs_link value of the address (24 hex characters, any case). It
// goes lowercased, as campaignLink, on the next session the client opens
// for its own player, and is cleared once a session answers with a token.
// Later sessions carry none. It is held in memory only and never sent in a
// playtest run, without consent or without a key. Anything that is not a
// link id is refused with "invalid_link" and changes nothing.
func (c *Client) SetCampaignLink(linkID string) error {
	if c == nil {
		return refused("no_key")
	}
	if !isHex(linkID, 24) {
		c.mu.Lock()
		c.logLocked(slog.LevelWarn, "Ravensight: SetCampaignLink refused (invalid_link): pass the 24 hex character rs_link value.")
		c.unlock()
		return refused("invalid_link")
	}
	c.mu.Lock()
	c.campaignLink = strings.ToLower(linkID)
	c.unlock()
	return nil
}

func isHex(text string, length int) bool {
	return isLowerHex(strings.ToLower(text), length)
}
