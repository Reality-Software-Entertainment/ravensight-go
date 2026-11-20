package ravensight

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

// The device scrub of automatic error text. It follows the shared rules every
// Ravensight SDK and the server use, and testdata/scrub-vectors.json is the
// binding set of cases. Go's regexp has no lookbehind, so the two rules that
// need one (a URL scheme and an email must start a run) capture the
// character before the match instead and put it back.
var (
	scrubURL       = regexp.MustCompile(`(?i)(^|[^a-z0-9+.-])([a-z][a-z0-9+.-]*://)(?:[^\s/?#@'"<>]*@)?([^\s?#'"<>]*)(?:[?#][^\s'"<>]*)?`)
	scrubBearer    = regexp.MustCompile(`(?i)\bBearer\s+[A-Za-z0-9\-._~+/]+=*`)
	scrubJWT       = regexp.MustCompile(`\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*`)
	scrubKey       = regexp.MustCompile(`\bgt_[A-Za-z0-9_]+`)
	scrubEmail     = regexp.MustCompile(`(^|[^A-Za-z0-9._%+-])[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`)
	scrubHomeUnix  = regexp.MustCompile(`/(?:Users|home)/[^/\\'"<>:\r\n]+`)
	scrubHomeWin   = regexp.MustCompile(`(?i)\b[A-Za-z]:[\\/](?:Users|Documents and Settings)[\\/][^/\\'"<>:\r\n]+`)
	scrubIPv4      = regexp.MustCompile(`\b(?:\d{1,3}\.){3}\d{1,3}\b`)
	scrubIPv6Full  = regexp.MustCompile(`(?i)\b(?:[0-9a-f]{1,4}:){7}[0-9a-f]{1,4}\b`)
	scrubIPv6Short = regexp.MustCompile(`(?i)(?:\b[0-9a-f]{1,4}(?::[0-9a-f]{1,4}){0,6})?::(?:[0-9a-f]{1,4}(?::[0-9a-f]{1,4}){0,6}\b)?`)
	scrubLongRun   = regexp.MustCompile(`[A-Za-z0-9+/=_-]{32,}`)
	pathLower      = regexp.MustCompile(`^[a-z0-9]+$`)
	pathCamel      = regexp.MustCompile(`^[A-Z][a-z0-9]*(?:[A-Z][a-z0-9]+)*$`)
	pathAcronym    = regexp.MustCompile(`^[A-Z0-9]{1,6}$`)
	hasDigit       = regexp.MustCompile(`\d`)
)

// scrub applies the shared rules to text, in the contract's order.
func scrub(text string) string {
	if text == "" {
		return text
	}
	text = scrubURL.ReplaceAllString(text, "$1$2$3")
	text = scrubBearer.ReplaceAllString(text, "[token]")
	text = scrubJWT.ReplaceAllString(text, "[token]")
	text = scrubKey.ReplaceAllString(text, "[token]")
	text = scrubEmail.ReplaceAllString(text, "${1}[email]")
	text = scrubHomeWin.ReplaceAllString(text, "~")
	text = scrubHomeUnix.ReplaceAllString(text, "~")
	text = scrubIPv4.ReplaceAllString(text, "[ip]")
	text = scrubIPv6Full.ReplaceAllString(text, "[ip]")
	text = scrubIPv6Short.ReplaceAllStringFunc(text, func(match string) string {
		if hasDigit.MatchString(match) {
			return "[ip]"
		}
		return match
	})
	return scrubLongRun.ReplaceAllStringFunc(text, func(run string) string {
		if hasDigit.MatchString(run) && !isPathRun(run) {
			return "[token]"
		}
		return run
	})
}

// isPathRun reports whether a long run is a file path: it contains a slash
// and every slash piece is empty or a path word under 32 characters.
func isPathRun(run string) bool {
	if !strings.Contains(run, "/") {
		return false
	}
	for _, piece := range strings.Split(run, "/") {
		if !isPathWord(piece) {
			return false
		}
	}
	return true
}

func isPathWord(piece string) bool {
	if piece == "" {
		return true
	}
	if len(piece) >= 32 {
		return false
	}
	for _, part := range pathJoiner.Split(piece, -1) {
		if part == "" || (!pathLower.MatchString(part) && !pathCamel.MatchString(part) && !pathAcronym.MatchString(part)) {
			return false
		}
	}
	return true
}

var pathJoiner = regexp.MustCompile(`[_-]`)

// scrubField cuts text to four times its limit, scrubs it and cuts it to the
// limit, so a huge line never costs more than a bounded scrub.
func scrubField(text string, limit int) string {
	return cutUnits(scrub(cutUnits(text, 4*limit)), limit)
}

// utf16Len counts text the way the server does: UTF-16 code units, so a
// character outside the Basic Multilingual Plane counts two.
func utf16Len(text string) int {
	n := 0
	for _, r := range text {
		if r >= 0x10000 {
			n += 2
		} else {
			n++
		}
	}
	return n
}

// cutUnits keeps at most limit UTF-16 code units of text, never splitting a
// character.
func cutUnits(text string, limit int) string {
	if limit <= 0 {
		return ""
	}
	if len(text) <= limit {
		return text // every byte is at most one unit
	}
	n := 0
	for i, r := range text {
		w := 1
		if r >= 0x10000 {
			w = 2
		}
		if n+w > limit {
			return text[:i]
		}
		n += w
	}
	return text
}

// validUTF8 replaces invalid bytes so the text survives JSON as written.
func validUTF8(text string) string {
	if utf8.ValidString(text) {
		return text
	}
	return strings.ToValidUTF8(text, "�")
}

var (
	templateUUID   = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)
	templateURL    = regexp.MustCompile(`\b[a-zA-Z][a-zA-Z0-9+.-]*://\S+`)
	templateQuoted = regexp.MustCompile(`"[^"]*"|'[^']*'`)
	templateHex    = regexp.MustCompile(`\b[0-9a-fA-F]{8,}\b`)
	templateNumber = regexp.MustCompile(`\d+(\.\d+)?`)
)

// messageTemplate turns ids, URLs, quoted text and numbers into
// placeholders, for the local dedupe key.
func messageTemplate(message string) string {
	out := templateUUID.ReplaceAllString(message, "<uuid>")
	out = templateURL.ReplaceAllString(out, "<url>")
	out = templateQuoted.ReplaceAllString(out, "<str>")
	out = templateHex.ReplaceAllString(out, "<hex>")
	return templateNumber.ReplaceAllString(out, "<n>")
}
