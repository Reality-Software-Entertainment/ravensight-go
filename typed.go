package ravensight

import (
	"strings"
)

// Typed events carry a few reserved rs_ properties that the dashboard's
// economy, progression and health pages read. Each helper calls Track, so
// queueing, retries and consent behave the same, and returns what Track
// returns. A value the server cannot read is sent as given and flagged on
// the Implementation health card instead of being refused here.

// Purchase is a real-money purchase. Amount is whole minor units: 499 is
// 4.99.
type Purchase struct {
	Amount        int64
	Currency      string // three letters; sent upper-cased
	ItemType      string
	ItemID        string
	Cart          string         // optional
	TransactionID string         // optional
	Extra         map[string]any // your own properties; reserved keys win
	Name          string         // the event name; default "purchase"
}

// Resource is an in-game currency or resource gained or spent.
type Resource struct {
	Flow     string // "source" or "sink"
	Resource string
	Amount   float64
	ItemType string // optional
	ItemID   string // optional
	Extra    map[string]any
	Name     string // default "resource_source" or "resource_sink"
}

// Progression is a start, a completion or a failure of a level or stage.
type Progression struct {
	Status string // "start", "complete" or "fail"
	P1     string
	P2     string   // optional
	P3     string   // optional
	Score  *float64 // optional
	Extra  map[string]any
	Name   string // default "progression_<status>"
}

// ErrorReport is an error your game reports itself. It is sent as written
// (the server scrubs it), never held back by the error capture switches,
// and counts as gameplay in the queue.
type ErrorReport struct {
	Severity string // debug, info, warning, error or critical
	Message  string // cut to 200 characters
	Stack    string // optional, cut to 2,000 characters
	Type     string // optional, the error's type, cut to 100 characters
	Extra    map[string]any
	Name     string // default "error"
}

func merged(extra map[string]any, reserved map[string]any) map[string]any {
	out := make(map[string]any, len(extra)+len(reserved))
	for k, v := range extra {
		out[k] = v
	}
	for k, v := range reserved {
		out[k] = v
	}
	return out
}

func orDefault(name, fallback string) string {
	if name == "" {
		return fallback
	}
	return name
}

// TrackPurchase sends a typed purchase event.
func (c *Client) TrackPurchase(p Purchase) bool {
	reserved := map[string]any{
		"rs_kind":      "purchase",
		"rs_amount":    p.Amount,
		"rs_currency":  strings.ToUpper(p.Currency),
		"rs_item_type": p.ItemType,
		"rs_item_id":   p.ItemID,
	}
	if p.Cart != "" {
		reserved["rs_cart"] = p.Cart
	}
	if p.TransactionID != "" {
		reserved["rs_transaction_id"] = p.TransactionID
	}
	return c.Track(orDefault(p.Name, "purchase"), merged(p.Extra, reserved))
}

// TrackResource sends a typed resource source or sink event.
func (c *Client) TrackResource(r Resource) bool {
	reserved := map[string]any{
		"rs_kind":     "resource",
		"rs_flow":     r.Flow,
		"rs_resource": r.Resource,
		"rs_amount":   r.Amount,
	}
	if r.ItemType != "" {
		reserved["rs_item_type"] = r.ItemType
	}
	if r.ItemID != "" {
		reserved["rs_item_id"] = r.ItemID
	}
	return c.Track(orDefault(r.Name, "resource_"+r.Flow), merged(r.Extra, reserved))
}

// TrackProgression sends a typed progression event.
func (c *Client) TrackProgression(p Progression) bool {
	reserved := map[string]any{"rs_kind": "progression", "rs_status": p.Status, "rs_p1": p.P1}
	if p.P2 != "" {
		reserved["rs_p2"] = p.P2
	}
	if p.P3 != "" {
		reserved["rs_p3"] = p.P3
	}
	if p.Score != nil {
		reserved["rs_score"] = *p.Score
	}
	return c.Track(orDefault(p.Name, "progression_"+p.Status), merged(p.Extra, reserved))
}

// TrackError sends an error your game reports itself, with rs_kind "error".
func (c *Client) TrackError(e ErrorReport) bool {
	reserved := map[string]any{
		"rs_kind":     "error",
		"rs_severity": e.Severity,
		"rs_message":  cutUnits(e.Message, messageUnits),
	}
	if e.Stack != "" {
		reserved["rs_stack"] = cutUnits(e.Stack, stackUnits)
	}
	if e.Type != "" {
		reserved["rs_error_type"] = cutUnits(e.Type, typeUnits)
	}
	return c.Track(orDefault(e.Name, "error"), merged(e.Extra, reserved))
}
