package ravensight

import (
	"context"
	"slices"
	"strings"
)

// FeedbackCategories are the categories SubmitFeedback accepts.
var FeedbackCategories = []string{"bug", "suggestion", "complaint", "praise", "playtest", "other"}

// Feedback is a player's message to the studio.
type Feedback struct {
	Message  string // required, at most 2,000 characters
	Category string // optional: one of FeedbackCategories
	Rating   int    // 0 leaves it out, or 1 to 5
}

type feedbackJob struct {
	body     []byte
	done     chan error // buffered: the worker never waits on a caller
	reopened bool
}

// SubmitFeedback sends player feedback and waits for the answer or ctx.
// Bad input is refused on the spot with no request: "empty_message",
// "message_too_long", "invalid_category" or "invalid_rating". While the
// client is not sending it returns the status code, such as "disabled". A
// server refusal comes back as a *RefusalError with the server's code.
// Feedback is never retried on its own; call again if you want to.
func (c *Client) SubmitFeedback(ctx context.Context, f Feedback) error {
	if c == nil || c.inert {
		return refused(string(StatusNoKey))
	}
	switch {
	case strings.TrimSpace(f.Message) == "":
		return refused("empty_message")
	case utf16Len(f.Message) > maxFeedbackUnits:
		return refused("message_too_long")
	case f.Category != "" && !slices.Contains(FeedbackCategories, f.Category):
		return refused("invalid_category")
	case f.Rating < 0 || f.Rating > 5:
		return refused("invalid_rating")
	}
	body := map[string]any{"message": f.Message}
	if f.Category != "" {
		body["category"] = f.Category
	}
	if f.Rating > 0 {
		body["rating"] = f.Rating
	}
	raw, _ := marshalJSON(body)
	job := &feedbackJob{body: raw, done: make(chan error, 1)}

	c.mu.Lock()
	if !c.activeLocked() || c.closing {
		s := c.statusLocked()
		c.unlock()
		if s == StatusOK {
			s = "closed"
		}
		return refused(string(s))
	}
	c.jobs = append(c.jobs, job)
	c.wakeWorker()
	c.unlock()

	select {
	case err := <-job.done:
		return err
	case <-ctx.Done():
		c.mu.Lock()
		if i := slices.Index(c.jobs, job); i >= 0 {
			c.jobs = slices.Delete(c.jobs, i, i+1)
		}
		c.unlock()
		return ctx.Err()
	}
}

func (c *Client) sendFeedback() bool {
	c.mu.Lock()
	if len(c.jobs) == 0 {
		c.unlock()
		return true
	}
	job := c.jobs[0]
	c.jobs = c.jobs[1:]
	token := c.sess.token
	c.unlock()
	a := c.request("POST", "/feedback", job.body, map[string]string{"X-Session-Token": token})

	c.mu.Lock()
	defer c.unlock()
	switch {
	case a.err != nil:
		job.done <- refused("offline")
		return false
	case a.status >= 200 && a.status < 300:
		job.done <- nil
	case a.status == 401 && !job.reopened:
		// The session expired: open a new one once and send again.
		job.reopened = true
		c.sess = session{}
		c.jobs = append([]*feedbackJob{job}, c.jobs...)
	default:
		code := a.code
		if code == "" {
			code = "http_error"
		}
		job.done <- &RefusalError{Code: code, Status: a.status}
	}
	return true
}

// failJobsLocked answers every waiting feedback call with code.
func (c *Client) failJobsLocked(code string) {
	for _, job := range c.jobs {
		job.done <- refused(code)
	}
	c.jobs = nil
}
