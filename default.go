package ravensight

import "sync/atomic"

// A small package-level convenience for games that want one client and no
// plumbing. Everything here works on the client Init made; a Client value
// of your own needs none of it.

var defaultClient atomic.Pointer[Client]

// Init makes a client with New and keeps it as the package default, so
// Track, Default and Close reach it from anywhere. Calling Init again
// replaces the default; close the old client yourself.
func Init(cfg Config) *Client {
	c := New(cfg)
	defaultClient.Store(c)
	return c
}

// Default is the client Init made, or nil. Every Client method is safe to
// call on nil and does nothing.
func Default() *Client { return defaultClient.Load() }

// Track queues an event on the default client. See Client.Track.
func Track(name string, data map[string]any) bool { return Default().Track(name, data) }

// Close closes the default client. See Client.Close.
func Close() error { return Default().Close() }
