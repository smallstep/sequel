package clock

import "time"

type Clock interface {
	Now() time.Time
	Backdate() time.Time
}

type clock struct{}

// New creates a new clock.
func New() Clock {
	return &clock{}
}

// Now returns the current time in UTC.
func (c *clock) Now() time.Time {
	return time.Now().UTC()
}

// Backdate returns now - 1m,
func (c *clock) Backdate() time.Time {
	return time.Now().UTC().Add(-time.Minute)
}

type mock struct {
	t time.Time
}

// NewMock returns a mock implementation of the clock.
func NewMock(t time.Time) Clock { return &mock{t: t} }

// Now returns the mocked time.
func (m *mock) Now() time.Time { return m.t }

// Now returns the mocked time - 1m.
func (m *mock) Backdate() time.Time { return m.t.Add(-time.Minute) }
