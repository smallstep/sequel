package clock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name string
		want *clock
	}{
		{"ok", &clock{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, New())
		})
	}
}

func Test_clock_Now(t *testing.T) {
	tests := []struct {
		name         string
		c            *clock
		want         time.Time
		wantLocation *time.Location
	}{
		{"ok", &clock{}, time.Now().UTC(), time.UTC},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &clock{}
			got := c.Now()
			assert.InDelta(t, tt.want.Unix(), got.Unix(), 1)
			assert.Equal(t, tt.wantLocation, got.Location())
		})
	}
}

func Test_clock_Backdate(t *testing.T) {
	tests := []struct {
		name         string
		c            *clock
		want         time.Time
		wantLocation *time.Location
	}{
		{"ok", &clock{}, time.Now().UTC().Add(-time.Minute), time.UTC},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &clock{}
			got := c.Backdate()
			assert.InDelta(t, tt.want.Unix(), got.Unix(), 1)
			assert.Equal(t, tt.wantLocation, got.Location())
		})
	}
}

func TestMock(t *testing.T) {
	t0 := time.Now()
	m := NewMock(t0)
	assert.Equal(t, t0, m.Now())
	assert.Equal(t, t0.Add(-time.Minute), m.Backdate())
}
