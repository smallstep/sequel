package sequel

import (
	"errors"
	"slices"
	"testing"

	"github.com/go-sqlx/sqlx"
)

func Test_readReplicas_add(t *testing.T) {
	tests := []struct {
		name string
		rrs  []*sqlx.DB
	}{
		{
			name: "add single",
			rrs:  []*sqlx.DB{sqlx.NewDb(nil, "fakeDriver")},
		},
		{
			name: "add multiple",
			rrs:  []*sqlx.DB{sqlx.NewDb(nil, "fakeDriver"), sqlx.NewDb(nil, "fakeDriver"), sqlx.NewDb(nil, "fakeDriver")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create readReplicas
			rr := &readReplicas{}

			// Add connections to read replicas
			for _, c := range tt.rrs {
				rr.add(c)
			}

			// Confirm they are all added
			if !rrContains(t, rr, tt.rrs) {
				t.Fatal("readReplicas does not contain all added conns")
			}
		})
	}
}

func Test_readReplicas_next(t *testing.T) {
	newRRs := func(rrs ...*sqlx.DB) *readReplicas {
		rr := &readReplicas{}
		for _, c := range rrs {
			rr.add(c)
		}

		return rr
	}

	c1, c2, c3 := sqlx.NewDb(nil, "fakeDriver"), sqlx.NewDb(nil, "fakeDriver"), sqlx.NewDb(nil, "fakeDriver")

	emptyRR := newRRs()
	singleRR := newRRs(c1)
	threeRR := newRRs(c1, c2, c3)

	tests := []struct {
		name    string
		rr      *readReplicas
		want    *sqlx.DB
		wantErr error
	}{
		{
			name:    "empty readReplicas",
			rr:      emptyRR,
			want:    nil,
			wantErr: ErrNoReadReplicaConnection,
		},
		{
			name:    "one read replica",
			rr:      singleRR,
			want:    c1,
			wantErr: nil,
		},
		{
			name:    "one read replica (2 calls)",
			rr:      singleRR,
			want:    c1,
			wantErr: nil,
		},
		{
			name:    "one read replica (3 calls)",
			rr:      singleRR,
			want:    c1,
			wantErr: nil,
		},
		{
			name:    "three read replicas",
			rr:      threeRR,
			want:    c1,
			wantErr: nil,
		},
		{
			name:    "three read replicas (2 calls)",
			rr:      threeRR,
			want:    c3,
			wantErr: nil,
		},
		{
			name:    "three read replicas (3 calls)",
			rr:      threeRR,
			want:    c2,
			wantErr: nil,
		},
		{
			name:    "three read replicas (4 calls)",
			rr:      threeRR,
			want:    c1,
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.rr.next()
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("got err %v, expecting %v", err, tt.wantErr)
			}

			if got != tt.want {
				t.Fatalf("got rr %p, want %p", got, tt.want)
			}
		})
	}
}

// rrContains is a test helper to see if a [readReplicas] contains all the connections passed in
func rrContains(t *testing.T, rr *readReplicas, conns []*sqlx.DB) bool {
	t.Helper()

	var findCnt int
	head := rr.current
	for c := head.next; ; c = c.next {
		if slices.Contains(conns, c.db) {
			findCnt++
		}

		if c == head {
			break
		}
	}

	return findCnt == len(conns)
}
