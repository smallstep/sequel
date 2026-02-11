package sequel

import (
	"errors"
	"sync"

	"github.com/go-sqlx/sqlx"
)

var ErrNoReadReplicaConnection = errors.New("no read replica connections")

type readReplica struct {
	db *sqlx.DB

	next *readReplica
}

// readReplicas contains a set of DB connections. It is intended to give fair round robin access
// through a circular singularly linked list.
//
// Replicas are appended after the current one. The intended use is to build the replica set before
// querying, but all operations are concurrent-safe.
type readReplicas struct {
	m sync.Mutex

	current *readReplica
}

// add adds a DB to the collection of read replicas
func (rr *readReplicas) add(db *sqlx.DB) {
	rr.m.Lock()
	defer rr.m.Unlock()

	r := readReplica{
		db: db,
	}

	// Empty ring, add new DB
	if rr.current == nil {
		r.next = &r

		rr.current = &r

		return
	}

	// Insert new db after current
	n := rr.current.next
	r.next = n
	rr.current.next = &r
}

// next returns the current DB. The current pointer is advanced.
func (rr *readReplicas) next() (*sqlx.DB, error) {
	rr.m.Lock()
	defer rr.m.Unlock()

	if rr.current == nil {
		return nil, ErrNoReadReplicaConnection
	}

	c := rr.current

	rr.current = rr.current.next

	return c.db, nil
}

// Close closes all read replica connections
func (rr *readReplicas) Close() {
	rr.m.Lock()
	defer rr.m.Unlock()

	first := rr.current
	for c := first; c != first; c = c.next {
		c.db.Close()
	}
}
