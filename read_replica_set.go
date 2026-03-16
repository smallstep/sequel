package sequel

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/go-sqlx/sqlx"
)

var ErrNoReadReplicaConnections = errors.New("no read replica connections available")

type readReplica struct {
	db *sqlx.DB

	next *readReplica
}

// ReadReplicaSet contains a set of DB connections. It is intended to give fair round robin access
// through a circular singularly linked list.
//
// Replicas are appended after the current one. The intended use is to build the replica set before
// querying, but all operations are concurrent-safe.
type ReadReplicaSet struct {
	m sync.Mutex

	current *readReplica
}

// add adds a DB to the collection of read replicas
func (rr *ReadReplicaSet) add(db *sqlx.DB) {
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
func (rr *ReadReplicaSet) next() (*sqlx.DB, error) {
	rr.m.Lock()
	defer rr.m.Unlock()

	if rr.current == nil {
		return nil, ErrNoReadReplicaConnections
	}

	c := rr.current

	rr.current = rr.current.next

	return c.db, nil
}

// Close closes all read replica connections
func (rr *ReadReplicaSet) Close() {
	if rr == nil {
		return
	}

	rr.m.Lock()
	defer rr.m.Unlock()

	// If this instance has no replicas, current would be nil
	if rr.current == nil {
		return
	}

	first := rr.current
	for c := first; ; c = c.next {
		c.db.Close()

		if c.next == first {
			break
		}
	}
}

// Query executes a query against a read replica. Queries that are not SELECTs may not work.
// The args are for any placeholder parameters in the query.
func (rr *ReadReplicaSet) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if rr == nil {
		return nil, ErrNoReadReplicaConnections
	}

	db, err := rr.next()
	if err != nil {
		return nil, fmt.Errorf("did not get read replica connection: %w", err)
	}

	return db.QueryContext(ctx, query, args...)
}

// QueryRow executes a query that is expected to return at most one row against a read replica.
// QueryRowContext always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
//
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *Row's Scan scans the first selected row and discards the
// rest.
func (rr *ReadReplicaSet) QueryRow(ctx context.Context, query string, args ...any) (*sql.Row, error) {
	if rr == nil {
		return nil, ErrNoReadReplicaConnections
	}

	db, err := rr.next()
	if err != nil {
		return nil, fmt.Errorf("did not get read replica connection: %w", err)
	}

	return db.QueryRowContext(ctx, query, args...), nil
}

// Get populates the given model for the result of the given select query against a read replica.
func (rr *ReadReplicaSet) Get(ctx context.Context, dest Model, query string, args ...any) error {
	if rr == nil {
		return ErrNoReadReplicaConnections
	}

	db, err := rr.next()
	if err != nil {
		return fmt.Errorf("did not get read replica connection: %w", err)
	}

	return db.GetContext(ctx, dest, query, args...)
}

// GetAll populates the given destination with all the results of the given
// select query (from a read replica). The method will fail if the destination is not a pointer to a
// slice.
func (rr *ReadReplicaSet) GetAll(ctx context.Context, dest any, query string, args ...any) error {
	if rr == nil {
		return ErrNoReadReplicaConnections
	}

	db, err := rr.next()
	if err != nil {
		return fmt.Errorf("did not get read replica connection: %w", err)
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}

	defer rows.Close()

	if err := rows.Err(); err != nil {
		return err
	}
	return sqlx.StructScan(rows, dest)
}
