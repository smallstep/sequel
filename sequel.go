package sequel

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/go-sqlx/sqlx"
	"github.com/jackc/pgx/v5/pgconn"

	// use pgx/v5 driver
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.step.sm/sequel/clock"
)

// MaxOpenConnections is the maximum number of open connections. If we reach
// this value, the requests will wait until one connection is free.
const MaxOpenConnections = 100

// DB is the type that holds the database client and adds support for database
// operations on a Model.
type DB struct {
	db    *sqlx.DB
	clock clock.Clock
}

type options struct {
	Clock clock.Clock
}

// Option is the type of options that can be used to modify the database. This
// can be useful for testing purposes.
type Option func(*options)

// WithClock sets a custom clock to the database.
func WithClock(c clock.Clock) Option {
	return func(o *options) {
		o.Clock = c
	}
}

// New creates a new DB. It will fail if it cannot ping it.
func New(dataSourceName string, opts ...Option) (*DB, error) {
	options := &options{
		Clock: clock.New(),
	}
	for _, fn := range opts {
		fn(options)
	}

	// Connect opens the database and verifies with a ping
	db, err := sqlx.Connect("pgx/v5", dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("error connecting to the database: %w", err)
	}
	db.SetMaxOpenConns(MaxOpenConnections)
	return &DB{
		db:    db,
		clock: options.Clock,
	}, nil
}

type dbKey struct{}

// NewContext returns a new context with the given DB.
func NewContext(ctx context.Context, db *DB) context.Context {
	return context.WithValue(ctx, dbKey{}, db)
}

// FromContext returns the DB associated with this context.
func FromContext(ctx context.Context) (db *DB, ok bool) {
	db, ok = ctx.Value(dbKey{}).(*DB)
	return
}

// Context returns the default database context with a 15s timeout.
func Context(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, 15*time.Second)
}

// IsErrNotFound returns true if the given error is equal to sql.ErrNoRows
func IsErrNotFound(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}

// IsUniqueViolation returns true if the given error is equal to the postgres
// unique violation error (23505).
func IsUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505"
	}
	return false
}

// RowsAffected checks that the numbers of rows affected matches the given one,
// if not it will return an error.
func RowsAffected(res sql.Result, n int64) error {
	got, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if got == n {
		return nil
	}
	if got == 0 {
		return sql.ErrNoRows
	}
	return fmt.Errorf("unexpected number of rows: got %d, want %d", got, n)
}

// Close closes the database and prevents new queries from starting. Close then
// waits for all queries that have started processing on the server to finish.
func (d *DB) Close() error {
	return d.db.Close()
}

// Query executes a query that returns rows, typically a SELECT. The args are
// for any placeholder parameters in the query.
func (d *DB) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return d.db.QueryContext(ctx, query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRowContext always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
//
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *Row's Scan scans the first selected row and discards the
// rest.
func (d *DB) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return d.db.QueryRowContext(ctx, query, args...)
}

// Exec executes a query without returning any rows. The args are for any
// placeholder parameters in the query.
func (d *DB) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return d.db.ExecContext(ctx, query, args...)
}

// Get populates the given model for the result of the given select query.
func (d *DB) Get(ctx context.Context, dest Model, query string, args ...any) error {
	return d.db.GetContext(ctx, dest, query, args...)
}

// GetAll populates the given destination with all the results of the given
// select query. The method will fail if the destination is not a pointer to a
// slice.
func (d *DB) GetAll(ctx context.Context, dest any, query string, args ...any) error {
	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return sqlx.StructScan(rows, dest)
}

// Select populates the given model with the result of a select by id query.
func (d *DB) Select(ctx context.Context, dest Model, id string) error {
	return d.db.GetContext(ctx, dest, dest.Select(), id)
}

// Insert inserts the given model in the database.
func (d *DB) Insert(ctx context.Context, arg Model) error {
	var id string
	t0 := d.clock.Now()
	arg.SetCreatedAt(t0)
	arg.SetUpdatedAt(t0)

	query, qargs, err := d.db.BindNamed(arg.Insert(), arg)
	if err != nil {
		return err
	}

	// Do insert using an exec if necessary.
	if _, ok := arg.(ModelWithExecInsert); ok {
		return d.insertWithExec(ctx, query, qargs...)
	}

	row := d.db.QueryRowContext(ctx, query, qargs...)
	if err := row.Scan(&id); err != nil {
		return err
	}
	arg.SetID(id)
	return nil
}

func (d *DB) insertWithExec(ctx context.Context, query string, args ...any) error {
	r, err := d.db.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	return RowsAffected(r, 1)
}

// InsertBatch inserts the given modules in a database using a transaction.
func (d *DB) InsertBatch(ctx context.Context, args []Model) error {
	t0 := d.clock.Now()

	tx, err := d.db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var id string
	for _, a := range args {
		a.SetCreatedAt(t0)
		a.SetUpdatedAt(t0)
		query, qargs, err := tx.BindNamed(a.Insert(), a)
		if err != nil {
			return err
		}
		if _, ok := a.(ModelWithExecInsert); ok {
			r, err := tx.Exec(query, qargs...)
			if err != nil {
				return err
			}
			if err := RowsAffected(r, 1); err != nil {
				return err
			}
		} else {
			row := tx.QueryRow(query, qargs...)
			if err := row.Scan(&id); err != nil {
				return err
			}
			a.SetID(id)
		}
	}

	return tx.Commit()
}

// Update updates the given model in the datastore.
func (d *DB) Update(ctx context.Context, arg Model) error {
	arg.SetUpdatedAt(d.clock.Now())
	query, qargs, err := d.db.BindNamed(arg.Update(), arg)
	if err != nil {
		return err
	}
	r, err := d.db.ExecContext(ctx, query, qargs...)
	if err != nil {
		return err
	}
	return RowsAffected(r, 1)
}

// Delete soft-deletes the given model in the database setting the deleted_at
// column to the current date.
func (d *DB) Delete(ctx context.Context, arg Model) error {
	t0 := d.clock.Now()
	r, err := d.db.ExecContext(ctx, arg.Delete(), t0, arg.GetID())
	if err != nil {
		return err
	}
	if err := RowsAffected(r, 1); err != nil {
		return err
	}

	arg.SetDeletedAt(t0)
	return nil
}

// HardDelete deletes the given model from the database.
func (d *DB) HardDelete(ctx context.Context, arg ModelWithHardDelete) error {
	r, err := d.db.ExecContext(ctx, arg.HardDelete(), arg.GetID())
	if err != nil {
		return err
	}
	return RowsAffected(r, 1)
}

// Tx is an wrapper around sqlx.Tx with extra functionality.
type Tx struct {
	tx    *sqlx.Tx
	clock clock.Clock
}

// Begin begins a transaction and returns a new Tx.
func (d *DB) Begin(ctx context.Context) (*Tx, error) {
	tx, err := d.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &Tx{
		tx:    tx,
		clock: d.clock,
	}, nil
}

// Commit commits the transaction.
func (t *Tx) Commit() error {
	return t.tx.Commit()
}

// Rollback aborts the transaction.
func (t *Tx) Rollback() error {
	return t.tx.Rollback()
}

// Insert adds a new insert query for the given model in the transaction.
func (t *Tx) Insert(arg Model) error {
	var id string
	t0 := t.clock.Now()
	arg.SetCreatedAt(t0)
	arg.SetUpdatedAt(t0)

	query, qargs, err := t.tx.BindNamed(arg.Insert(), arg)
	if err != nil {
		return err
	}

	// Do insert using an exec if necessary.
	if _, ok := arg.(ModelWithExecInsert); ok {
		return t.insertWithExec(query, qargs...)
	}

	// Insert query with 'RETURNING id'
	row := t.tx.QueryRow(query, qargs...)
	if err := row.Scan(&id); err != nil {
		return err
	}
	arg.SetID(id)
	return nil
}

func (t *Tx) insertWithExec(query string, args ...any) error {
	r, err := t.tx.Exec(query, args...)
	if err != nil {
		return err
	}
	return RowsAffected(r, 1)
}

// Update adds a new update query for the given model in the transaction.
func (t *Tx) Update(arg Model) error {
	arg.SetUpdatedAt(t.clock.Now())
	query, qargs, err := t.tx.BindNamed(arg.Update(), arg)
	if err != nil {
		return err
	}
	r, err := t.tx.Exec(query, qargs...)
	if err != nil {
		return err
	}
	return RowsAffected(r, 1)
}

// Delete adds a new soft-delete query in the transaction.
func (t *Tx) Delete(arg Model) error {
	t0 := t.clock.Now()
	r, err := t.tx.Exec(arg.Delete(), t0, arg.GetID())
	if err != nil {
		return err
	}
	if err := RowsAffected(r, 1); err != nil {
		return err
	}

	arg.SetDeletedAt(t0)
	return nil
}

// HardDelete ads a new hard-delete query in the transaction.
func (t *Tx) HardDelete(arg ModelWithHardDelete) error {
	r, err := t.tx.Exec(arg.HardDelete(), arg.GetID())
	if err != nil {
		return err
	}
	return RowsAffected(r, 1)
}
