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
	db            *sqlx.DB
	clock         clock.Clock
	doRebindModel bool
	driverName    string
}

type options struct {
	Clock              clock.Clock
	DriverName         string
	RebindModel        bool
	MaxOpenConnections int
}

func newOptions(driverName string) *options {
	return &options{
		Clock:              clock.New(),
		DriverName:         driverName,
		RebindModel:        false,
		MaxOpenConnections: MaxOpenConnections,
	}
}

func (o *options) apply(opts []Option) *options {
	for _, fn := range opts {
		fn(o)
	}
	return o
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

// WithDriver defines the driver to use, defaults to pgx/v5. This default driver
// is automatically loaded by this package, any other driver must be loaded by
// the user.
func WithDriver(driverName string) Option {
	return func(o *options) {
		o.DriverName = driverName
	}
}

// WithRebindModel enables query rebind on unnamed queries from the model, the
// queries from Select(), Delete(), and HardDelete() methods.
func WithRebindModel() Option {
	return func(o *options) {
		o.RebindModel = true
	}
}

// WithMaxOpenConnections sets the maximum number of open connections to the
// database. If it is not set it will use [MaxOpenConnections] (100).
func WithMaxOpenConnections(n int) Option {
	return func(o *options) {
		o.MaxOpenConnections = n
	}
}

// New creates a new DB. It will fail if it cannot ping it.
func New(dataSourceName string, opts ...Option) (*DB, error) {
	options := newOptions("pgx/v5").apply(opts)

	// Connect opens the database and verifies with a ping
	db, err := sqlx.Connect(options.DriverName, dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("error connecting to the database: %w", err)
	}
	db.SetMaxOpenConns(options.MaxOpenConnections)

	return &DB{
		db:            db,
		clock:         options.Clock,
		doRebindModel: options.RebindModel,
		driverName:    options.DriverName,
	}, nil
}

// NewDB creates a new DB wrapping the opened database handle with the given
// driverName. It will fail if it cannot ping it.
func NewDB(db *sql.DB, driverName string, opts ...Option) (*DB, error) {
	options := newOptions(driverName).apply(opts)

	// Wrap an opened *sql.DB and verify the connection with a ping
	dbx := sqlx.NewDb(db, options.DriverName)
	if err := dbx.Ping(); err != nil {
		dbx.Close()
		return nil, fmt.Errorf("error connecting to the database: %w", err)
	}
	dbx.SetMaxOpenConns(options.MaxOpenConnections)

	return &DB{
		db:            dbx,
		clock:         options.Clock,
		doRebindModel: options.RebindModel,
		driverName:    options.DriverName,
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

// Driver returns the name of the driver used.
func (d *DB) Driver() string {
	return d.driverName
}

// DB returns the embedded *sql.DB.
func (d *DB) DB() *sql.DB {
	return d.db.DB
}

// Rebind transforms a query from `?` to the DB driver's bind type.
func (d *DB) Rebind(query string) string {
	return d.db.Rebind(query)
}

func (d *DB) rebindModel(query string) string {
	if d.doRebindModel {
		return d.Rebind(query)
	}
	return query
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

// Query executes a query that returns rows, typically a SELECT. The query is
// rebound from `?` to the DB driver's bind type. The args are for any
// placeholder parameters in the query.
func (d *DB) RebindQuery(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return d.db.QueryContext(ctx, d.db.Rebind(query), args...)
}

// QueryRow executes a query that is expected to return at most one row. The
// query is rebound from `?` to the DB driver's bind type. QueryRowContext
// always returns a non-nil value. Errors are deferred until Row's Scan method
// is called.
//
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *Row's Scan scans the first selected row and discards the
// rest.
func (d *DB) RebindQueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return d.db.QueryRowContext(ctx, d.db.Rebind(query), args...)
}

// Exec executes a query without returning any rows. The query is rebound from
// `?` to the DB driver's bind type. The args are for any placeholder parameters
// in the query.
func (d *DB) RebindExec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return d.db.ExecContext(ctx, d.db.Rebind(query), args...)
}

// NamedQuery executes a query that returns rows. Any named placeholder
// parameters are replaced with fields from arg.
func (d *DB) NamedQuery(ctx context.Context, query string, arg any) (*sqlx.Rows, error) {
	return d.db.NamedQueryContext(ctx, query, arg)
}

// NamedExec using executes a query without returning any rows. Any named
// placeholder parameters are replaced with fields from arg.
func (d *DB) NamedExec(ctx context.Context, query string, arg any) (sql.Result, error) {
	return d.db.NamedExecContext(ctx, query, arg)
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
	return d.db.GetContext(ctx, dest, d.rebindModel(dest.Select()), id)
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
	r, err := d.db.ExecContext(ctx, d.rebindModel(arg.Delete()), t0, arg.GetID())
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
	r, err := d.db.ExecContext(ctx, d.rebindModel(arg.HardDelete()), arg.GetID())
	if err != nil {
		return err
	}
	return RowsAffected(r, 1)
}

// Prepare creates a prepared statement.
func (d *DB) Prepare(ctx context.Context, query string) (*sql.Stmt, error) {
	return d.db.PrepareContext(ctx, query)
}

// Tx is an wrapper around sqlx.Tx with extra functionality.
type Tx struct {
	tx            *sqlx.Tx
	clock         clock.Clock
	doRebindModel bool
	oncommit      []func()
}

// Begin begins a transaction and returns a new Tx.
func (d *DB) Begin(ctx context.Context) (*Tx, error) {
	tx, err := d.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &Tx{
		tx:            tx,
		clock:         d.clock,
		doRebindModel: d.doRebindModel,
	}, nil
}

// Rebind transforms a query from QUESTION to the DB driver's bind type.
func (t *Tx) Rebind(query string) string {
	return t.tx.Rebind(query)
}

func (t *Tx) rebindModel(query string) string {
	if t.doRebindModel {
		return t.Rebind(query)
	}
	return query
}

// Commit commits the transaction.
func (t *Tx) Commit() error {
	err := t.tx.Commit()
	if err != nil {
		return err
	}
	for _, fn := range t.oncommit {
		fn()
	}
	return nil
}

// PostCommit registers a function to be called after a successful commit.
func (t *Tx) PostCommit(fn func()) {
	t.oncommit = append(t.oncommit, fn)
}

// Rollback aborts the transaction.
func (t *Tx) Rollback() error {
	return t.tx.Rollback()
}

// Query executes a query that returns rows, typically a SELECT. The args are
// for any placeholder parameters in the query.
func (t *Tx) Query(query string, args ...any) (*sql.Rows, error) {
	return t.tx.Query(query, args...)
}

// QueryContext works like Query but with context.
func (t *Tx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return t.tx.QueryContext(ctx, query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRowContext always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
//
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *Row's Scan scans the first selected row and discards the
// rest.
func (t *Tx) QueryRow(query string, args ...any) *sql.Row {
	return t.tx.QueryRow(query, args...)
}

// QueryRowContext works like QueryRow but with context.
func (t *Tx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return t.tx.QueryRowContext(ctx, query, args...)
}

// Exec executes a query without returning any rows. The args are for any
// placeholder parameters in the query.
func (t *Tx) Exec(query string, args ...any) (sql.Result, error) {
	return t.tx.Exec(query, args...)
}

// ExecContext works like Exec but with context.
func (t *Tx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.tx.ExecContext(ctx, query, args...)
}

// Query executes a query that returns rows, typically a SELECT. The query is
// rebound from `?` to the DB driver's bind type. The args are for any
// placeholder parameters in the query.
func (t *Tx) RebindQuery(query string, args ...any) (*sql.Rows, error) {
	return t.tx.Query(t.tx.Rebind(query), args...)
}

// QueryRow executes a query that is expected to return at most one row. The
// query is rebound from `?` to the DB driver's bind type. QueryRowContext
// always returns a non-nil value. Errors are deferred until Row's Scan method
// is called.
//
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *Row's Scan scans the first selected row and discards the
// rest.
func (t *Tx) RebindQueryRow(query string, args ...any) *sql.Row {
	return t.tx.QueryRow(t.tx.Rebind(query), args...)
}

// Exec executes a query without returning any rows. The query is rebound from
// `?` to the DB driver's bind type. The args are for any placeholder parameters
// in the query.
func (t *Tx) RebindExec(query string, args ...any) (sql.Result, error) {
	return t.tx.Exec(t.tx.Rebind(query), args...)
}

// NamedQuery executes a query that returns rows. Any named placeholder
// parameters are replaced with fields from arg.
func (t *Tx) NamedQuery(query string, arg any) (*sqlx.Rows, error) {
	return t.tx.NamedQuery(query, arg)
}

// NamedExec using executes a query without returning any rows. Any named
// placeholder parameters are replaced with fields from arg.
func (t *Tx) NamedExec(query string, arg any) (sql.Result, error) {
	return t.tx.NamedExec(query, arg)
}

// Select populates the given model with the result of a select by id query.
func (t *Tx) Select(dest Model, id string) error {
	return t.tx.Get(dest, t.rebindModel(dest.Select()), id)
}

// Get populates the given model for the result of the given select query.
func (t *Tx) Get(dest Model, query string, args ...any) error {
	return t.tx.Get(dest, query, args...)
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
	r, err := t.tx.Exec(t.rebindModel(arg.Delete()), t0, arg.GetID())
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
	r, err := t.tx.Exec(t.rebindModel(arg.HardDelete()), arg.GetID())
	if err != nil {
		return err
	}
	return RowsAffected(r, 1)
}

// Prepare creates a prepared statement
func (t *Tx) Prepare(query string) (*sql.Stmt, error) {
	return t.tx.Prepare(query)
}
