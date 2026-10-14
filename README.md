# sequel

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Package for inserting and scanning data from a PostgreSQL database into Go structs.

## Usage

```shell
go get go.step.sm/sequel
```

### Defining a model

A model is a struct that embeds `sequel.Base` and implements the four query
methods of `sequel.Model`. The queries themselves are generated from the struct
tags by [qb](https://github.com/smallstep/qb): `dbtable` names the table and
`db` names each column.

```go
package users

import (
    "database/sql"

    "go.step.sm/qb"
    "go.step.sm/sequel"
)

var selectQ, insertQ, updateQ, deleteQ string

func init() {
    selectQ, insertQ, updateQ, deleteQ = sequel.Queries(qb.Must(&User{}))
}

// User maps to the users table.
type User struct {
    sequel.Base `dbtable:"users"`
    Name        string         `db:"name"`
    Email       sql.NullString `db:"email"`
}

func (u *User) Select() string { return selectQ }
func (u *User) Insert() string { return insertQ }
func (u *User) Update() string { return updateQ }
func (u *User) Delete() string { return deleteQ }
```

`sequel.Base` contributes the `id`, `created_at`, `updated_at`, and `deleted_at`
columns, and implements the ID and timestamp setters of `sequel.Model`; the
table is expected to have those four columns. Building the queries once in
`init` keeps them out of the request path.

### Connecting

```go
db, err := sequel.New(os.Getenv("DATABASE_URL"))
if err != nil {
    return err
}
defer db.Close()
```

The default driver is `pgx/v5`, which this package registers for you. Any other
driver must be registered by your program and selected with
`sequel.WithDriver`. Drivers that use `?` placeholders instead of `$1` also
need `sequel.WithRebindModel`, so that the queries coming from the model are
rebound. `sequel.New` connects eagerly and fails if the ping does not succeed;
it opens up to `sequel.MaxOpenConnections` (100) connections unless
`sequel.WithMaxOpenConnections` says otherwise. If your program already has an
open `*sql.DB`, use `sequel.NewDB` to wrap it instead.

### Reading and writing models

```go
u := &User{Name: "Jane"}

// Insert sets created_at, updated_at, and the generated id on u.
if err := db.Insert(ctx, u); err != nil {
    if sequel.IsUniqueViolation(err) {
        return errAlreadyExists
    }
    return err
}

// Select looks up a row by id, ignoring soft-deleted ones.
var got User
if err := db.Select(ctx, &got, u.ID); err != nil {
    if sequel.IsErrNotFound(err) {
        return errNotFound
    }
    return err
}

// Update refreshes updated_at; Delete is a soft delete that sets deleted_at.
got.Name = "Jane Doe"
if err := db.Update(ctx, &got); err != nil {
    return err
}
if err := db.Delete(ctx, &got); err != nil {
    return err
}
```

`Update` and `Delete` return `sql.ErrNoRows` when they do not affect exactly one
row, so `sequel.IsErrNotFound` covers them too. A model that also implements
`HardDelete() string` can be removed for good with `db.HardDelete`, and
`db.InsertBatch` inserts a slice of models inside a single transaction.

For anything the model queries do not cover, `Get` scans one row and `GetAll`
scans into a slice:

```go
var u User
err := db.Get(ctx, &u, "SELECT * FROM users WHERE email = $1 AND deleted_at IS NULL", email)

var active []User
err = db.GetAll(ctx, &active, "SELECT * FROM users WHERE deleted_at IS NULL")
```

`Query`, `QueryRow`, and `Exec` are also available for raw SQL, each with a
`Rebind`-prefixed variant that rewrites `?` placeholders for the driver in use,
along with `NamedQuery` and `NamedExec` for queries with `:name` parameters.

### Transactions

`db.Begin` returns a `Tx` with the same model helpers. They take no context, as
the one passed to `Begin` governs the transaction:

```go
tx, err := db.Begin(ctx)
if err != nil {
    return err
}
defer tx.Rollback() //nolint:errcheck // no-op once committed

if err := tx.Insert(u); err != nil {
    return err
}
if err := tx.Update(account); err != nil {
    return err
}

// Registered functions run in their own goroutine after a successful commit.
tx.PostCommit(func() { notify(u.ID) })

return tx.Commit()
```

## Read replicas

Read replicas are configured on the primary connection with the
`sequel.WithReadReplica` option. Pass it once per replica DSN:

```go
db, err := sequel.New(os.Getenv("DATABASE_URL"),
    sequel.WithReadReplica(os.Getenv("DATABASE_REPLICA_1_URL")),
    sequel.WithReadReplica(os.Getenv("DATABASE_REPLICA_2_URL")),
)
if err != nil {
    return err
}
defer db.Close()
```

Every replica is connected and pinged when the `DB` is created, so `sequel.New`
fails if any replica is unreachable. `db.Close()` closes the replica
connections along with the primary one. The same
`sequel.WithMaxOpenConnections` limit is applied to each replica
independently, so a program with two replicas and a limit of 100 may open up to
300 connections in total.

To route a query to a replica, use `db.ReadReplicaSet()` instead of `db`. It
exposes the read-only subset of the `DB` API, and each call picks the next
replica in round-robin order:

```go
func ListUsers(ctx context.Context, db *sequel.DB) ([]User, error) {
    var users []User
    // Served by a read replica.
    if err := db.ReadReplicaSet().GetAll(ctx, &users, "SELECT * FROM users WHERE deleted_at IS NULL"); err != nil {
        return nil, err
    }
    return users, nil
}

func CreateUser(ctx context.Context, db *sequel.DB, u *User) error {
    // Writes always go to the primary.
    return db.Insert(ctx, u)
}
```

The available methods are `Query`, `QueryRow`, `Get`, and `GetAll`, matching
their `DB` counterparts except that `QueryRow` also returns an error, since
picking a replica can fail. Only `SELECT` statements are supported; writes and
transactions must go through `db` directly.

### Falling back to the primary

`db.ReadReplicaSet()` returns `nil` when no replica is configured, and the
returned methods are safe to call on a `nil` set: they return
`sequel.ErrNoReadReplicaConnections`. That makes it possible to write code that
prefers a replica but still works in environments configured without one, such
as local development or tests:

```go
func getAll(ctx context.Context, db *sequel.DB, dest any, query string, args ...any) error {
    err := db.ReadReplicaSet().GetAll(ctx, dest, query, args...)
    if errors.Is(err, sequel.ErrNoReadReplicaConnections) {
        return db.GetAll(ctx, dest, query, args...)
    }
    return err
}
```

Because replication is asynchronous, a replica may not yet have the rows
committed by a recent write. Read your own writes from the primary, and reserve
the replica set for queries that tolerate a small amount of staleness.
