# AGENTS.md

Guidance for AI coding agents working in this repository.

## Overview

`sequel` (module `go.step.sm/sequel`) is a small, public, Apache-2.0 Go library for inserting and scanning PostgreSQL rows into Go structs. It wraps `github.com/go-sqlx/sqlx` on top of the `pgx/v5` stdlib driver and adds a `Model` interface with soft-delete/timestamp bookkeeping, transactions with post-commit hooks, round-robin read replicas, multi-row batch inserts, and a generic `Array[T]` scanner for PostgreSQL array columns. Its only first-party dependency is `go.step.sm/qb` (the query builder that generates the SQL a `Model` returns). It is consumed by Go services that talk to PostgreSQL; it has no `main` package and no runtime configuration of its own.

## Commands

```bash
make bootstrap   # install golangci-lint, govulncheck, gotestsum into $(go env GOPATH)/bin
make test        # gotestsum with coverage (what CI runs, as `V=1 make test`); needs Docker
make race        # same tests with -race
make lint        # golangci-lint (config fetched from smallstep/workflows) + govulncheck
make fmt         # goimports -local go.step.sm/sequel
make all         # lint + test
```

Single test (still starts the Postgres containers, see below):

```bash
gotestsum -- -run 'TestDBQueries/insert' ./...
go test -run TestBatch ./
go test ./clock/          # the only package that runs without Docker
```

`go build ./...` and `go vet ./...` both pass with no setup. `make generate` exists but there are no `go:generate` directives, so it is a no-op. There is no generated code and no mocks.

Tests need a running Docker daemon: `TestMain` in `main_test.go` uses testcontainers to start three `postgres:16.0-alpine` containers (one primary initialised by `testdata/init-db.sh`, two "read replicas" initialised by `testdata/init-db-rr.sh`, which also seeds rows) with the schema from `testdata/schema.sql`. The full suite takes about 15 seconds locally. `make test` writes `defaultcoverage.out` in the repo root; it is gitignored (`*.out`).

## CI and releases

- `.github/workflows/ci.yml` calls the shared `smallstep/workflows` `goCI.yml` with `run-build: false`, CodeQL enabled, latest Go only, and `test-command: 'V=1 make test'`. Lint runs there too.
- `.github/workflows/release.yml` runs on `v*` tags: it re-runs CI and then creates a GitHub release (tags containing `-rc` are marked pre-release). There are no binaries; a release is just a Go module version.
- `actionci.yml` (actionlint + zizmor, configured by `.github/zizmor.yml`), `code-scan-cron.yml`, and `dependabot-auto-merge.yml` are the org-standard workflows.

## Architecture

Everything is one flat package plus `clock`:

```
sequel/
├── sequel.go              # DB (New/NewDB + options), Tx, query/exec helpers, IsErrNotFound, IsUniqueViolation, RowsAffected
├── model.go               # Model / ModelWithHardDelete / ModelWithExecInsert interfaces, Base struct, Queries(qb)
├── read_replica_set.go    # ReadReplicaSet: round-robin ring of sqlx.DBs (Query, QueryRow, Get, GetAll)
├── batch.go               # Batch[T]: chunked multi-row INSERT (BatchSize = 100) through the Executor interface
├── array.go               # Array[T] sql.Scanner and ArrayScan for PostgreSQL arrays via pgtype
├── helpers.go             # NullBool/NullString/NullTime/... constructors (zero value => Valid=false)
├── clock/                 # Clock interface, real clock (UTC), NewMock(t) for tests
├── main_test.go           # TestMain: testcontainers setup for the three Postgres instances
└── testdata/              # schema.sql (person_test, array_test tables) + container init scripts
```

How a model is used: a struct embeds `sequel.Base` (`id`, `created_at`, `updated_at`, `deleted_at`) with a `dbtable:"..."` tag, builds its SQL once with `qb.Must(&model{})` and `sequel.Queries(builder)`, and returns those strings from `Select()/Insert()/Update()/Delete()`. `DB.Insert` runs the insert with `RETURNING id` and calls `SetID`; implement `ModelWithExecInsert` when the struct already carries its ID and the insert returns nothing. `Delete` is a soft delete (sets `deleted_at`); `HardDelete` requires `ModelWithHardDelete`. `created_at`/`updated_at` are stamped from the injected `clock.Clock`, which is what `WithClock(clock.NewMock(t))` is for in tests. See `personModel` in `sequel_test.go` for the canonical example.

Placeholders: queries from a `Model` use `$1`-style binds by default. `WithRebindModel()` makes `Select/Delete/HardDelete` rebind `?` placeholders for the driver; the `Rebind*` methods do that per call.

## Conventions

- Errors: `fmt.Errorf("...: %w", err)` and `errors.Is/As`; sentinel `ErrNoReadReplicaConnections`. Not-found is `sql.ErrNoRows` (check with `IsErrNotFound`).
- No logging in the library.
- Options are functional (`Option func(*options)`); add new knobs as `WithX` rather than new constructors.
- Every `DB` method takes a `context.Context`; `Tx` methods mostly do not (the transaction already carries one), with `*Context` variants where they exist.
- Tests use `testify` (`assert`/`require`), table-driven subtests, and hit the real containers; there are no mocks or golden files. Tests that write rows clean up in `t.Cleanup` (e.g. `DELETE ... WHERE name LIKE 'batch-%'`) because the containers are shared across the whole package.
- The schema in `testdata/schema.sql` is test-only; the library has no migrations.
- Keep the public API backward compatible: this is a versioned module consumed by other repositories, and a tag is a release.

## Related repos

- `go.step.sm/qb` (github.com/smallstep/qb): query builder that produces the SQL strings a `Model` returns; `Queries()` in `model.go` is the bridge. To develop both together, add `replace go.step.sm/qb => ../qb` to `go.mod` locally (do not commit it).
- `smallstep/workflows`: the reusable CI workflows and the `.golangci.yml` that `make lint` downloads.
