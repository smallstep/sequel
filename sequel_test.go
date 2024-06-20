package sequel

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-sqlx/sqlx"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.step.sm/qb"
	"go.step.sm/sequel/clock"
)

var (
	personSelectQ, personInsertQ, personUpdateQ, personDeleteQ                         string
	personInsertExecQ, personHardDeleteQ                                               string
	personExecQ                                                                        string
	personBindedSelectQ, personBindedInsertQ, personBindedUpdateQ, personBindedDeleteQ string
	personBindedHardDeleteQ                                                            string
)

func init() {
	builder := qb.Must(&personModel{})
	personSelectQ, personInsertQ, personUpdateQ, personDeleteQ = Queries(builder)
	personInsertExecQ = builder.NamedInsert()
	personHardDeleteQ = builder.HardDelete()
	personExecQ = builder.Insert()

	builder = qb.Must(&personModelBinded{}, qb.BindType(qb.QUESTION))
	personBindedSelectQ, personBindedInsertQ, personBindedUpdateQ, personBindedDeleteQ = Queries(builder)
	personBindedHardDeleteQ = builder.HardDelete()
}

type personModel struct {
	Base  `dbtable:"person_test"`
	Name  string         `db:"name"`
	Email sql.NullString `db:"email"`
}

func (m *personModel) Select() string { return personSelectQ }
func (m *personModel) Insert() string { return personInsertQ }
func (m *personModel) Update() string { return personUpdateQ }
func (m *personModel) Delete() string { return personDeleteQ }

type personModelBinded struct {
	personModel `dbtable:"person_test"`
}

func (m *personModelBinded) Select() string     { return personBindedSelectQ }
func (m *personModelBinded) Insert() string     { return personBindedInsertQ }
func (m *personModelBinded) Update() string     { return personBindedUpdateQ }
func (m *personModelBinded) Delete() string     { return personBindedDeleteQ }
func (m *personModelBinded) HardDelete() string { return personBindedHardDeleteQ }

type personModelExtra struct {
	personModel
}

func (m *personModelExtra) Insert() string {
	return personInsertExecQ
}

func (m *personModelExtra) HardDelete() string {
	return personHardDeleteQ
}

func (m *personModelExtra) WithExecInsert() {}

func assertEqualPerson(t *testing.T, want, got *personModel) bool {
	t.Helper()
	if got != nil {
		got.CreatedAt = got.CreatedAt.UTC().Truncate(time.Second)
		got.UpdatedAt = got.UpdatedAt.UTC().Truncate(time.Second)
		if got.DeletedAt.Valid {
			got.DeletedAt = NullTime(got.DeletedAt.Time.UTC().Truncate(time.Second))
		}
	}
	want.CreatedAt = want.CreatedAt.Truncate(time.Second)
	want.UpdatedAt = want.UpdatedAt.Truncate(time.Second)
	if want.DeletedAt.Valid {
		want.DeletedAt = NullTime(want.DeletedAt.Time.Truncate(time.Second))
	}
	return assert.Equal(t, want, got)
}

func assertEqualPersons(t *testing.T, want, got []*personModel) bool {
	t.Helper()
	for i := range got {
		got[i].CreatedAt = got[i].CreatedAt.UTC().Truncate(time.Second)
		got[i].UpdatedAt = got[i].UpdatedAt.UTC().Truncate(time.Second)
		if got[i].DeletedAt.Valid {
			got[i].DeletedAt = NullTime(got[i].DeletedAt.Time.UTC().Truncate(time.Second))
		}
	}
	for i := range want {
		want[i].CreatedAt = want[i].CreatedAt.Truncate(time.Second)
		want[i].UpdatedAt = want[i].UpdatedAt.Truncate(time.Second)
		if want[i].DeletedAt.Valid {
			want[i].DeletedAt = NullTime(want[i].DeletedAt.Time.Truncate(time.Second))
		}
	}
	return assert.ElementsMatch(t, want, got)
}

func TestNew(t *testing.T) {
	type args struct {
		dataSourceName string
		opts           []Option
	}
	tests := []struct {
		name      string
		args      args
		assertion assert.ErrorAssertionFunc
	}{
		{"ok", args{postgresDataSource, nil}, assert.NoError},
		{"ok with clock", args{postgresDataSource, []Option{WithClock(clock.NewMock(time.Now()))}}, assert.NoError},
		{"ok with driver", args{postgresDataSource, []Option{WithDriver("pgx/v5")}}, assert.NoError},
		{"ok with rebindModel", args{postgresDataSource, []Option{WithRebindModel()}}, assert.NoError},
		{"fail ping", args{strings.ReplaceAll(postgresDataSource, dbUser, "foo"), nil}, assert.Error},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := New(tt.args.dataSourceName, tt.args.opts...)
			tt.assertion(t, err)
			if db != nil {
				assert.NoError(t, db.Close())
			}
		})
	}
}

func TestNewContext(t *testing.T) {
	db, err := New(postgresDataSource)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	ctx := NewContext(context.Background(), db)
	db1, ok := FromContext(ctx)
	assert.Equal(t, db, db1)
	assert.True(t, ok)

	ctx = context.WithValue(context.Background(), dbKey{}, 123)
	db2, ok := FromContext(ctx)
	assert.Nil(t, db2)
	assert.False(t, ok)
}

func TestIsErrNotFound(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"true", args{sql.ErrNoRows}, true},
		{"true wrapped", args{fmt.Errorf("some error: %w", sql.ErrNoRows)}, true},
		{"false", args{errors.New(sql.ErrNoRows.Error())}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsErrNotFound(tt.args.err))
		})
	}
}

func TestIsUniqueViolation(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"true", args{&pgconn.PgError{Code: "23505"}}, true},
		{"false", args{&pgconn.PgError{Code: "10000"}}, false},
		{"false other", args{sql.ErrNoRows}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsUniqueViolation(tt.args.err))
		})
	}
}

func TestDBQueries(t *testing.T) {
	db, err := New(postgresDataSource)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	p1 := &personModel{
		Name:  "Lucky Luke",
		Email: NullString("lucky@example.com"),
	}
	p2 := &personModel{
		Name:  "Jolly Jumper",
		Email: NullString("jolly@example.com"),
	}
	p3 := &personModel{
		Name:  "Joe Walton",
		Email: NullString("joe@example.com"),
	}
	p4 := &personModelExtra{
		personModel: personModel{
			Base: Base{
				ID: "f2026a37-1334-409a-939b-6a6f5c270724",
			},
			Name:  "Jack Dalton",
			Email: NullString("jack@example.com"),
		},
	}
	p5 := &personModelExtra{
		personModel: personModel{
			Base: Base{
				ID: "ce5b82e5-16cc-45c3-8d82-6678680ee37f",
			},
			Name:  "Will Dalton",
			Email: NullString("Will@example.com"),
		},
	}

	ctx := context.Background()

	t.Run("rebind", func(t *testing.T) {
		query := db.Rebind("SELECT * FROM person_test WHERE name = ? AND email = ?")
		assert.Equal(t, "SELECT * FROM person_test WHERE name = $1 AND email = $2", query)
	})

	t.Run("insert", func(t *testing.T) {
		assert.NoError(t, db.Insert(ctx, p1))
		assert.NoError(t, db.InsertBatch(ctx, []Model{p2, p3, p4}))
		assert.NoError(t, db.Insert(ctx, p5))
		// unique index
		err := db.Insert(ctx, p1)
		assert.Error(t, err)
		assert.True(t, IsUniqueViolation(err))
		// unique index on batch
		err = db.InsertBatch(ctx, []Model{&personModel{
			Name: "Fail Dalton", Email: NullString("fail@example.com"),
		}, p1})
		assert.Error(t, err)
		assert.True(t, IsUniqueViolation(err))
		// email is missing
		assert.Error(t, db.Insert(ctx, &personModel{
			Name: "Fail Dalton",
		}))
		// id is missing
		assert.Error(t, db.Insert(ctx, &personModelExtra{
			personModel: personModel{
				Name:  "Fail Dalton",
				Email: NullString("fail@smallstep.com"),
			},
		}))
	})
	time.Sleep(time.Second)
	t.Run("query", func(t *testing.T) {
		rows, err := db.Query(ctx, "SELECT * FROM person_test WHERE id = $1", p1.GetID())
		assert.NoError(t, err)
		for rows.Next() {
			var p personModel
			assert.NoError(t, rows.Scan(&p.ID, &p.CreatedAt, &p.UpdatedAt, &p.DeletedAt, &p.Name, &p.Email))
			assertEqualPerson(t, p1, &p)
		}
		assert.NoError(t, rows.Err())
		assert.NoError(t, rows.Close()) //nolint:sqlclosecheck // no defer for testing purposes
	})

	t.Run("queryRow", func(t *testing.T) {
		var p personModel
		row := db.QueryRow(ctx, "SELECT * FROM person_test WHERE id = $1", p1.GetID())
		assert.NoError(t, row.Err())
		assert.NoError(t, row.Scan(&p.ID, &p.CreatedAt, &p.UpdatedAt, &p.DeletedAt, &p.Name, &p.Email))
		assertEqualPerson(t, p1, &p)
	})

	t.Run("rebindQuery", func(t *testing.T) {
		rows, err := db.RebindQuery(ctx, "SELECT * FROM person_test WHERE id = ?", p1.GetID())
		assert.NoError(t, err)
		for rows.Next() {
			var p personModel
			assert.NoError(t, rows.Scan(&p.ID, &p.CreatedAt, &p.UpdatedAt, &p.DeletedAt, &p.Name, &p.Email))
			assertEqualPerson(t, p1, &p)
		}
		assert.NoError(t, rows.Err())
		assert.NoError(t, rows.Close()) //nolint:sqlclosecheck // no defer for testing purposes
	})

	t.Run("rebindQueryRow", func(t *testing.T) {
		var p personModel
		row := db.RebindQueryRow(ctx, "SELECT * FROM person_test WHERE id = ?", p1.GetID())
		assert.NoError(t, row.Err())
		assert.NoError(t, row.Scan(&p.ID, &p.CreatedAt, &p.UpdatedAt, &p.DeletedAt, &p.Name, &p.Email))
		assertEqualPerson(t, p1, &p)
	})

	t.Run("namedQuery", func(t *testing.T) {
		rows, err := db.NamedQuery(ctx, "SELECT * FROM person_test WHERE id = :id", p1)
		assert.NoError(t, err)
		for rows.Next() {
			var p personModel
			assert.NoError(t, rows.Scan(&p.ID, &p.CreatedAt, &p.UpdatedAt, &p.DeletedAt, &p.Name, &p.Email))
			assertEqualPerson(t, p1, &p)
		}
		assert.NoError(t, rows.Err())
		assert.NoError(t, rows.Close()) //nolint:sqlclosecheck // no defer for testing purposes
	})

	t.Run("namedQuery withMap", func(t *testing.T) {
		rows, err := db.NamedQuery(ctx, "SELECT * FROM person_test WHERE id = :id", map[string]any{
			"id": p1.GetID(),
		})
		assert.NoError(t, err)
		for rows.Next() {
			var p personModel
			assert.NoError(t, rows.Scan(&p.ID, &p.CreatedAt, &p.UpdatedAt, &p.DeletedAt, &p.Name, &p.Email))
			assertEqualPerson(t, p1, &p)
		}
		assert.NoError(t, rows.Err())
		assert.NoError(t, rows.Close()) //nolint:sqlclosecheck // no defer for testing purposes
	})

	t.Run("get", func(t *testing.T) {
		var pp1, pp2 personModel
		assert.NoError(t, db.Get(ctx, &pp1, "SELECT * FROM person_test WHERE id = $1", p1.GetID()))
		assertEqualPerson(t, p1, &pp1)
		assert.Equal(t, sql.ErrNoRows, db.Get(ctx, &pp2, "SELECT * FROM person_test WHERE id = $1 AND deleted_at IS NOT NULL", p1.GetID()))
		assertEqualPerson(t, &personModel{}, &pp2)
	})

	t.Run("getAll", func(t *testing.T) {
		var ap []*personModel
		assert.NoError(t, db.GetAll(ctx, &ap, "SELECT * FROM person_test"))
		assertEqualPersons(t, []*personModel{p1, p2, p3, &p4.personModel, &p5.personModel}, ap)
		assert.NoError(t, db.GetAll(ctx, &ap, "SELECT * FROM person_test WHERE deleted_at IS NOT NULL"))
		assertEqualPersons(t, []*personModel{}, ap)
	})

	t.Run("select", func(t *testing.T) {
		var pp1, pp2 personModel
		assert.NoError(t, db.Select(ctx, &pp1, p2.GetID()))
		assertEqualPerson(t, p2, &pp1)
		assert.Equal(t, sql.ErrNoRows, db.Select(ctx, &pp2, "cf349a3d-7bc7-4208-bb73-1b1651e80540"))
		assertEqualPerson(t, &personModel{}, &pp2)
	})

	t.Run("update", func(t *testing.T) {
		var pp personModel
		p3.Name = "Averell Dalton"
		p3.Email = NullString("averell@example.com")
		assert.NoError(t, db.Update(ctx, p3))
		assert.NoError(t, db.Select(ctx, &pp, p3.GetID()))
		assertEqualPerson(t, p3, &pp)
		assert.Equal(t, "Averell Dalton", pp.Name)
		assert.Equal(t, NullString("averell@example.com"), pp.Email)
	})

	t.Run("delete", func(t *testing.T) {
		var pp personModel
		assert.NoError(t, db.Delete(ctx, p3))
		assert.Error(t, db.Select(ctx, &pp, p3.GetID()))
	})
	t.Run("hard delete", func(t *testing.T) {
		var pp personModel
		assert.NoError(t, db.HardDelete(ctx, p5))
		assert.Error(t, db.HardDelete(ctx, p5))
		assert.Error(t, db.Select(ctx, &pp, p5.GetID()))
	})

	t.Run("rebindExec", func(t *testing.T) {
		var pp personModel
		p1.DeletedAt = sql.NullTime{
			Valid: true,
			Time:  time.Now().UTC().Truncate(time.Second),
		}
		res, err := db.RebindExec(ctx, "UPDATE person_test SET deleted_at = ? WHERE id = ?", p1.DeletedAt, p1.ID)
		assert.NoError(t, err)
		assert.NoError(t, RowsAffected(res, 1))
		assert.NoError(t, db.Get(ctx, &pp, "SELECT * FROM person_test WHERE id = $1", p1.GetID()))
		assertEqualPerson(t, p1, &pp)
	})

	t.Run("namedExec", func(t *testing.T) {
		var pp personModel
		p1.DeletedAt = sql.NullTime{
			Valid: true,
			Time:  time.Now().UTC().Truncate(time.Second),
		}
		res, err := db.NamedExec(ctx, "UPDATE person_test SET deleted_at = :deleted_at WHERE id = :id", p1)
		assert.NoError(t, err)
		assert.NoError(t, RowsAffected(res, 1))
		assert.NoError(t, db.Get(ctx, &pp, "SELECT * FROM person_test WHERE id = $1", p1.GetID()))
		assertEqualPerson(t, p1, &pp)
	})

	t.Run("namedExec map", func(t *testing.T) {
		var pp personModel
		p1.DeletedAt = sql.NullTime{
			Valid: true,
			Time:  time.Now().UTC().Truncate(time.Second),
		}
		res, err := db.NamedExec(ctx, "UPDATE person_test SET deleted_at = :deleted_at WHERE id = :id", map[string]any{
			"deleted_at": p1.DeletedAt.Time,
			"id":         p1.ID,
		})
		assert.NoError(t, err)
		assert.NoError(t, RowsAffected(res, 1))
		assert.NoError(t, db.Get(ctx, &pp, "SELECT * FROM person_test WHERE id = $1", p1.GetID()))
		assertEqualPerson(t, p1, &pp)
	})

	t.Run("exec (clear table)", func(t *testing.T) {
		res, err := db.Exec(ctx, "DELETE FROM person_test")
		assert.NoError(t, err)
		assert.NoError(t, RowsAffected(res, 4)) // p1 to p4, p5 is hard deleted
	})
}

func TestTxQueries(t *testing.T) {
	db, err := New(postgresDataSource)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	ctx := context.Background()

	tx, err := db.Begin(ctx)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, tx.Rollback())
	}()

	p1 := &personModel{
		Name:  "Lucky Luke",
		Email: NullString("lucky@example.com"),
	}
	p2 := &personModelExtra{
		personModel: personModel{
			Base: Base{
				ID: "d59a4685-9ab9-4323-9af9-14ca352cc65b",
			},
			Name:  "Jolly Jumper",
			Email: NullString("jolly@example.com"),
		},
	}

	t.Run("rebind", func(t *testing.T) {
		tx, err := db.Begin(ctx)
		require.NoError(t, err)
		query := tx.Rebind("SELECT * FROM person_test WHERE name = ? AND email = ?")
		assert.Equal(t, "SELECT * FROM person_test WHERE name = $1 AND email = $2", query)
		assert.NoError(t, tx.Rollback())
	})

	t.Run("insert", func(t *testing.T) {
		tx, err := db.Begin(ctx)
		require.NoError(t, err)
		defer func() {
			assert.Error(t, tx.Rollback())
		}()

		assert.NoError(t, tx.Insert(p1))
		assert.NoError(t, tx.Insert(p2))
		assert.NoError(t, tx.Commit())
	})

	t.Run("insert error", func(t *testing.T) {
		tx, err := db.Begin(ctx)
		require.NoError(t, err)
		assert.Error(t, tx.Insert(p1))
		assert.NoError(t, tx.Rollback())
	})

	t.Run("query", func(t *testing.T) {
		tx, err := db.Begin(ctx)
		require.NoError(t, err)
		rows, err := tx.Query("SELECT * FROM person_test WHERE id = $1", p1.GetID())
		assert.NoError(t, err)
		for rows.Next() {
			var p personModel
			assert.NoError(t, rows.Scan(&p.ID, &p.CreatedAt, &p.UpdatedAt, &p.DeletedAt, &p.Name, &p.Email))
			assertEqualPerson(t, p1, &p)
		}
		assert.NoError(t, rows.Err())
		assert.NoError(t, rows.Close()) //nolint:sqlclosecheck // no defer for testing purposes
		assert.NoError(t, tx.Commit())
	})

	t.Run("queryRow", func(t *testing.T) {
		var p personModel
		tx, err := db.Begin(ctx)
		require.NoError(t, err)
		row := tx.QueryRow("SELECT * FROM person_test WHERE id = $1", p1.GetID())
		assert.NoError(t, row.Err())
		assert.NoError(t, row.Scan(&p.ID, &p.CreatedAt, &p.UpdatedAt, &p.DeletedAt, &p.Name, &p.Email))
		assertEqualPerson(t, p1, &p)
		assert.NoError(t, tx.Commit())
	})

	t.Run("rebindQuery", func(t *testing.T) {
		tx, err := db.Begin(ctx)
		require.NoError(t, err)
		rows, err := tx.RebindQuery("SELECT * FROM person_test WHERE id = ?", p1.GetID())
		assert.NoError(t, err)
		for rows.Next() {
			var p personModel
			assert.NoError(t, rows.Scan(&p.ID, &p.CreatedAt, &p.UpdatedAt, &p.DeletedAt, &p.Name, &p.Email))
			assertEqualPerson(t, p1, &p)
		}
		assert.NoError(t, rows.Err())
		assert.NoError(t, rows.Close()) //nolint:sqlclosecheck // no defer for testing purposes
		assert.NoError(t, tx.Commit())
	})

	t.Run("rebindQueryRow", func(t *testing.T) {
		var p personModel
		tx, err := db.Begin(ctx)
		require.NoError(t, err)
		row := tx.RebindQueryRow("SELECT * FROM person_test WHERE id = ?", p1.GetID())
		assert.NoError(t, row.Err())
		assert.NoError(t, row.Scan(&p.ID, &p.CreatedAt, &p.UpdatedAt, &p.DeletedAt, &p.Name, &p.Email))
		assertEqualPerson(t, p1, &p)
		assert.NoError(t, tx.Commit())
	})

	t.Run("namedQuery", func(t *testing.T) {
		tx, err := db.Begin(ctx)
		require.NoError(t, err)
		rows, err := tx.NamedQuery("SELECT * FROM person_test WHERE id = :id", p1)
		assert.NoError(t, err)
		for rows.Next() {
			var p personModel
			assert.NoError(t, rows.Scan(&p.ID, &p.CreatedAt, &p.UpdatedAt, &p.DeletedAt, &p.Name, &p.Email))
			assertEqualPerson(t, p1, &p)
		}
		assert.NoError(t, rows.Err())
		assert.NoError(t, rows.Close()) //nolint:sqlclosecheck // no defer for testing purposes
		assert.NoError(t, tx.Commit())
	})

	t.Run("get", func(t *testing.T) {
		var p personModel
		tx, err := db.Begin(ctx)
		require.NoError(t, err)
		err = tx.Get(&p, "SELECT * FROM person_test WHERE id = $1", p1.GetID())
		assert.NoError(t, err)
		assertEqualPerson(t, p1, &p)
		assert.NoError(t, tx.Commit())
	})

	t.Run("select", func(t *testing.T) {
		var p personModel
		tx, err := db.Begin(ctx)
		require.NoError(t, err)
		err = tx.Select(&p, p1.GetID())
		assert.NoError(t, err)
		assertEqualPerson(t, p1, &p)
		assert.NoError(t, tx.Commit())
	})

	t.Run("update", func(t *testing.T) {
		tx, err := db.Begin(ctx)
		require.NoError(t, err)
		defer func() {
			assert.Error(t, tx.Rollback())
		}()

		assert.NoError(t, tx.Update(p1))
		assert.NoError(t, tx.Commit())
	})

	t.Run("update error", func(t *testing.T) {
		tx, err := db.Begin(ctx)
		require.NoError(t, err)
		pp := &personModel{
			Base:  p1.Base,
			Name:  p1.Name,
			Email: p2.Email,
		}

		assert.Error(t, tx.Update(pp))
		assert.NoError(t, tx.Rollback())
	})

	t.Run("delete", func(t *testing.T) {
		tx, err := db.Begin(ctx)
		require.NoError(t, err)
		defer func() {
			assert.Error(t, tx.Rollback())
		}()

		assert.NoError(t, tx.Delete(p1))
		assert.NoError(t, tx.HardDelete(p2))
		assert.NoError(t, tx.Commit())
	})

	t.Run("delete error", func(t *testing.T) {
		tx, err := db.Begin(ctx)
		require.NoError(t, err)
		assert.Error(t, tx.Delete(p2))
		assert.NoError(t, tx.Rollback())
	})

	t.Run("hard delete error", func(t *testing.T) {
		tx, err := db.Begin(ctx)
		require.NoError(t, err)
		assert.Error(t, tx.HardDelete(p2))
		assert.NoError(t, tx.Rollback())
	})

	t.Run("rebindExec", func(t *testing.T) {
		var p personModel
		tx, err := db.Begin(ctx)
		require.NoError(t, err)
		defer func() {
			assert.Error(t, tx.Rollback())
		}()

		p1.DeletedAt = sql.NullTime{
			Time:  time.Now().UTC().Truncate(time.Second),
			Valid: true,
		}

		res, err := tx.RebindExec("UPDATE person_test SET deleted_at = ? WHERE id = ?", p1.DeletedAt, p1.ID)
		assert.NoError(t, err)
		n, err := res.RowsAffected()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), n)
		// In transaction
		row := tx.RebindQueryRow("SELECT * FROM person_test WHERE id = ?", p1.GetID())
		assert.NoError(t, row.Err())
		assert.NoError(t, row.Scan(&p.ID, &p.CreatedAt, &p.UpdatedAt, &p.DeletedAt, &p.Name, &p.Email))
		assertEqualPerson(t, p1, &p)
		assert.NoError(t, tx.Commit())
		// After commit
		row = db.RebindQueryRow(ctx, "SELECT * FROM person_test WHERE id = ?", p1.GetID())
		assert.NoError(t, row.Err())
		assert.NoError(t, row.Scan(&p.ID, &p.CreatedAt, &p.UpdatedAt, &p.DeletedAt, &p.Name, &p.Email))
		assertEqualPerson(t, p1, &p)
	})

	t.Run("namedExec", func(t *testing.T) {
		var p personModel
		tx, err := db.Begin(ctx)
		require.NoError(t, err)
		defer func() {
			assert.Error(t, tx.Rollback())
		}()

		p1.DeletedAt = sql.NullTime{
			Time:  time.Now().UTC().Truncate(time.Second),
			Valid: true,
		}

		res, err := tx.NamedExec("UPDATE person_test SET deleted_at = :deleted_at WHERE id = :id", p1)
		assert.NoError(t, err)
		n, err := res.RowsAffected()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), n)
		// In transaction
		row := tx.QueryRow("SELECT * FROM person_test WHERE id = $1", p1.GetID())
		assert.NoError(t, row.Err())
		assert.NoError(t, row.Scan(&p.ID, &p.CreatedAt, &p.UpdatedAt, &p.DeletedAt, &p.Name, &p.Email))
		assertEqualPerson(t, p1, &p)
		assert.NoError(t, tx.Commit())
		// After commit
		row = db.QueryRow(ctx, "SELECT * FROM person_test WHERE id = $1", p1.GetID())
		assert.NoError(t, row.Err())
		assert.NoError(t, row.Scan(&p.ID, &p.CreatedAt, &p.UpdatedAt, &p.DeletedAt, &p.Name, &p.Email))
		assertEqualPerson(t, p1, &p)
	})

	t.Run("exec", func(t *testing.T) {
		tx, err := db.Begin(ctx)
		require.NoError(t, err)
		defer func() {
			assert.Error(t, tx.Rollback())
		}()

		res, err := tx.Exec(personExecQ, p2.ID, p2.CreatedAt, p2.UpdatedAt, nil, p2.Name, p2.Email)
		assert.NoError(t, err)
		n, err := res.RowsAffected()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), n)
		assert.NoError(t, tx.Commit())
	})

	t.Run("exec (clear table)", func(t *testing.T) {
		_, err := db.Exec(ctx, "DELETE FROM person_test")
		assert.NoError(t, err)
	})
}

func TestDBQueriesRebind(t *testing.T) {
	db, err := New(postgresDataSource, WithRebindModel())
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	p1 := &personModelBinded{
		personModel: personModel{
			Name:  "Lucky Luke",
			Email: NullString("lucky@example.com"),
		},
	}

	ctx := context.Background()
	t.Run("insert", func(t *testing.T) {
		assert.NoError(t, db.Insert(ctx, p1))
	})

	t.Run("select", func(t *testing.T) {
		var pp personModelBinded
		assert.NoError(t, db.Select(ctx, &pp, p1.GetID()))
		assertEqualPerson(t, &p1.personModel, &pp.personModel)
	})

	t.Run("delete", func(t *testing.T) {
		var pp personModelBinded
		assert.NoError(t, db.Delete(ctx, p1))
		assert.Error(t, db.Select(ctx, &pp, p1.GetID()))
	})

	t.Run("hardDelete", func(t *testing.T) {
		var pp personModelBinded
		assert.NoError(t, db.HardDelete(ctx, p1))
		assert.Error(t, db.Select(ctx, &pp, p1.GetID()))
	})

	t.Run("exec (clear table)", func(t *testing.T) {
		_, err := db.Exec(ctx, "DELETE FROM person_test")
		assert.NoError(t, err)
	})
}

func TestTXQueriesRebind(t *testing.T) {
	db, err := New(postgresDataSource, WithRebindModel())
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	p1 := &personModelBinded{
		personModel: personModel{
			Name:  "Lucky Luke",
			Email: NullString("lucky@example.com"),
		},
	}

	ctx := context.Background()
	t.Run("insert", func(t *testing.T) {
		assert.NoError(t, db.Insert(ctx, p1))
	})

	t.Run("select", func(t *testing.T) {
		var pp personModelBinded
		tx, err := db.Begin(ctx)
		require.NoError(t, err)
		defer func() {
			assert.Error(t, tx.Rollback())
		}()
		assert.NoError(t, tx.Select(&pp, p1.GetID()))
		assertEqualPerson(t, &p1.personModel, &pp.personModel)
		assert.NoError(t, tx.Commit())
	})

	t.Run("delete", func(t *testing.T) {
		var pp personModelBinded
		tx, err := db.Begin(ctx)
		require.NoError(t, err)
		defer func() {
			assert.Error(t, tx.Rollback())
		}()
		assert.NoError(t, tx.Delete(p1))
		assert.Error(t, tx.Select(&pp, p1.GetID()))
		assert.NoError(t, tx.Commit())
	})

	t.Run("hardDelete", func(t *testing.T) {
		var pp personModelBinded
		tx, err := db.Begin(ctx)
		require.NoError(t, err)
		defer func() {
			assert.Error(t, tx.Rollback())
		}()
		assert.NoError(t, tx.HardDelete(p1))
		assert.Error(t, tx.Select(&pp, p1.GetID()))
		assert.NoError(t, tx.Commit())
	})

	t.Run("exec (clear table)", func(t *testing.T) {
		_, err := db.Exec(ctx, "DELETE FROM person_test")
		assert.NoError(t, err)
	})
}

func TestDB_Rebind(t *testing.T) {
	db, err := New(postgresDataSource)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	type fields struct {
		db    *sqlx.DB
		clock clock.Clock
	}
	type args struct {
		query string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{"ok", fields{db.db, db.clock}, args{"SELECT * FROM person_test WHERE id = ?"}, "SELECT * FROM person_test WHERE id = $1"},
		{"ok multiple", fields{db.db, db.clock}, args{"SELECT * FROM person_test WHERE name = ? AND email = ?"}, "SELECT * FROM person_test WHERE name = $1 AND email = $2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DB{
				db:    tt.fields.db,
				clock: tt.fields.clock,
			}
			assert.Equal(t, tt.want, d.Rebind(tt.args.query))
		})
	}
}

func TestDB_Driver(t *testing.T) {
	db, err := New(postgresDataSource)
	require.NoError(t, err)
	assert.Equal(t, "pgx/v5", db.Driver())
	assert.NoError(t, db.Close())

	db, err = New(postgresDataSource, WithDriver("pgx/v5"))
	require.NoError(t, err)
	assert.Equal(t, "pgx/v5", db.Driver())
	assert.NoError(t, db.Close())
}
