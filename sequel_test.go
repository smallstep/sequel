package sequel

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.step.sm/qb"

	"go.step.sm/sequel/clock"
)

var personSelectQ, personInsertQ, personUpdateQ, personDeleteQ string
var personInsertExecQ, personHardDeleteQ string

func init() {
	builder := qb.Must(&personModel{})
	personSelectQ, personInsertQ, personUpdateQ, personDeleteQ = Queries(builder)
	personInsertExecQ = builder.NamedInsert()
	personHardDeleteQ = builder.HardDelete()
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
	equalPerson := func(t *testing.T, want, got *personModel) bool {
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
	equalPersons := func(t *testing.T, want, got []*personModel) bool {
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
			equalPerson(t, p1, &p)
		}
		assert.NoError(t, rows.Err())
		assert.NoError(t, rows.Close()) //nolint:sqlclosecheck // no defer for testing purposes
	})

	t.Run("queryRow", func(t *testing.T) {
		var p personModel
		row := db.QueryRow(ctx, "SELECT * FROM person_test WHERE id = $1", p1.GetID())
		assert.NoError(t, err)
		assert.NoError(t, row.Scan(&p.ID, &p.CreatedAt, &p.UpdatedAt, &p.DeletedAt, &p.Name, &p.Email))
		equalPerson(t, p1, &p)
	})

	t.Run("get", func(t *testing.T) {
		var pp1, pp2 personModel
		assert.NoError(t, db.Get(ctx, &pp1, "SELECT * FROM person_test WHERE id = $1", p1.GetID()))
		equalPerson(t, p1, &pp1)
		assert.Equal(t, sql.ErrNoRows, db.Get(ctx, &pp2, "SELECT * FROM person_test WHERE id = $1 AND deleted_at IS NOT NULL", p1.GetID()))
		equalPerson(t, &personModel{}, &pp2)
	})

	t.Run("getAll", func(t *testing.T) {
		var ap []*personModel
		assert.NoError(t, db.GetAll(ctx, &ap, "SELECT * FROM person_test"))
		equalPersons(t, []*personModel{p1, p2, p3, &p4.personModel, &p5.personModel}, ap)
		assert.NoError(t, db.GetAll(ctx, &ap, "SELECT * FROM person_test WHERE deleted_at IS NOT NULL"))
		equalPersons(t, []*personModel{}, ap)
	})

	t.Run("select", func(t *testing.T) {
		var pp1, pp2 personModel
		assert.NoError(t, db.Select(ctx, &pp1, p2.GetID()))
		equalPerson(t, p2, &pp1)
		assert.Equal(t, sql.ErrNoRows, db.Select(ctx, &pp2, "cf349a3d-7bc7-4208-bb73-1b1651e80540"))
		equalPerson(t, &personModel{}, &pp2)
	})

	t.Run("update", func(t *testing.T) {
		var pp personModel
		p3.Name = "Averell Dalton"
		p3.Email = NullString("averell@example.com")
		assert.NoError(t, db.Update(ctx, p3))
		assert.NoError(t, db.Select(ctx, &pp, p3.GetID()))
		equalPerson(t, p3, &pp)
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

	t.Run("exec (clear table)", func(t *testing.T) {
		res, err := db.Exec(ctx, "DELETE FROM person_test")
		assert.NoError(t, err)
		assert.NoError(t, RowsAffected(res, 4)) // p1 to p4, p5 is hard deleted
	})
}
