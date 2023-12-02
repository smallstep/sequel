package sequel

import (
	"database/sql"
	"time"

	"go.step.sm/qb"
)

// Model is the interface implemented by all the database models.
type Model interface {
	GetID() string
	SetID(id string)
	SetCreatedAt(t time.Time)
	SetUpdatedAt(t time.Time)
	SetDeletedAt(t time.Time)
	Select() string
	Insert() string
	Update() string
	Delete() string
}

// ModelWithHardDelete is the interface implemented by a model that can be hard
// deleted.
type ModelWithHardDelete interface {
	Model
	HardDelete() string
}

// ModelWithExecInsert is the interface implemented by a model which inserts
// already contains the id and are not returning one.
type ModelWithExecInsert interface {
	Model
	WithExecInsert()
}

type Base struct {
	ID        string       `db:"id"`
	CreatedAt time.Time    `db:"created_at"`
	UpdatedAt time.Time    `db:"updated_at"`
	DeletedAt sql.NullTime `db:"deleted_at"`
}

func (m Base) GetID() string             { return m.ID }
func (m *Base) SetID(id string)          { m.ID = id }
func (m *Base) SetCreatedAt(t time.Time) { m.CreatedAt = t }
func (m *Base) SetUpdatedAt(t time.Time) { m.UpdatedAt = t }
func (m *Base) SetDeletedAt(t time.Time) {
	m.DeletedAt = sql.NullTime{
		Valid: !t.IsZero(),
		Time:  t,
	}
}

func Queries(builder *qb.QueryBuilder) (selectQ, insertQ, updateQ, deleteQ string) {
	selectQ = builder.Select()
	insertQ = builder.NamedInsertWithReturning()
	updateQ = builder.NamedUpdate()
	deleteQ = builder.Delete()
	return
}
