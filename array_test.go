package sequel

import (
	"context"
	"database/sql"
	"fmt"
	"net/netip"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.step.sm/qb"
)

var arraySelectQ, arrayInsertQ, arrayUpdateQ, arrayDeleteQ string

func init() {
	builder := qb.Must(&arrayModel{})
	arraySelectQ, arrayInsertQ, arrayUpdateQ, arrayDeleteQ = Queries(builder)
}

type arrayModel struct {
	Base     `dbtable:"array_test"`
	CIDRs    Array[netip.Prefix] `db:"cidrs"`
	Integers Array[int]          `db:"integers"`
	Varchars Array[string]       `db:"varchars"`
	Texts    Array[string]       `db:"texts"`
}

func (m *arrayModel) Select() string { return arraySelectQ }
func (m *arrayModel) Insert() string { return arrayInsertQ }
func (m *arrayModel) Update() string { return arrayUpdateQ }
func (m *arrayModel) Delete() string { return arrayDeleteQ }

func TestArray_Scan(t *testing.T) {
	db, err := New(postgresDataSource)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	m1 := &arrayModel{
		CIDRs: []netip.Prefix{
			netip.MustParsePrefix("127.0.0.1/32"),
			netip.MustParsePrefix("192.168.10.0/24"),
			netip.MustParsePrefix("10.10.0.0/16"),
			netip.MustParsePrefix("10.0.0.0/8"),
			netip.MustParsePrefix("2001:4f8:3:ba::/64"),
			netip.MustParsePrefix("2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128"),
		},
		Integers: []int{2, 3, 5, 7, 11, 13, 17, 19},
		Varchars: []string{"foo", "bar", "foobar"},
		Texts:    []string{"foobar", "barzar"},
	}
	m2 := &arrayModel{
		CIDRs:    nil,
		Integers: []int{2, 3, 5, 7, 11, 13, 17, 19},
		Varchars: []string{},
		Texts:    []string{"foobar", "barzar"},
	}
	m3 := &arrayModel{}

	// Insert all
	ctx, cancel := Context(context.Background())
	defer cancel()
	err = db.InsertBatch(ctx, []Model{m1, m2, m3})
	require.NoError(t, err)

	// Not found
	t.Run("not found", func(t *testing.T) {
		var m arrayModel

		ctx, cancel := Context(context.Background())
		defer cancel()

		err = db.Select(ctx, &m, "5dab8df0-a7f5-426b-9fb9-01aef6acf7bc")
		assert.Error(t, err)
		assert.Equal(t, sql.ErrNoRows, err)
		assert.Equal(t, arrayModel{}, m)
	})

	// Select one by one
	for i, m := range []*arrayModel{m1, m2, m3} {
		t.Run(fmt.Sprintf("select m%d", i+1), func(t *testing.T) {
			var dst arrayModel

			ctx, cancel := Context(context.Background())
			defer cancel()

			err = db.Select(ctx, &dst, m.GetID())
			assert.NoError(t, err)
			dst.CreatedAt = dst.CreatedAt.UTC()
			dst.UpdatedAt = dst.UpdatedAt.UTC()
			assert.Equal(t, m, &dst)
		})
	}

	// Select all
	t.Run("select all", func(t *testing.T) {
		var arrays []*arrayModel

		ctx, cancel := Context(context.Background())
		defer cancel()

		err = db.GetAll(ctx, &arrays, "SELECT * FROM array_test")
		assert.NoError(t, err)
		assert.Len(t, arrays, 3)

		for i := range arrays {
			arrays[i].CreatedAt = arrays[i].CreatedAt.UTC()
			arrays[i].UpdatedAt = arrays[i].UpdatedAt.UTC()
		}

		assert.ElementsMatch(t, []*arrayModel{m1, m2, m3}, arrays)
	})

	t.Run("scan fail", func(t *testing.T) {
		var a Array[arrayModel]
		assert.Error(t, a.Scan(nil))

		var b Array[int]
		assert.Error(t, b.Scan(`{foo,bar}`))
	})
}

func TestArrayScan(t *testing.T) {
	var gotInts []int
	assert.NoError(t, ArrayScan(pgtype.Int4ArrayOID, `{1,2,3,4,5}`, &gotInts))
	assert.Equal(t, []int{1, 2, 3, 4, 5}, gotInts)

	var gotStrings []string
	assert.NoError(t, ArrayScan[string](pgtype.TextArrayOID, []byte(`{foo,bar,zar}`), &gotStrings))
	assert.Equal(t, []string{"foo", "bar", "zar"}, gotStrings)

	var badOID []int
	assert.Error(t, ArrayScan(pgtype.CIDArrayOID, []byte(`{1,2,3,4,5}`), &badOID))
	assert.Nil(t, badOID)

	var badSrc []int
	assert.Error(t, ArrayScan(pgtype.CIDArrayOID, []byte(`123`), &badSrc))
	assert.Nil(t, badSrc)

	var badDest []int
	assert.Error(t, ArrayScan(pgtype.TextArrayOID, []byte(`{foo,bar,zar}`), &badDest))
	assert.Nil(t, badDest)

	var badType []int
	assert.Error(t, ArrayScan(pgtype.TextArrayOID, []int{1, 2, 3, 4, 5}, &badType))
	assert.Nil(t, badType)
}
