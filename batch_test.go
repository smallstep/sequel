package sequel

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatch(t *testing.T) {
	db, err := New(postgresDataSource)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	ctx := context.Background()
	t.Cleanup(func() {
		_, _ = db.Exec(ctx, "DELETE FROM person_test WHERE name LIKE 'batch-%'")
	})

	type person struct {
		Name  string
		Email string
	}

	columns := []string{"name", "email"}
	fn := func(p person) []any {
		return []any{p.Name, p.Email}
	}

	t.Run("empty", func(t *testing.T) {
		err := Batch(ctx, nil, "person_test", columns, []person{}, fn)
		assert.NoError(t, err)
	})

	t.Run("single", func(t *testing.T) {
		items := []person{
			{Name: "batch-single", Email: "batch-single@example.com"},
		}
		require.NoError(t, Batch(ctx, db, "person_test", columns, items, fn))

		var name string
		err := db.QueryRow(ctx, "SELECT name FROM person_test WHERE email = $1", "batch-single@example.com").Scan(&name)
		require.NoError(t, err)
		assert.Equal(t, "batch-single", name)
	})

	t.Run("multiple", func(t *testing.T) {
		items := []person{
			{Name: "batch-multi-1", Email: "batch-multi-1@example.com"},
			{Name: "batch-multi-2", Email: "batch-multi-2@example.com"},
			{Name: "batch-multi-3", Email: "batch-multi-3@example.com"},
		}
		require.NoError(t, Batch(ctx, db, "person_test", columns, items, fn))

		for _, p := range items {
			var name string
			err := db.QueryRow(ctx, "SELECT name FROM person_test WHERE email = $1", p.Email).Scan(&name)
			require.NoError(t, err)
			assert.Equal(t, p.Name, name)
		}
	})

	t.Run("chunked", func(t *testing.T) {
		// Insert more than BatchSize to verify chunking.
		items := make([]person, BatchSize+5)
		for i := range items {
			items[i] = person{
				Name:  fmt.Sprintf("batch-chunk-%d", i),
				Email: fmt.Sprintf("batch-chunk-%d@example.com", i),
			}
		}
		require.NoError(t, Batch(ctx, db, "person_test", columns, items, fn))

		var count int
		err := db.QueryRow(ctx, "SELECT COUNT(*) FROM person_test WHERE name LIKE 'batch-chunk-%'").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, len(items), count)
	})

	t.Run("tx", func(t *testing.T) {
		tx, err := db.Begin(ctx)
		require.NoError(t, err)

		items := []person{
			{Name: "batch-tx-1", Email: "batch-tx-1@example.com"},
			{Name: "batch-tx-2", Email: "batch-tx-2@example.com"},
		}
		require.NoError(t, Batch(ctx, tx, "person_test", columns, items, fn))
		require.NoError(t, tx.Commit())

		var count int
		err = db.QueryRow(ctx, "SELECT COUNT(*) FROM person_test WHERE name LIKE 'batch-tx-%'").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 2, count)
	})
}
