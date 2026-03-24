package sequel

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// Executor is the interface for types that can execute queries. Both [DB] and
// [Tx] satisfy this interface.
type Executor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// BatchSize is the maximum number of records inserted per statement.
const BatchSize = 100

// Batch inserts a slice of items into the given table using multi-row INSERT
// statements. Items are inserted in chunks of [BatchSize]. The columns
// parameter specifies the column names, and the values function maps each item to
// its column values. The length of the slice returned by values must match the
// length of columns. Batch does nothing if items is empty. Table, columns and onConfict
// are not sanitized; they must come from a trusted source. The values function will
// never be called concurrently.
func Batch[T any](ctx context.Context, exec Executor, table string, columns []string, onConflict string, items []T, values func(T) []any) error {
	for i := 0; i < len(items); i += BatchSize {
		end := min(i+BatchSize, len(items))
		query, args := batchQuery(table, columns, onConflict, items[i:end], values)
		if _, err := exec.ExecContext(ctx, query, args...); err != nil {
			return err
		}
	}
	return nil
}

func batchQuery[T any](table string, columns []string, onConflict string, items []T, values func(T) []any) (string, []any) {
	ncols := len(columns)
	args := make([]any, 0, len(items)*ncols)

	var b strings.Builder
	fmt.Fprintf(&b, "INSERT INTO %s (%s) VALUES ", table, strings.Join(columns, ", "))

	for i, item := range items {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteByte('(')
		vals := values(item)
		for j, v := range vals {
			if j > 0 {
				b.WriteString(", ")
			}
			fmt.Fprintf(&b, "$%d", i*ncols+j+1)
			args = append(args, v)
		}
		b.WriteByte(')')
	}

	fmt.Fprintf(&b, " %s", onConflict)

	return b.String(), args
}
