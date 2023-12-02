package sequel

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
)

var defaultMap = pgtype.NewMap()

// Array is a generic type that implements the sql.Scanner interface.
type Array[T any] []T

// Scan implements the sql.Scanner interface on the Array.
func (a *Array[T]) Scan(src any) error {
	typ, ok := defaultMap.TypeForValue(pgtype.Array[T]{})
	if !ok {
		return fmt.Errorf("cannot type for %T", a)
	}

	var aa []T
	if err := ArrayScan[T](typ.OID, src, &aa); err != nil {
		return err
	}
	*a = aa
	return nil
}

// ArrayScan scans the source using the PostgresType with the given oid and
// stores the result in the destination.
func ArrayScan[T any](oid uint32, src any, dest *[]T) error {
	if src == nil {
		*dest = nil
		return nil
	}

	switch v := src.(type) {
	case []byte:
		var pgArray pgtype.Array[T]
		if err := defaultMap.Scan(oid, pgtype.TextFormatCode, v, &pgArray); err != nil {
			return err
		}
		*dest = pgArray.Elements
		return nil
	case string:
		return ArrayScan(oid, []byte(v), dest)
	default:
		return fmt.Errorf("unsupported type %T", v)
	}
}
