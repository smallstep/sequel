package sequel

import (
	"database/sql"
	"time"
)

// NullBool is a helper that returns a sql.NullBool with the valid set to false
// if the zero value is given.
func NullBool(b bool) sql.NullBool {
	return sql.NullBool{
		Bool: b, Valid: b,
	}
}

// NullByte is a helper that returns a sql.NullByte with the valid set to false
// if the zero value is given.
func NullByte(b byte) sql.NullByte {
	return sql.NullByte{
		Byte: b, Valid: b != 0,
	}
}

// NullFloat64 is a helper that returns a sql.NullFloat64 with the valid set to
// false if the zero value is given.
func NullFloat64(f float64) sql.NullFloat64 {
	return sql.NullFloat64{
		Float64: f, Valid: f != 0,
	}
}

// NullInt16 is a helper that returns a sql.NullInt16 with the valid set to
// false if the zero value is given.
func NullInt16(i int16) sql.NullInt16 {
	return sql.NullInt16{
		Int16: i, Valid: i != 0,
	}
}

// NullInt32 is a helper that returns a sql.NullInt32 with the valid set to
// false if the zero value is given.
func NullInt32(i int32) sql.NullInt32 {
	return sql.NullInt32{
		Int32: i, Valid: i != 0,
	}
}

// NullInt64 is a helper that returns a sql.NullInt64 with the valid set to
// false if the zero value is given.
func NullInt64(i int64) sql.NullInt64 {
	return sql.NullInt64{
		Int64: i, Valid: i != 0,
	}
}

// NullString is a helper that returns a sql.NullString with the valid set to
// false if the zero value is given.
func NullString(s string) sql.NullString {
	return sql.NullString{
		String: s, Valid: s != "",
	}
}

// NullTime is a helper that returns a sql.NullTime with the valid set to false
// if the zero value is given.
func NullTime(t time.Time) sql.NullTime {
	return sql.NullTime{
		Time: t, Valid: !t.IsZero(),
	}
}
