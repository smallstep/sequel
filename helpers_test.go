package sequel

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHelpers(t *testing.T) {
	assert.Equal(t, sql.NullBool{Bool: true, Valid: true}, NullBool(true))
	assert.Equal(t, sql.NullBool{Bool: false, Valid: false}, NullBool(false))

	assert.Equal(t, sql.NullByte{Byte: 1, Valid: true}, NullByte(1))
	assert.Equal(t, sql.NullByte{Byte: 0, Valid: false}, NullByte(0))

	assert.Equal(t, sql.NullFloat64{Float64: 1.1, Valid: true}, NullFloat64(1.1))
	assert.Equal(t, sql.NullFloat64{Float64: 0, Valid: false}, NullFloat64(0))

	assert.Equal(t, sql.NullInt16{Int16: 1, Valid: true}, NullInt16(1))
	assert.Equal(t, sql.NullInt16{Int16: 0, Valid: false}, NullInt16(0))

	assert.Equal(t, sql.NullInt32{Int32: 1, Valid: true}, NullInt32(1))
	assert.Equal(t, sql.NullInt32{Int32: 0, Valid: false}, NullInt32(0))

	assert.Equal(t, sql.NullInt64{Int64: 1, Valid: true}, NullInt64(1))
	assert.Equal(t, sql.NullInt64{Int64: 0, Valid: false}, NullInt64(0))

	assert.Equal(t, sql.NullString{String: "abc", Valid: true}, NullString("abc"))
	assert.Equal(t, sql.NullString{String: "", Valid: false}, NullString(""))

	now := time.Now()
	assert.Equal(t, sql.NullTime{Time: now, Valid: true}, NullTime(now))
	assert.Equal(t, sql.NullTime{Time: time.Time{}, Valid: false}, NullTime(time.Time{}))
}
