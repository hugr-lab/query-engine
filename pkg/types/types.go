package types

import "time"

type ScalarTypes interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64 | ~string | ~bool | Int32Range | Int64Range | TimeRange | BaseRange | time.Time
}
