package cache

import (
	"errors"

	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	ItemDataTypeMap = iota
	ItemDataTypeArrowTable
	ItemDataTypeJsonValue
	ItemDataTypeBytes
	ItemDataTypeString
	ItemDataTypeResponse
	ItemNil
)

type CacheItem struct {
	Type int8
	Data any
}

func NewCacheItem(data any) (*CacheItem, error) {
	if data == nil {
		return &CacheItem{
			Type: ItemNil,
			Data: nil,
		}, nil
	}
	switch v := data.(type) {
	case map[string]any:
		return &CacheItem{
			Type: ItemDataTypeMap,
			Data: v,
		}, nil
	case db.ArrowTable:
		return &CacheItem{
			Type: ItemDataTypeArrowTable,
			Data: v,
		}, nil
	case *db.JsonValue:
		return &CacheItem{
			Type: ItemDataTypeJsonValue,
			Data: v,
		}, nil
	case []db.JsonValue:
		return &CacheItem{
			Type: ItemDataTypeJsonValue,
			Data: v,
		}, nil
	case []byte:
		return &CacheItem{
			Type: ItemDataTypeBytes,
			Data: v,
		}, nil
	case string:
		return &CacheItem{
			Type: ItemDataTypeString,
			Data: v,
		}, nil
	default:
		return nil, errors.New("not supported cache item data type")
	}
}

var _ msgpack.CustomDecoder = (*CacheItem)(nil)

func (item *CacheItem) DecodeMsgpack(dec *msgpack.Decoder) (err error) {
	item.Type, err = dec.DecodeInt8()
	if err != nil {
		return err
	}
	switch item.Type {
	case ItemDataTypeMap:
		item.Data, err = dec.DecodeMap()
	case ItemDataTypeArrowTable:
		data := new(db.ArrowTableStream)
		err = dec.Decode(data)
		item.Data = data
	case ItemDataTypeJsonValue:
		data, err := dec.DecodeString()
		if err != nil {
			return err
		}
		val := db.JsonValue(data)
		item.Data = &val
	case ItemDataTypeBytes:
		item.Data, err = dec.DecodeBytes()
	case ItemDataTypeString:
		item.Data, err = dec.DecodeString()
	case ItemNil:
		item.Data = nil
		err = dec.DecodeNil()
	default:
		return errors.New("not supported cache item data type")
	}
	return err
}

var _ msgpack.CustomEncoder = (*CacheItem)(nil)

func (item *CacheItem) EncodeMsgpack(enc *msgpack.Encoder) error {
	if err := enc.EncodeInt8(item.Type); err != nil {
		return err
	}
	if item.Data == nil {
		enc.EncodeNil()
	}
	return enc.Encode(item.Data)
}
