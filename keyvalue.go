package leveldb

import "bytes"

type KeyValue struct {
	key   []byte
	value []byte
}

func KeyValueCompare(a KeyValue, b KeyValue) int {
	return bytes.Compare(a.key, b.key)
}

func Key(key []byte) KeyValue {
	return KeyValue{key: key}
}
