package leveldb

type KeyValue struct {
	key   []byte
	value []byte
}

func Key(key []byte) KeyValue {
	return KeyValue{key: key}
}
