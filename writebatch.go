package leveldb

type WriteBatch struct {
	entries []KeyValue
}

func (wb *WriteBatch) Put(key []byte, value []byte) {
	wb.entries = append(wb.entries, KeyValue{key: key, value: value})
}

func (wb *WriteBatch) Remove(key []byte) {
	wb.entries = append(wb.entries, Key(key))
}
