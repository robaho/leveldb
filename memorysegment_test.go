package leveldb

import (
	"bytes"
	"testing"
)

func TestMemorySegment_Put(t *testing.T) {
	ms := newMemorySegment("", 0)
	ms.Put([]byte("mykey"), []byte("myvalue"))
	val, _ := ms.Get([]byte("mykey"))
	if !bytes.Equal(val, []byte("myvalue")) {
		t.Fail()
	}
}

func TestMemorySegment_Remove(t *testing.T) {
	ms := newMemorySegment("", 0)
	ms.Put([]byte("mykey"), []byte("myvalue"))
	val, err := ms.Remove([]byte("mykey"))
	if err != nil || !bytes.Equal(val, []byte("myvalue")) {
		t.Fail()
	}
}
