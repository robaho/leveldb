package leveldb

import (
	"bytes"
	"testing"
)

func TestMemorySegment_Put(t *testing.T) {
	ms := newMemoryOnlySegment()
	ms.Put([]byte("mykey"), []byte("myvalue"))
	val, _ := ms.Get([]byte("mykey"))
	if !bytes.Equal(val, []byte("myvalue")) {
		t.Fail()
	}
}

func TestMemorySegment_Remove(t *testing.T) {
	ms := newMemoryOnlySegment()
	ms.Put([]byte("mykey"), []byte("myvalue"))
	val, err := ms.Remove([]byte("mykey"))
	if err != nil || !bytes.Equal(val, []byte("myvalue")) {
		t.Fail()
	}
}

func TestMemorySegment_UserKeyOrder(t *testing.T) {
	// simple compare that reverses order
	kc := func(a, b []byte) int {
		return -1 * bytes.Compare(a, b)
	}
	ms := newMemorySegment("", 0, Options{UserKeyCompare: kc})
	ms.Put([]byte("mykey1"), []byte("myvalue1"))
	ms.Put([]byte("mykey2"), []byte("myvalue2"))

	itr, err := ms.Lookup(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	k, v, err := itr.Next()
	if "mykey2" != string(k) {
		t.Fatal("wrong order")
	}
	if "myvalue2" != string(v) {
		t.Fatal("wrong value")
	}
}
