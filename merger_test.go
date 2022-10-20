package leveldb

import (
	"fmt"
	"os"
	"testing"
)

func TestMerger(t *testing.T) {
	os.RemoveAll("test")
	os.Mkdir("test", os.ModePerm)
	m1 := newMemoryOnlySegment()
	for i := 0; i < 100000; i++ {
		m1.Put([]byte(fmt.Sprint("mykey", i)), []byte(fmt.Sprint("myvalue", i)))
	}
	m2 := newMemoryOnlySegment()
	for i := 100000; i < 200000; i++ {
		m2.Put([]byte(fmt.Sprint("mykey", i)), []byte(fmt.Sprint("myvalue", i)))
	}

	merged, err := mergeSegments1(newNullDeleter(), "test", []segment{m1, m2}, false)
	if err != nil {
		t.Fatal(err)
	}

	itr, err := merged.Lookup(nil, nil)
	count := 0

	for {
		_, _, err := itr.Next()
		if err != nil {
			break
		}
		count++
	}

	if count != 200000 {
		t.Fatal("wrong number of records", count)
	}
}

func TestMergerRemove(t *testing.T) {
	os.RemoveAll("test")
	os.Mkdir("test", os.ModePerm)
	m1 := newMemoryOnlySegment()
	for i := 0; i < 100000; i++ {
		m1.Put([]byte(fmt.Sprint("mykey", i)), []byte(fmt.Sprint("myvalue", i)))
	}
	m2 := newMemoryOnlySegment()
	for i := 0; i < 100000; i++ {
		m2.Remove([]byte(fmt.Sprint("mykey", i)))
	}

	merged, err := mergeSegments1(newNullDeleter(), "test", []segment{m1, m2}, false)
	if err != nil {
		t.Fatal(err)
	}

	itr, err := merged.Lookup(nil, nil)
	count := 0

	for {
		_, _, err := itr.Next()
		if err != nil {
			break
		}
		count++
	}

	if count != 100000 { // without purge, should have entries
		t.Fatal("wrong number of records", count)
	}
}
func TestMergerRemoveWithPurge(t *testing.T) {
	os.RemoveAll("test")
	os.Mkdir("test", os.ModePerm)
	m1 := newMemoryOnlySegment()
	for i := 0; i < 100000; i++ {
		m1.Put([]byte(fmt.Sprint("mykey", i)), []byte(fmt.Sprint("myvalue", i)))
	}
	m2 := newMemoryOnlySegment()
	for i := 0; i < 100000; i++ {
		m2.Remove([]byte(fmt.Sprint("mykey", i)))
	}

	merged, err := mergeSegments1(newNullDeleter(), "test", []segment{m1, m2}, true)
	if err != nil {
		t.Fatal(err)
	}

	itr, err := merged.Lookup(nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for {
		_, _, err := itr.Next()
		if err != nil {
			break
		}
		count++
	}

	if count != 0 {
		t.Fatal("wrong number of records", count)
	}
}
