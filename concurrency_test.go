package leveldb_test

import (
	"github.com/robaho/leveldb"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestConcurrentWriter(t *testing.T) {
	leveldb.Remove("test/mydb")

	db, err := leveldb.Open("test/mydb", leveldb.Options{CreateIfNeeded: true})
	if err != nil {
		t.Fatal("unable to create database", err)
	}

	nrecs := 1000000

	wg := sync.WaitGroup{}

	f := func(prefix string) {
		for i := 0; i < nrecs; i++ {
			err = db.Put([]byte(prefix+strconv.Itoa(i)), []byte("myvalue"+strconv.Itoa(i)))
			if err != nil {
				t.Fatal("unable to put key/Value", err)
			}
		}
		wg.Done()
	}

	wg.Add(2)

	go f("writera")
	go f("writerb")

	wg.Wait()

	itr, err := db.Lookup(nil, nil)
	if err != nil {
		t.Fatal("unable to open iterator", err)
	}
	count := 0
	for {
		key, value, err := itr.Next()
		if err == leveldb.EndOfIterator {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(string(key), "writera") && !strings.HasPrefix(string(key), "writerb") {
			t.Fatal("incorrect key", string(key))
		}
		if !strings.HasPrefix(string(value), "myvalue") {
			t.Fatal("incorrect value", string(value))
		}
		count++
	}
	if count != 1000000*2 {
		t.Fatal("incorrect record count, expected", nrecs*2, "received", count)
	}

	err = db.CloseWithMerge(1)
	if err != nil {
		t.Fatal("unable to close database", err)
	}
}
