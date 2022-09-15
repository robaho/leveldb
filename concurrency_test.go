package leveldb_test

import (
	"fmt"
	"github.com/robaho/leveldb"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// Test using multiple writers and readers.
func TestConcurrency(t *testing.T) {
	leveldb.Remove("test/mydb")

	db, err := leveldb.Open("test/mydb", leveldb.Options{CreateIfNeeded: true})
	if err != nil {
		t.Fatal("unable to create database", err)
	}

	nrecs := 1000000

	wg := sync.WaitGroup{}

	writer := func(prefix string) {
		for i := 0; i < nrecs; i++ {
			err := db.Put([]byte(prefix+strconv.Itoa(i)), []byte("myvalue"+strconv.Itoa(i)))
			if err != nil {
				t.Fatal("unable to put key/Value", err)
			}
		}
		wg.Done()
	}

	reader := func(prefix string) {
		for i := 0; i < nrecs; i++ {
			j := rand.Intn(nrecs)
			_, err := db.Get([]byte(prefix + strconv.Itoa(j)))
			if err != nil && err != leveldb.KeyNotFound {
				t.Fatal("unable to get key/Value", err)
			}
		}
		fmt.Print("reader done\n")
		wg.Done()
	}

	wg.Add(4)

	go writer("prefixa")
	go writer("prefixb")
	go reader("prefixa")
	go reader("prefixb")

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
		if !strings.HasPrefix(string(key), "prefixa") && !strings.HasPrefix(string(key), "prefixb") {
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

	err = db.Close()
	if err != nil {
		t.Fatal("unable to close database", err)
	}
}
