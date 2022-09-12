package leveldb_test

import (
	"bytes"
	"fmt"
	"github.com/robaho/leveldb"
	"io/ioutil"
	"strings"
	"testing"
)

var options = leveldb.Options{CreateIfNeeded: true, DisableBgMerge: true}

func TestDatabase(t *testing.T) {
	leveldb.Remove("test/mydb")

	db, err := leveldb.Open("test/mydb", options)
	if err != nil {
		t.Fatal("unable to create database", err)
	}

	err = db.Put([]byte("mykey"), []byte("myvalue"))
	if err != nil {
		t.Fatal("unable to put key/value", err)
	}
	_, err = db.Get([]byte("mykey"))
	if err != nil {
		t.Fatal("unable to get by key", err)
	}
	err = db.Put([]byte("mykey2"), []byte("myvalue2"))
	if err != nil {
		t.Fatal("unable to put key/value", err)
	}
	_, err = db.Get([]byte("mykey2"))
	if err != nil {
		t.Fatal("unable to get by key", err)
	}

	large := make([]byte, 1025)
	err = db.Put(large, []byte("myvalue"))
	if err == nil {
		t.Fatal("should not of been able to Put a large key")
	}
	_, err = db.Remove([]byte("mykey"))
	if err != nil {
		t.Fatal("unable to remove by key", err)
	}
	_, err = db.Get([]byte("mykey"))
	if err != leveldb.KeyNotFound {
		t.Fatal("should not of found removed key")
	}
	err = db.CloseWithMerge(1)
	if err != nil {
		t.Fatal("unable to close database", err)
	}

	db, err = leveldb.Open("test/mydb", options)
	if err != nil {
		t.Fatal("unable to create database", err)
	}

	_, err = db.Get([]byte("mykey"))
	if err != leveldb.KeyNotFound {
		t.Fatal("should not of found removed key")
	}
}

func TestDatabaseIterator(t *testing.T) {
	leveldb.Remove("test/mydb")

	db, err := leveldb.Open("test/mydb", options)
	if err != nil {
		t.Fatal("unable to create database", err)
	}
	err = db.Put([]byte("mykey"), []byte("myvalue"))
	if err != nil {
		t.Fatal("unable to put key/Value", err)
	}
	err = db.Put([]byte("mykey2"), []byte("myvalue2"))
	if err != nil {
		t.Fatal("unable to put key/Value", err)
	}
	err = db.Put([]byte("mykey3"), []byte("myvalue3"))
	if err != nil {
		t.Fatal("unable to put key/Value", err)
	}
	itr, err := db.Lookup([]byte("mykey2"), nil)

	key, value, err := itr.Next()
	if err != nil {
		t.Fatal("iterator failed", err)
	}
	if !bytes.Equal(key, []byte("mykey2")) {
		t.Fatal("wrong key", string(key), "mykey2")
	}
	if !bytes.Equal(value, []byte("myvalue2")) {
		t.Fatal("wrong Value", string(key), "myvalue2")
	}
	key, value, err = itr.Next()
	if err != nil {
		t.Fatal("iterator failed", err)
	}
	if !bytes.Equal(key, []byte("mykey3")) {
		t.Fatal("wrong key", string(key), "mykey3")
	}
	if !bytes.Equal(value, []byte("myvalue3")) {
		t.Fatal("wrong Value", string(key), "myvalue3")
	}
	itr, err = db.Lookup(nil, []byte("mykey2"))
	key, value, err = itr.Next()
	if err != nil {
		t.Fatal("iterator failed", err)
	}
	key, value, err = itr.Next()
	if err != nil {
		t.Fatal("iterator failed", err)
	}
	itr, err = db.Lookup([]byte("mykey2"), []byte("mykey2"))
	key, value, err = itr.Next()
	if err != nil {
		t.Fatal("iterator failed", err)
	}
	itr, err = db.Lookup([]byte("mykey4"), nil)

	err = db.Close()
	if err != nil {
		t.Fatal("unable to close database", err)
	}
}

func TestSegmentMerge(t *testing.T) {
	leveldb.Remove("test/mydb")

	nsegs := 100
	// open, write, close to force disk segment creation

	for i := 0; i < nsegs; i++ {
		db, err := leveldb.Open("test/mydb", options)
		if err != nil {
			t.Fatal("unable to create database", err)
		}

		for i := 0; i < 100; i++ {
			err = db.Put([]byte(fmt.Sprint("mykey", i)), []byte(fmt.Sprint("myvalue", i)))
			if err != nil {
				t.Fatal("unable to put key/Value", err)
			}
		}

		db.CloseWithMerge(0)
	}

	count := countFiles("test/mydb")
	if count != nsegs*2 {
		t.Fatal("there should be ", nsegs*2, " files, count is ", count)
	}

	db, err := leveldb.Open("test/mydb", options)
	db.CloseWithMerge(1)

	count = countFiles("test/mydb")

	if count != 2 { // there are two files for every segment
		t.Fatal("there should only be 2 files at this point, count is ", count)
	}

	db, err = leveldb.Open("test/mydb", options)
	if err != nil {
		t.Fatal("unable to open database", err)
	}
	itr, err := db.Lookup(nil, nil)
	count = 0
	for {
		_, _, err = itr.Next()
		if err != nil {
			break
		}
		count++
	}
	if count != 100 {
		t.Fatal("incorrect count, should be 100, is ", count)
	}
}

func countFiles(path string) int {
	files, _ := ioutil.ReadDir(path)
	count := 0
	for _, file := range files {
		if strings.Index(file.Name(), "keys.") >= 0 || strings.Index(file.Name(), "data.") >= 0 {
			count++
		}
	}
	return count
}

func TestPersistence(t *testing.T) {
	leveldb.Remove("test/mydb")

	db, err := leveldb.Open("test/mydb", options)
	if err != nil {
		t.Fatal("unable to create database", err)
	}

	err = db.Put([]byte("mykey"), []byte("myvalue"))
	if err != nil {
		t.Fatal("unable to put key/Value", err)
	}

	db.Close()

	db, err = leveldb.Open("test/mydb", options)
	if err != nil {
		t.Fatal("database did not exist", err)
	}

	val, err := db.Get([]byte("mykey"))
	if err != nil {
		t.Fatal("unable to get by key", err)
	}
	if string(val) != "myvalue" {
		t.Fatal("incorrect value", string(val), "expected myvalue")
	}

	err = db.Close()
	if err != nil {
		t.Fatal("unable to close database", err)
	}
}

func TestRemovedKeys(t *testing.T) {
	leveldb.Remove("test/mydb")

	db, err := leveldb.Open("test/mydb", options)
	if err != nil {
		t.Fatal("unable to create database", err)
	}

	err = db.Put([]byte("mykey"), []byte("myvalue"))
	if err != nil {
		t.Fatal("unable to put key/Value", err)
	}
	_, err = db.Get([]byte("mykey"))
	if err != nil {
		t.Fatal("unable to get by key", err)
	}

	err = db.CloseWithMerge(1)
	if err != nil {
		t.Fatal("unable to close database", err)
	}

	db, err = leveldb.Open("test/mydb", options)
	if err != nil {
		t.Fatal("unable to create database", err)
	}

	_, err = db.Remove([]byte("mykey"))
	if err != nil {
		t.Fatal("unable to remove key", err)
	}
	_, err = db.Get([]byte("mykey"))
	if err != leveldb.KeyNotFound {
		t.Fatal("should not of found key", err)
	}
	err = db.CloseWithMerge(1)
	db, err = leveldb.Open("test/mydb", options)
	if err != nil {
		t.Fatal("unable to create database", err)
	}
	_, err = db.Get([]byte("mykey"))
	if err != leveldb.KeyNotFound {
		t.Fatal("should not of found key", err)
	}
	itr, err := db.Lookup(nil, nil)
	if err != nil {
		t.Fatal("unable to open iterator", err)
	}
	key, _, err := itr.Next()
	if err != leveldb.EndOfIterator {
		t.Fatal("iterator should be empty key = "+string(key), err)
	}
	err = db.CloseWithMerge(1)
}
