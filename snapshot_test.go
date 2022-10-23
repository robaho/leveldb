package leveldb_test

import (
	"bytes"
	"github.com/robaho/leveldb"
	"testing"
)

func TestSnapshot_Get(t *testing.T) {

	leveldb.Remove("test/mydb")

	db, err := leveldb.Open("test/mydb", options)
	if err != nil {
		t.Fatal("unable to create database", err)
	}

	err = db.Put([]byte("mykey"), []byte("myvalue"))
	if err != nil {
		t.Fatal("unable to put key/value", err)
	}

	s, err := db.Snapshot()
	if err != nil {
		t.Fatal("unable to get snapshot", err)
	}

	err = db.Put([]byte("mykey1"), []byte("myvalue1"))
	if err != nil {
		t.Fatal("unable to put key/value", err)
	}

	val, err := s.Get([]byte("mykey"))
	if err != nil {
		t.Fatal("unable to get key/value", err)
	}
	if !bytes.Equal([]byte("myvalue"), val) {
		t.Fatal("value does not match")
	}
	val, err = s.Get([]byte("keykey1"))
	if val != nil || err != leveldb.KeyNotFound {
		t.Fatal("should have return nil,KeyNotFound")
	}
	db.Close()
}

func TestSnapshot_Lookup(t *testing.T) {
	leveldb.Remove("test/mydb")

	db, err := leveldb.Open("test/mydb", options)
	if err != nil {
		t.Fatal("unable to create database", err)
	}

	err = db.Put([]byte("mykey"), []byte("myvalue"))
	if err != nil {
		t.Fatal("unable to put key/value", err)
	}

	s, err := db.Snapshot()
	if err != nil {
		t.Fatal("unable to get snapshot", err)
	}

	err = db.Put([]byte("mykey1"), []byte("myvalue1"))
	if err != nil {
		t.Fatal("unable to put key/value", err)
	}

	itr, err := s.Lookup(nil, nil)
	if err != nil {
		t.Fatal("unable to lookup() on snapshot", err)
	}
	k, v, err := itr.Next()
	if err != nil {
		t.Fatal("unable to Next()", err)
	}
	if !bytes.Equal([]byte("mykey"), k) {
		t.Fatal("key does not match")
	}
	if !bytes.Equal([]byte("myvalue"), v) {
		t.Fatal("key does not match")
	}
	_, _, err = itr.Next()
	if err != leveldb.EndOfIterator {
		t.Fatal("should of seen EndOfIterator")
	}
	db.Close()
}

func TestSnapshot_DbClose(t *testing.T) {
	leveldb.Remove("test/mydb")

	db, err := leveldb.Open("test/mydb", options)
	if err != nil {
		t.Fatal("unable to create database", err)
	}

	err = db.Put([]byte("mykey"), []byte("myvalue"))
	if err != nil {
		t.Fatal("unable to put key/value", err)
	}

	s, err := db.Snapshot()
	if err != nil {
		t.Fatal("unable to get snapshot", err)
	}

	_, err = s.Get([]byte("mykey"))
	if err != nil {
		t.Fatal("unable to get key/value", err)
	}
	db.Close()
	_, err = s.Get([]byte("mykey"))
	if err != leveldb.DatabaseClosed {
		t.Fatal("should have been closed", err)
	}
}

func TestSnapshot_Close(t *testing.T) {
	leveldb.Remove("test/mydb")

	db, err := leveldb.Open("test/mydb", options)
	if err != nil {
		t.Fatal("unable to create database", err)
	}

	err = db.Put([]byte("mykey"), []byte("myvalue"))
	if err != nil {
		t.Fatal("unable to put key/value", err)
	}

	s, err := db.Snapshot()
	if err != nil {
		t.Fatal("unable to get snapshot", err)
	}

	err = db.Put([]byte("mykey1"), []byte("myvalue1"))
	if err != nil {
		t.Fatal("unable to put key/value", err)
	}

	_, err = s.Get([]byte("mykey"))
	if err != nil {
		t.Fatal("unable to get key/value", err)
	}
	s.Close()
	_, err = s.Get([]byte("mykey"))
	if err != leveldb.SnapshotClosed {
		t.Fatal("should have been closed", err)
	}

	db.Close()
}
