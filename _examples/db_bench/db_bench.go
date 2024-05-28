package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"runtime"
	"time"

	"github.com/robaho/leveldb"
)

// benchmark similar in scope to leveldb db_bench.cc, uses 16 byte keys and 100 byte values

const nr = 1000000
const vSize = 100
const kSize = 16
const batchSize = 1000

var value []byte
var dbname = "test/mydb"

func main() {

	value = make([]byte, vSize)
	rand.Read(value)

	runtime.GOMAXPROCS(4)

	testWrite(false, true)
	testWrite(true, true)
	testBatch()
	testWrite(false, false)
	testRead()
	testCompact()
	testRead()
}

func testWrite(sync bool, remove bool) {
	if remove {
		leveldb.Remove(dbname)
	}

	db, err := leveldb.Open(dbname, leveldb.Options{CreateIfNeeded: true, EnableSyncWrite: sync, MaxSegments: 64})
	if err != nil {
		log.Fatal("unable to create database", err)
	}

	start := time.Now()

	n := nr
	if sync {
		n = n / 100
	}

	for i := 0; i < n; i++ {
		key := make([]byte, kSize)
		keyS := []byte(fmt.Sprintf("%07d.........", i))
		copy(key, keyS)
		err = db.Put(key, value)
		if err != nil {
			panic(err)
		}
	}

	end := time.Now()
	duration := end.Sub(start).Microseconds()

	mode := "no-sync"
	if sync {
		mode = "sync"
	}
	if !remove {
		mode = mode + " overwrite"
	}

	fmt.Println("write", mode, "time", n, "records =", duration/1000, "ms, usec per op", float64(duration)/float64(n))
	start = time.Now()
	err = db.Close()
	end = time.Now()
	duration = end.Sub(start).Microseconds()

	fmt.Println("close time ", duration/1000.0, "ms")
	if err != nil {
		panic(err)
	}

	fmt.Println("database size ", dbsize("test/mydb"))
}

func testBatch() {
	leveldb.Remove(dbname)

	db, err := leveldb.Open(dbname, leveldb.Options{CreateIfNeeded: true, MaxSegments: 64})
	if err != nil {
		log.Fatal("unable to create database", err)
	}

	start := time.Now()
	for i := 0; i < nr; {
		wb := leveldb.WriteBatch{}
		for j := 0; j < batchSize; j++ {
			wb.Put([]byte(fmt.Sprintf("%07d.........", i+j)), value)
		}
		db.Write(wb)
		i += batchSize
	}

	end := time.Now()
	duration := end.Sub(start).Microseconds()

	fmt.Println("batch insert time ", nr, "records = ", duration/1000, "ms, usec per op ", float64(duration)/nr)
	start = time.Now()
	err = db.Close()
	end = time.Now()
	duration = end.Sub(start).Microseconds()

	fmt.Println("close time ", duration/1000.0, "ms")
	if err != nil {
		panic(err)
	}

	fmt.Println("database size ", dbsize("test/mydb"))
}
func testCompact() {
	db, err := leveldb.Open(dbname, leveldb.Options{CreateIfNeeded: false, MaxSegments: 64})
	if err != nil {
		log.Fatal("unable to create database", err)
	}
	start := time.Now()
	db.CloseWithMerge(1)
	end := time.Now()
	duration := end.Sub(start).Microseconds()

	fmt.Println("compact time ", duration/1000.0, "ms")
	fmt.Println("database size ", dbsize("test/mydb"))
}
func dbsize(path string) string {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatal(err)
	}
	var size int64 = 0
	for _, file := range files {
		size += file.Size()
	}
	return fmt.Sprintf("%.1dM", size/(1024*1024))
}

func testRead() {
	db, err := leveldb.Open("test/mydb", leveldb.Options{})
	if err != nil {
		log.Fatal("unable to open database", err)
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	keys := r.Perm(nr);

	start := time.Now()
	for _, index := range keys {
		_, err := db.Get([]byte(fmt.Sprintf("%07d.........", index)))
		if err != nil {
			panic(err)
		}
	}
	end := time.Now()
	duration := end.Sub(start).Microseconds()

	fmt.Println("read random time ", float64(duration)/(nr), "us per get")

	start = time.Now()
	itr, _ := db.Lookup(nil, nil)
	count := 0
	for {
		_, _, err = itr.Next()
		if err != nil {
			break
		}
		count++
	}
	if count != nr {
		log.Fatal("incorrect count != ", nr, ", count is ", count)
	}
	end = time.Now()
	duration = end.Sub(start).Microseconds()

	fmt.Println("read seq time ", duration/1000, "ms, usec per op ", float64(duration)/nr)

	err = db.Close()
	if err != nil {
		log.Fatal("unable to close", err)
	}
}
