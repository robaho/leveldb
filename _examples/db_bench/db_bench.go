package main

import (
	"fmt"
	"github.com/robaho/leveldb"
	"io/ioutil"
	"log"
	"math/rand"
	"runtime"
	"time"
)

// benchmark similar in scope to leveldb db_bench.cc, uses 16 byte keys and 100 byte values

const nr = 1000000
const vSize = 100
const kSize = 16
const batchSize = 1000

var value []byte

func main() {

	value = make([]byte, vSize)
	rand.Read(value)

	runtime.GOMAXPROCS(4)

	testWrite(false)
	testBatch()
	testWrite(true)
	testRead()

	db, err := leveldb.Open("test/mydb", leveldb.Options{})
	if err != nil {
		log.Fatal("unable to open database", err)
	}
	start := time.Now()
	db.CloseWithMerge(1)
	end := time.Now()
	duration := end.Sub(start).Microseconds()

	fmt.Println("close with merge 1 time ", float64(duration)/1000, "ms")

	testRead()
}

func testWrite(sync bool) {
	leveldb.Remove("test/mydb")

	db, err := leveldb.Open("test/mydb", leveldb.Options{CreateIfNeeded: true, EnableSyncWrite: sync, MaxSegments: 16})
	if err != nil {
		log.Fatal("unable to create database", err)
	}

	start := time.Now()

	for i := 0; i < nr; i++ {
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

	mode := ""
	if sync {
		mode = "sync"
	}

	fmt.Println("insert", mode, "time", nr, "records =", duration/1000, "ms, usec per op", float64(duration)/nr)
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
	leveldb.Remove("test/mydb")

	db, err := leveldb.Open("test/mydb", leveldb.Options{CreateIfNeeded: true})
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
	start := time.Now()
	itr, err := db.Lookup(nil, nil)
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
	end := time.Now()
	duration := end.Sub(start).Microseconds()

	fmt.Println("scan time ", duration/1000, "ms, usec per op ", float64(duration)/nr)

	start = time.Now()
	itr, err = db.Lookup([]byte("0300000........."), []byte("0799999........."))
	count = 0
	for {
		_, _, err = itr.Next()
		if err != nil {
			break
		}
		count++
	}
	if count != 500000 {
		log.Fatal("incorrect count != 500000, count is ", count)
	}
	end = time.Now()
	duration = end.Sub(start).Microseconds()

	fmt.Println("scan time 50% ", duration/1000, "ms, usec per op ", float64(duration)/500000)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	start = time.Now()

	for i := 0; i < nr/10; i++ {
		index := r.Intn(nr / 10)
		_, err := db.Get([]byte(fmt.Sprintf("%07d.........", index)))
		if err != nil {
			panic(err)
		}
	}
	end = time.Now()
	duration = end.Sub(start).Microseconds()

	fmt.Println("random access time ", float64(duration)/(nr/10), "us per get")

	db.Close()
}
