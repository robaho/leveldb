package main

import (
	"fmt"
	"github.com/robaho/leveldb"
	"log"
	"math/rand"
	"runtime"
	"time"
)

const nr = 10000000

func main() {

	runtime.GOMAXPROCS(4)

	leveldb.Remove("test/mydb")

	db, err := leveldb.Open("test/mydb", leveldb.Options{CreateIfNeeded: true})
	if err != nil {
		log.Fatal("unable to create database", err)
	}

	start := time.Now()
	for i := 0; i < nr; i++ {
		db.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)))
	}

	end := time.Now()
	duration := end.Sub(start).Microseconds()

	fmt.Println("insert time ", nr, "records = ", duration/1000, "ms, usec per op ", float64(duration)/nr)
	start = time.Now()
	err = db.Close()
	end = time.Now()
	duration = end.Sub(start).Microseconds()

	fmt.Println("close time ", duration/1000.0, "ms")
	if err != nil {
		panic(err)
	}

	testRead()

	db, err = leveldb.Open("test/mydb", leveldb.Options{})
	if err != nil {
		log.Fatal("unable to open database", err)
	}
	start = time.Now()
	db.CloseWithMerge(1)
	end = time.Now()
	duration = end.Sub(start).Microseconds()

	fmt.Println("close with merge 1 time ", float64(duration)/1000, "ms")

	testRead()
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
	itr, err = db.Lookup([]byte("mykey 300000"), []byte("mykey 799999"))
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
		_, err := db.Get([]byte(fmt.Sprintf("mykey%7d", index)))
		if err != nil {
			panic(err)
		}
	}
	end = time.Now()
	duration = end.Sub(start).Microseconds()

	fmt.Println("random access time ", float64(duration)/(nr/10), "us per get")

	db.Close()
}
