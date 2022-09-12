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

	testInsert(false)
	testRead()
	testInsert(true)
	testRead()
	testInsertBatch()
	testRead()
}

func testInsert(noflush bool) {

	leveldb.Remove("test/mydb")

	db, err := leveldb.Open("test/mydb", leveldb.Options{CreateIfNeeded: true, DisableWriteFlush: noflush})
	if err != nil {
		log.Fatal("unable to create database", err)
	}

	start := time.Now()
	for i := 0; i < nr; i++ {
		db.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)))
	}

	end := time.Now()
	duration := end.Sub(start).Microseconds()

	s := ""
	if noflush {
		s = "NoFlush"
	}

	fmt.Println("insert", s, "time ", nr, "records = ", duration/1000, "ms, usec per op ", float64(duration)/nr)
	start = time.Now()
	err = db.Close()
	end = time.Now()
	duration = end.Sub(start).Microseconds()

	fmt.Println("close time ", duration/1000.0, "ms")
	if err != nil {
		panic(err)
	}
}

func testInsertBatch() {
	err := leveldb.Remove("test/mydb")

	db, err := leveldb.Open("test/mydb", leveldb.Options{CreateIfNeeded: true})
	if err != nil {
		log.Fatal("unable to create database", err)
	}

	start := time.Now()
	if err != nil {
		panic(err)
	}
	for i := 0; i < nr; {

		b := leveldb.WriteBatch{}
		for j := 0; j < 1000; j++ {
			b.Put([]byte(fmt.Sprintf("mykey%7d", i+j)), []byte(fmt.Sprint("myvalue", i+j)))
		}
		err = db.Write(b)
		if err != nil {
			log.Fatal("unable to Write batch", err)
		}
		i += 1000
	}

	end := time.Now()
	duration := end.Sub(start).Microseconds()

	fmt.Println("insert batch time ", nr, "records = ", duration/1000, "ms, usec per op ", float64(duration)/nr)

	start = time.Now()
	err = db.Close()
	end = time.Now()
	duration = end.Sub(start).Microseconds()

	fmt.Println("close time ", (duration)/1000, "ms")
	if err != nil {
		panic(err)
	}
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
