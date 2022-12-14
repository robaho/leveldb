package main

import (
	"flag"
	"fmt"
	"github.com/robaho/leveldb"
	"log"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"time"
)

const nr = 10000000

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	runtime.GOMAXPROCS(4)

	testInsert(false)
	testRead()
	testInsert(true)
	testRead()
	testInsertBatch()
	testRead()
	db, err := leveldb.Open("test/mydb", leveldb.Options{})
	if err != nil {
		panic(err)
	}
	db.Put([]byte(fmt.Sprintf("mykey%7d", 0)), []byte(fmt.Sprint("myvalue", 0)))
	fmt.Println("closing with 1 segment")
	db.CloseWithMerge(1)
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
	fmt.Println("number of segments", db.Stats().NumberOfSegments)
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
	itr, err = db.Lookup([]byte("mykey5000000"), []byte("mykey5099999"))
	count = 0
	for {
		_, _, err = itr.Next()
		if err != nil {
			break
		}
		count++
	}
	if count != nr/100 {
		log.Fatal("incorrect count, count is ", count)
	}
	end = time.Now()
	duration = end.Sub(start).Microseconds()

	fmt.Println("scan time 1% ", duration/1000, "ms, usec per op ", float64(duration)/(nr/100))

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
