# leveldb

high performance key value database written in Go. The api is based on Google's [leveldb](http://github.com/google/leveldb). The implementation is based on
the http://github.com/robaho/keydb.

**Note: snapshot support is currently under development.**

bulk insert and sequential read < 1 micro sec

random access read of disk based record < 4 micro secs

uses LSM trees, see https://en.wikipedia.org/wiki/Log-structured_merge-tree

limitation of max 1024 byte keys, to allow efficient on disk index searching, but has
compressed keys which allows for very efficient storage of time series data
(market tick data) in the same table

use the dbdump and dbload utilities to save/restore databases to a single file, but just zipping up the directory works as
well...

see the related http://github.com/robaho/leveldbr which allows remote access to a leveldb instance, and allows a leveldb database to be shared by multiple processes
      
# TODOs

make some settings configurable

purge removed key/value, it currently stores an empty []byte

snapshot support

seek to end, backwards iteration

# How To Use

	db, err := level.Open("test/mydb", leveldb.Options{})
	if err != nil {
		t.Fatal("unable to create database", err)
	}
	err = db.Put([]byte("mykey"), []byte("myvalue"))
	if err != nil {
		t.Fatal("unable to put key/Value", err)
	}
    err = db.Close()
    if err != nil {
        t.Fatal("unable to close database", err)
    }

# Performance

Using example/performance.go

```
Using Go 1.19:
insert  time  10000000 records =  48406 ms, usec per op  4.8406189
close time  2535 ms
scan time  3943 ms, usec per op  0.3943393
scan time 50%  69 ms, usec per op  0.138772
random access time  3.490679 us per get
insert NoFlush time  10000000 records =  12235 ms, usec per op  1.2235342
close time  6556 ms
scan time  3442 ms, usec per op  0.3442262
scan time 50%  70 ms, usec per op  0.141112
random access time  3.636962 us per get
insert batch time  10000000 records =  11967 ms, usec per op  1.1967498
close time  8024 ms
scan time  3354 ms, usec per op  0.3354182
scan time 50%  65 ms, usec per op  0.13129
random access time  3.641579 us per get
```
