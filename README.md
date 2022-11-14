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
well.

see the related http://github.com/robaho/leveldbr which allows remote access to a leveldb instance, and allows a leveldb database to be shared by multiple processes
      
# TODOs

make some settings configurable

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

DbBench using Go 1.19.2

```
write no-sync time 1000000 records = 4586 ms, usec per op 4.586062
close time  1271 ms
database size  118M
write sync time 10000 records = 1014 ms, usec per op 101.4993
close time  8 ms
database size  1M
batch insert time  1000000 records =  1204 ms, usec per op  1.204402
close time  1380 ms
database size  118M
write no-sync overwrite time 1000000 records = 4743 ms, usec per op 4.743957
close time  1272 ms
database size  237M
read random time  3.69818 us per get
read seq time  1791 ms, usec per op  1.791742
compact time  2851 ms
database size  118M
read random time  2.04372 us per get
read seq time  132 ms, usec per op  0.132152
```
