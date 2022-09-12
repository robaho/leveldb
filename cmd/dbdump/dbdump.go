package main

import (
	"bufio"
	"encoding/hex"
	"flag"
	"fmt"
	"github.com/robaho/leveldb"
	"html"
	"log"
	"os"
	"path/filepath"
)

// dump a database to stdout
func main() {
	path := flag.String("path", "", "set the database path")
	out := flag.String("out", "", "set output file, defaults to dbname.xml")
	hexEncode := flag.Bool("hex", false, "whether to use hex encoding of keys and values, else strings")

	flag.Parse()

	dbpath := filepath.Clean(*path)

	if *out == "" {
		*out = filepath.Base(dbpath) + ".xml"
	}

	outfile, err := os.Create(*out)
	if err != nil {
		log.Fatal("unable to open output file ", err)
	}
	defer outfile.Close()

	w := bufio.NewWriter(outfile)

	fi, err := os.Stat(dbpath)
	if err != nil {
		log.Fatalln("unable to open database directory", err)
	}

	if !fi.IsDir() {
		log.Fatalln("path is not a directory")
	}

	db, err := leveldb.Open(dbpath, leveldb.Options{})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Fprintf(w, "<db path=\"%s\" hex=\"%t\">\n", html.EscapeString(dbpath), *hexEncode)
	itr, err := db.Lookup(nil, nil)
	if err != nil {
		log.Fatal("unable to open iterator", err)
	}
	for {
		if key, value, err := itr.Next(); err == nil {
			var k, v string
			if *hexEncode {
				k = hex.EncodeToString(key)
				v = hex.EncodeToString(value)
			} else {
				k = html.EscapeString(string(key))
				v = html.EscapeString(string(value))
			}
			fmt.Fprintf(w, "\t\t<entry><key>%s</key> <value>%s</value></entry>\n", k, v)
		} else {
			if err == leveldb.EndOfIterator {
				break
			}
			if err != nil {
				log.Fatal("error processing db")
			}
		}
	}
	fmt.Fprintln(w, "</db>")

	err = w.Flush()
	if err != nil {
		log.Fatal("unable to flush writer, io errors,", err)
	}
}
