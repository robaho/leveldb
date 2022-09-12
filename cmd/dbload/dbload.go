package main

import (
	"bufio"
	"encoding/hex"
	"encoding/xml"
	"flag"
	"github.com/robaho/leveldb"
	"log"
	"os"
	"path/filepath"
	"strconv"
)

// load a database a dbdump file
func main() {
	path := flag.String("path", "", "set the database path")
	in := flag.String("in", "", "set the input file, defaults to dbname.xml")
	remove := flag.Bool("remove", true, "remove existing db if it exists")
	create := flag.Bool("create", true, "create database if it doesn't exist")

	flag.Parse()

	if *path == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	dbpath := filepath.Clean(*path)

	if *in == "" {
		*in = filepath.Base(dbpath) + ".xml"
	}

	infile, err := os.Open(*in)
	if err != nil {
		log.Fatal("unable to open input file ", err)
	}
	defer infile.Close()

	if *remove {
		err = leveldb.Remove(dbpath)
		if err != leveldb.NoDatabaseFound {
			log.Fatal("unable to remove ")
		}
	}

	r := bufio.NewReader(infile)

	var hexEncoded bool

	var db *leveldb.Database

	decoder := xml.NewDecoder(r)

	type EntryElement struct {
		Key   string `xml:"key"`
		Value string `xml:"value"`
	}

	db, err = leveldb.Open(dbpath, leveldb.Options{CreateIfNeeded: *create})
	if err != nil {
		panic(err)
	}

	var inElement string
	for {
		// Read tokens from the XML document in a stream.
		t, _ := decoder.Token()
		if t == nil {
			break
		}
		// Inspect the type of the token just read.
		switch se := t.(type) {
		case xml.StartElement:
			// If we just read a StartElement token
			inElement = se.Name.Local
			// ...and its name is "page"
			if inElement == "db" {
				hexEncoded, _ = strconv.ParseBool(getAttr("hex", se.Attr))
			} else if inElement == "entry" {
				e := EntryElement{}

				decoder.DecodeElement(&e, &se)

				if hexEncoded {
					key, err := hex.DecodeString(e.Key)
					if err != nil {
						log.Fatal(err)
					}
					value, err := hex.DecodeString(e.Value)
					if err != nil {
						log.Fatal(err)
					}
					err = db.Put(key, value)
					if err != nil {
						log.Fatal(err)
					}
				} else {
					err := db.Put([]byte(e.Key), []byte(e.Value))
					if err != nil {
						log.Fatal(err)
					}
				}
			}
		case xml.EndElement:
		default:
		}

	}

	err = db.Close()
	if err != nil {
		panic(err)
	}

}

func getAttr(name string, attrs []xml.Attr) string {
	for _, v := range attrs {
		if v.Name.Local == name {
			return v.Value
		}
	}
	return ""
}
