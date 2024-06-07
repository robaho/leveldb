package leveldb

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Deleter interface {
	scheduleDeletion(filesToDelete []string) error
	deleteScheduled() error
}

type dbDeleter struct {
	path string
	file *os.File
}

type nullDeleter struct {
}

func (d *nullDeleter) scheduleDeletion(filesToDelete []string) error {
	return nil
}
func (d *nullDeleter) deleteScheduled() error {
	return nil
}
func newNullDeleter() Deleter {
	return &nullDeleter{}
}

func newDeleter(path string) Deleter {
	return &dbDeleter{
		path: path,
	}
}

func (d *dbDeleter) scheduleDeletion(filesToDelete []string) error {
	if len(filesToDelete) == 0 {
		return nil;
	} 
	if d.file == nil {
		file, err := os.OpenFile(filepath.Join(d.path, "deleted"), os.O_WRONLY|os.O_APPEND|os.O_CREATE|os.O_SYNC, 0600)
		if err != nil {
			return err
		}
		d.file = file
	}
	//fmt.Printf("%s,%s\n", ifExists, strings.Join(filesToDelete, ","))
	_, err := fmt.Fprintf(d.file, "%s\n", strings.Join(filesToDelete, ","))
	return err
}
func (d *dbDeleter) deleteScheduled() error {
	if d.file != nil {
		err := d.file.Close()
		if err != nil {
			return err
		}
		d.file = nil
	}
	filename := filepath.Join(d.path, "deleted")
	f, err := os.OpenFile(filename, os.O_RDONLY, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := s.Text()
		// if line=="" {
		// 	continue;
		// }
		//fmt.Println("deleted:", line)
		files := strings.Split(line, ",")
		for _, file := range files {
			path := filepath.Join(d.path, file)
			err := os.Remove(path)
			if err != nil && !os.IsNotExist(err) {
				// ignore if the file has already been deleted
				return err
			}
		}
	}
	err = f.Close()
	if err != nil {
		return err
	}
	err = os.Remove(filename)
	if err != nil {
		return err
	}
	return nil
}
