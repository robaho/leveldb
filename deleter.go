package leveldb

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Deleter interface {
	scheduleDeletion(ifExists string, filesToDelete []string) error
	deleteScheduled() error
}

type dbDeleter struct {
	path string
	file *os.File
}

type nullDeleter struct {
}

func (d *nullDeleter) scheduleDeletion(ifExists string, filesToDelete []string) error {
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

func (d *dbDeleter) scheduleDeletion(ifExists string, filesToDelete []string) error {
	if d.file == nil {
		file, err := os.OpenFile(filepath.Join(d.path, "deleted"), os.O_WRONLY|os.O_APPEND|os.O_CREATE|os.O_SYNC, 0600)
		if err != nil {
			return err
		}
		d.file = file
	}
	fmt.Printf("%s,%s\n", ifExists, strings.Join(filesToDelete, ","))
	_, err := fmt.Fprintf(d.file, "%s,%s\n", ifExists, strings.Join(filesToDelete, ","))
	return err
}
func (d *dbDeleter) deleteScheduled() error {
	if d.file != nil {
		err := d.file.Close()
		if err != nil {
			return err
		}
	}
	f, err := os.OpenFile(filepath.Join(d.path, "deleted"), os.O_RDONLY, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := s.Text()
		fmt.Println("deleted:", line)
		files := strings.Split(line, ",")
		path := filepath.Join(d.path, files[0])
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			// file does not exist
		} else {
			// file exists so delete the others
			for _, file := range files[1:] {
				path := filepath.Join(d.path, file)
				err := os.Remove(path)
				if err != nil && !os.IsNotExist(err) {
					// ignore if the file has already been deleted
					return err
				}
			}
		}
	}
	err = f.Close()
	if err != nil {
		return err
	}
	// create new deleted file
	file, err := os.OpenFile(filepath.Join(d.path, "deleted"), os.O_APPEND|os.O_CREATE|os.O_SYNC|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	d.file = file
	return nil
}
