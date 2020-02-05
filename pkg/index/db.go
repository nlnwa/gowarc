/*
 * Copyright 2020 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package index

import (
	"fmt"
	"github.com/dgraph-io/badger"
	log "github.com/sirupsen/logrus"
	"os"
	"path"
	"path/filepath"
	"time"
)

type Db struct {
	dbDir       string
	idIndex     *badger.DB
	fileIndex   *badger.DB
	idIndexGc   *time.Ticker
	fileIndexGc *time.Ticker
}

func NewIndexDb(dbDir string) (*Db, error) {
	idIndexDir := path.Join(dbDir, "id-index")
	fileIndexDir := path.Join(dbDir, "file-index")

	d := &Db{
		dbDir:       dbDir,
		idIndexGc:   time.NewTicker(5 * time.Minute),
		fileIndexGc: time.NewTicker(5 * time.Minute),
	}

	var err error

	d.idIndex, err = openIndex(idIndexDir, d.idIndexGc)
	if err != nil {
		return nil, err
	}

	d.fileIndex, err = openIndex(fileIndexDir, d.fileIndexGc)
	if err != nil {
		return nil, err
	}

	return d, nil
}

func openIndex(indexDir string, gcTrigger *time.Ticker) (db *badger.DB, err error) {
	if err := os.MkdirAll(indexDir, 0777); err != nil {
		return nil, err
	}
	opts := badger.DefaultOptions(indexDir)
	opts.Logger = log.StandardLogger()
	db, err = badger.Open(opts)
	go func() {
		for range gcTrigger.C {
		again:
			err := db.RunValueLogGC(0.7)
			if err == nil {
				goto again
			}
		}
	}()
	return
}

func (d *Db) Delete() {
	if err := os.RemoveAll(d.dbDir); err != nil {
		log.Fatal(err)
	}
}

func (d *Db) Close() {
	d.idIndexGc.Stop()
	d.fileIndexGc.Stop()
	for {
		err := d.idIndex.RunValueLogGC(0.7)
		if err != nil {
			break
		}
	}
	_ = d.idIndex.Close()
	for {
		err := d.fileIndex.RunValueLogGC(0.7)
		if err != nil {
			break
		}
	}
	_ = d.fileIndex.Close()
}

func (d *Db) Add(id, filePath string, offset int64) error {
	var err error
	filePath, err = filepath.Abs(filePath)
	if err != nil {
		return err
	}
	fileName := filepath.Base(filePath)
	err = d.fileIndex.Update(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(fileName))
		if err == badger.ErrKeyNotFound {
			err = txn.Set([]byte(fileName), []byte(filePath))
		}
		return err
	})
	if err != nil {
		return err
	}

	storageRef := fmt.Sprintf("warcfile:%s:%d", fileName, offset)
	err = d.idIndex.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(id), []byte(storageRef))
		return err
	})
	return err
}

func (d *Db) GetStorageRef(id string) (string, error) {
	var val []byte
	err := d.idIndex.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(id))
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	return string(val), err
}

func (d *Db) GetFilePath(fileName string) (string, error) {
	var val []byte
	err := d.fileIndex.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(fileName))
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	return string(val), err
}
