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
	"github.com/fsnotify/fsnotify"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type autoindexer struct {
	watcher     *fsnotify.Watcher
	indexWorker *indexWorker
}

func NewAutoIndexer(db *Db) *autoindexer {
	a := &autoindexer{
		indexWorker: NewIndexWorker(db, 8),
	}
	go a.fileWatcher()
	return a
}

func (a *autoindexer) Shutdown() {
	a.indexWorker.Shutdown()
}

func (a *autoindexer) fileWatcher() {
	var err error
	a.watcher, err = fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer a.watcher.Close()

	done := make(chan bool)
	go func() {
		for {
			select {
			case event, ok := <-a.watcher.Events:
				if !ok {
					return
				}
				if event.Op&fsnotify.Write == fsnotify.Write && !strings.HasSuffix(event.Name, "~") {
					log.Debugf("modified file: %v", event.Name)
					a.indexWorker.Queue(event.Name, 10*time.Second)
				}
			case err, ok := <-a.watcher.Errors:
				if !ok {
					return
				}
				log.Println("error:", err)
			}
		}
	}()

	for _, wd := range viper.GetStringSlice("warcdir") {
		err := a.watcher.Add(wd)
		if err != nil {
			log.Fatal(err)
		}

		a.indexDir(wd)
	}
	<-done
}

func (a *autoindexer) indexDir(dir string) {
	f, err := os.Open(dir)
	if err != nil {
		log.Fatal(err)
	}
	files, err := f.Readdir(-1)
	f.Close()
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		if !file.IsDir() && !strings.HasSuffix(file.Name(), "~") {
			a.indexWorker.Queue(filepath.Join(dir, file.Name()), 0)
		}
	}
}
