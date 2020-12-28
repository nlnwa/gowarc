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
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type autoindexer struct {
	watcher     *fsnotify.Watcher
	indexWorker *indexWorker
	watchDepth  int
}

func NewAutoIndexer(db *Db, watchDepth int) *autoindexer {
	a := &autoindexer{
		indexWorker: NewIndexWorker(db, 8),
		watchDepth:  watchDepth,
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

				if strings.HasSuffix(event.Name, "~") {
					continue
				}

				if event.Op&fsnotify.Write == fsnotify.Write {
					log.Debugf("modified file: %v", event.Name)
					a.indexWorker.Queue(event.Name, 10*time.Second)
				} else if event.Op&fsnotify.Create == fsnotify.Create {
					fStat, statErr := os.Stat(event.Name)
					if statErr != nil {
						// we don't panic if the program fails to listen
						log.Error(err)
						continue
					}

					if !fStat.Mode().IsDir() {
						continue
					}

					watchErr := a.watcher.Add(event.Name)
					if watchErr != nil {
						log.Errorf("Error occured when trying to listen to new directory '%v', err: %v", event.Name, err)
					}
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
		a.addAndIndexDir(wd, 0)
	}
	<-done
}

// Recursively add directory to autoindexer watcher and index it.
// This function will **panic** if path is a file or does not exist
func (a *autoindexer) addAndIndexDir(path string, currentDepth int) {
	err := a.watcher.Add(path)
	if err != nil {
		log.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}

	files, err := f.Readdir(-1)
	f.Close()
	if err != nil {
		log.Fatalf("%v: %v", f.Name(), err)
	}

	for _, file := range files {
		if strings.HasSuffix(file.Name(), "~") {
			continue
		}

		if !file.IsDir() {
			a.indexWorker.Queue(filepath.Join(path, file.Name()), 0)
		} else if currentDepth < a.watchDepth {
			a.addAndIndexDir(filepath.Join(path, file.Name()), currentDepth+1)
		}
	}
}
