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

package server

import (
	"context"
	"github.com/gorilla/mux"
	"github.com/nlnwa/gowarc/pkg/index"
	"github.com/nlnwa/gowarc/pkg/loader"
	log "github.com/sirupsen/logrus"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func Serve(db *index.Db) {
	l := &loader.Loader{
		Resolver: &storageRefResolver{db: db},
		Loader: &loader.FileStorageLoader{FilePathResolver: func(fileName string) (filePath string, err error) {
			return db.GetFilePath(fileName)
		}},
		NoUnpack: false,
	}

	r := mux.NewRouter()
	r.Handle("/id/{id}", &contentHandler{l})
	r.Handle("/files/", &fileHandler{l, db})
	r.Handle("/search/", &searchHandler{l})
	http.Handle("/", r)

	httpServer := &http.Server{
		Addr: ":8080",
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigs
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		httpServer.Shutdown(ctx)
	}()

	log.Info(httpServer.ListenAndServe())
	//log.Fatal(http.ListenAndServe(":8080", nil))
}

type storageRefResolver struct {
	db *index.Db
}

func (m *storageRefResolver) Resolve(warcId string) (storageRef string, err error) {
	return m.db.GetStorageRef(warcId)
}
