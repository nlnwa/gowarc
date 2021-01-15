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
	"github.com/gorilla/mux"
	"github.com/nlnwa/gowarc/pkg/index"
	"github.com/nlnwa/gowarc/pkg/loader"
	"github.com/nlnwa/gowarc/pkg/server/warcserver"
	"net/http"
)

func Handler(db *index.Db, middlewares ...func (http.Handler) http.Handler) http.Handler {
	l := &loader.Loader{
		Resolver: &storageRefResolver{db: db},
		Loader: &loader.FileStorageLoader{FilePathResolver: func(fileName string) (filePath string, err error) {
			fileInfo, err := db.GetFilePath(fileName)
			return fileInfo.Path, err
		}},
		NoUnpack: false,
	}

	r := mux.NewRouter()
	r.Handle("/id/{id}", &contentHandler{l})
	r.Handle("/files/", &fileHandler{l, db})
	r.Handle("/search", &searchHandler{l, db})
	warcserverRoutes := r.PathPrefix("/warcserver").Subrouter()
	warcserver.RegisterRoutes(warcserverRoutes, db, l)

	for _, middleware := range middlewares {
		r.Use(middleware)
	}

	return r
}

type storageRefResolver struct {
	db *index.Db
}

func (m *storageRefResolver) Resolve(warcId string) (storageRef string, err error) {
	return m.db.GetStorageRef(warcId)
}
