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
	"fmt"
	"github.com/nlnwa/gowarc/pkg/gowarc"
	"github.com/nlnwa/gowarc/pkg/index"
	"github.com/nlnwa/gowarc/pkg/loader"
	log "github.com/sirupsen/logrus"
	"io"
	"net/http"
)

type recordHandler struct {
	loader *loader.Loader
}

func (h *recordHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	warcid := r.URL.Path
	log.Debugf("request id: %v", warcid)
	record, err := h.loader.Get(warcid)

	if err != nil {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(404)
		w.Write([]byte("Document not found\n"))
		return
	}
	defer record.Close()

	switch v := record.Block().(type) {
	case gowarc.HttpResponseBlock:
		r, _ := v.Response()
		for k, vl := range r.Header {
			for _, v := range vl {
				w.Header().Set(k, v)
			}
		}
		io.Copy(w, r.Body)
	default:
		w.Header().Set("Content-Type", "text/plain")
		for _, k := range record.GF().Names() {
			fmt.Fprintf(w, "%v = %v\n", k, record.GF().GetAll(k))
		}
		fmt.Fprintln(w)
		rb, err := v.RawBytes()
		if err != nil {
			return
		}
		io.Copy(w, rb)
	}
}

func Serve() {
	// TODO: make configurable
	db, err := index.NewIndexDb("/tmp/cdx")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	l := &loader.Loader{
		Resolver: &storageRefResolver{db: db},
		Loader: &loader.FileStorageLoader{FilePathResolver: func(fileName string) (filePath string, err error) {
			return db.GetFilePath(fileName)
		}},
		NoUnpack: false,
	}

	rh := &recordHandler{l}

	http.Handle("/id/", http.StripPrefix("/id/", rh))

	log.Fatal(http.ListenAndServe(":8080", nil))
}

type storageRefResolver struct {
	db *index.Db
}

func (m *storageRefResolver) Resolve(warcId string) (storageRef string, err error) {
	return m.db.GetStorageRef(warcId)
}
