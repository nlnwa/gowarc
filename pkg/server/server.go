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
	"github.com/nlnwa/gowarc/pkg/loader"
	"io"
	"log"
	"net/http"
)

type recordHandler struct {
	loader *loader.Loader
}

func (h *recordHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	warcid := r.URL.Path
	fmt.Printf("ID: %v\n", warcid)
	record, err := h.loader.Get(warcid)
	defer record.Close()
	fmt.Printf("HEY: %v %v\n", record.Type(), err)
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
		io.Copy(w, v.RawBytes())
	}
}

func Serve() {
	l := &loader.Loader{
		StorageRefResolver: mockStorageRefResolver,
		StorageLoader:      loader.FileStorageLoader,
		NoUnpack:           false,
	}

	rh := &recordHandler{l}

	http.Handle("/id/", http.StripPrefix("/id/", rh))

	log.Fatal(http.ListenAndServe(":8080", nil))
}

func mockStorageRefResolver(warcId string) (storageRef string, err error) {
	fmt.Printf("RESOLVE: %v\n", warcId)
	switch warcId {
	case "urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008":
		storageRef = "warcfile:testdata/example.warc:0"
	case "urn:uuid:e9a0ee48-0221-11e7-adb1-0242ac120008":
		storageRef = "warcfile:testdata/example.warc:488"
	case "urn:uuid:a9c51e3e-0221-11e7-bf66-0242ac120005":
		storageRef = "warcfile:testdata/example.warc:1197"
	case "urn:uuid:a9c5c23a-0221-11e7-8fe3-0242ac120007":
		storageRef = "warcfile:testdata/example.warc:2566"
	case "urn:uuid:e6e395ca-0221-11e7-a18d-0242ac120005":
		storageRef = "warcfile:testdata/example.warc:3370"
	case "urn:uuid:e6e41fea-0221-11e7-8fe3-0242ac120007":
		storageRef = "warcfile:testdata/example.warc:4316"
	}
	return
}
