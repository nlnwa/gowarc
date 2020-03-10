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
	"github.com/nlnwa/gowarc/pkg/index"
	"github.com/nlnwa/gowarc/pkg/loader"
	"github.com/sirupsen/logrus"
	"net/http"
)

type searchHandler struct {
	loader *loader.Loader
	db     *index.Db
}

func (h *searchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	uri := r.URL.Query().Get("url")
	//uri := mux.Vars(r)["url"]
	logrus.Infof("request url: %v", uri)
	h.db.Search(uri, "")

	//record, err := h.loader.Get(warcid)
	//
	//if err != nil {
	//	w.Header().Set("Content-Type", "text/plain")
	//	w.WriteHeader(404)
	//	w.Write([]byte("Document not found\n"))
	//	return
	//}
	//defer record.Close()
	//
	//switch v := record.Block().(type) {
	//case warcrecord.HttpResponseBlock:
	//	r, _ := v.Response()
	//	for k, vl := range r.Header {
	//		for _, v := range vl {
	//			w.Header().Set(k, v)
	//		}
	//	}
	//	io.Copy(w, r.Body)
	//default:
	//	w.Header().Set("Content-Type", "text/plain")
	//	record.WarcHeader().Write(w)
	//	fmt.Fprintln(w)
	//	rb, err := v.RawBytes()
	//	if err != nil {
	//		return
	//	}
	//	io.Copy(w, rb)
	//}
}
