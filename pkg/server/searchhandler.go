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
	"github.com/dgraph-io/badger/v2"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/nlnwa/gowarc/pkg/index"
	"github.com/nlnwa/gowarc/pkg/loader"
	cdx "github.com/nlnwa/gowarc/proto"
	"github.com/sirupsen/logrus"
	"net/http"
)

type searchHandler struct {
	loader *loader.Loader
	db     *index.Db
}

var jsonMarshaler = &jsonpb.Marshaler{}

func (h *searchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	uri := r.URL.Query().Get("url")
	//uri := mux.Vars(r)["url"]
	logrus.Infof("request url: %v", uri)
	h.db.Search(uri, "", func(item *badger.Item) {
		result := &cdx.Cdx{}
		//k := item.Key()
		err := item.Value(func(v []byte) error {
			proto.Unmarshal(v, result)
			//fmt.Printf("key=%s, value=%s\n", k, result)

			cdxj, err := jsonMarshaler.MarshalToString(result)
			if err != nil {
				return err
			}
			fmt.Fprintf(w, "%s %s %s %s\n\n", result.Ssu, result.Sts, result.Srt, cdxj)

			return nil
		})
		if err != nil {
			//return err
		}
	})

}
