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

package warcserver

import (
	"github.com/dgraph-io/badger/v2"
	"github.com/nlnwa/gowarc/pkg/index"
	"github.com/nlnwa/gowarc/pkg/loader"
	"google.golang.org/protobuf/encoding/protojson"
	"net/http"
)

type indexHandler struct {
	loader *loader.Loader
	db     *index.Db
}

var jsonMarshaler = &protojson.MarshalOptions{}

func (h *indexHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cdxApi, err := parseCdxServerApi(w, r)
	if err != nil {
		handleError(err, w)
		return
	}

	var defaultPerItemFunc index.PerItemFunction = func(item *badger.Item) (stopIteration bool) {
		k := item.Key()
		if !cdxApi.dateRange.eval(k) {
			return false
		}

		return cdxApi.writeItem(item)
	}

	var defaultAfterIterationFunc index.AfterIterationFunction = func() error {
		return nil
	}

	if cdxApi.sort.closest != "" {
		h.db.Search(cdxApi.key, false, cdxApi.sort.add, cdxApi.sort.write)
	} else {
		h.db.Search(cdxApi.key, cdxApi.sort.reverse, defaultPerItemFunc, defaultAfterIterationFunc)
	}
}
