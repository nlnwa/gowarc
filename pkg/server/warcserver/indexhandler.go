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
	"fmt"
	"github.com/dgraph-io/badger/v2"
	"github.com/nlnwa/gowarc/pkg/index"
	"github.com/nlnwa/gowarc/pkg/loader"
	cdx "github.com/nlnwa/gowarc/proto"
	"net/http"
)

type indexHandler struct {
	loader *loader.Loader
	db     *index.Db
}

func (h *indexHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var renderFunc RenderFunc = func(w http.ResponseWriter, record *cdx.Cdx, cdxApi *cdxServerApi) error {
		cdxj, err := jsonMarshaler.Marshal(record)
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "%s %s %s %s\n", record.Ssu, record.Sts, record.Srt, cdxj)
		return nil
	}

	cdxApi, err := parseCdxServerApi(w, r, renderFunc)
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
