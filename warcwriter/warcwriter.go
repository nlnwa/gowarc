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

package warcwriter

import (
	"compress/gzip"
	"fmt"
	"github.com/nlnwa/gowarc/warcoptions"
	"github.com/nlnwa/gowarc/warcrecord"
	"io"
)

const CRLF = "\r\n"

type writer struct {
	opts *warcoptions.WarcOptions
}

func NewWriter(opts *warcoptions.WarcOptions) *writer {
	return &writer{
		opts: opts,
	}
}

func (m *writer) WriteRecord(w io.Writer, record warcrecord.WarcRecord) (bytesWritten int64, err error) {
	if m.opts.Compress {
		gz := gzip.NewWriter(w)
		defer gz.Close()
		w = gz
	}
	var n int
	n, err = fmt.Fprintf(w, "WARC/%v\r\n", record.Version())
	bytesWritten += int64(n)
	if err != nil {
		return
	}

	n, err = record.WarcHeader().Write(w)
	bytesWritten += int64(n)
	if err != nil {
		return
	}

	n, err = w.Write([]byte(CRLF))
	bytesWritten += int64(n)
	if err != nil {
		return
	}

	if m, ok := record.Block().(Marshaler); ok {
		n, err = m.Write(w)
		bytesWritten += int64(n)
		if err != nil {
			return
		}
	}

	//var n2 int64
	//var r io.Reader
	//r, err = record.Block().RawBytes()
	//if err != nil {
	//	return
	//}
	//n2, err = io.Copy(w, r)
	////n, err = w.Write([]byte(CRLF))
	//bytesWritten += n2
	//if err != nil {
	//	return
	//}

	return
}
