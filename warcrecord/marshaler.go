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

package warcrecord

import (
	"compress/gzip"
	"fmt"
	"github.com/nlnwa/gowarc/warcoptions"
	"io"
)

type Marshaler struct {
	opts *warcoptions.WarcOptions
}

func NewMarshaler(opts *warcoptions.WarcOptions) *Marshaler {
	return &Marshaler{
		opts: opts,
	}
}

func (m *Marshaler) WriteRecord(w io.Writer, record WarcRecord) (bytesWritten int64, err error) {
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

	var n2 int64
	var r io.Reader
	r, err = record.Block().RawBytes()
	if err != nil {
		return
	}
	n2, err = io.Copy(w, r)
	//n, err = w.Write([]byte(CRLF))
	bytesWritten += n2
	if err != nil {
		return
	}

	return
}
