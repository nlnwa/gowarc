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
	"fmt"
	"io"
)

type WarcWriterOpts struct {
	Strict   bool
	Compress bool
}

type WarcWriter struct {
	opts *WarcWriterOpts
}

func NewWarcWriter(opts *WarcWriterOpts) *WarcWriter {
	return &WarcWriter{
		opts: opts,
	}
}

func (ww *WarcWriter) WriteRecord(w io.Writer, record WarcRecord) (bytesWritten int, err error) {
	var n int
	//n, err = fmt.Fprintf(w, "Names: %v\r\n", record.headers.Names())
	n, err = fmt.Fprintf(w, "WARC/%v\r\n", record.Version())
	bytesWritten += n
	if err != nil {
		return
	}
	//n, err = record.headers.Write(w)
	//bytesWritten += n
	//if err != nil {
	//	return
	//}
	n, err = w.Write([]byte(CRLF))
	bytesWritten += n
	if err != nil {
		return
	}
	return
}
