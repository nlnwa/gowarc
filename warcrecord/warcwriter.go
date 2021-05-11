/*
 * Copyright 2021 National Library of Norway.
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
	"io"
)

type writer struct {
	opts *options
}

func NewWriter(opts *options) *writer {
	return &writer{
		opts: opts,
	}
}

func (m *writer) WriteRecord(w io.Writer, record WarcRecord) (int64, error) {
	if m.opts.compress {
		gz := gzip.NewWriter(w)
		defer gz.Close()
		w = gz
	}

	//if err := record.Finalize(); err != nil {
	//	return 0, err
	//}

	// Write WARC record version
	n, err := fmt.Fprintf(w, "%v\r\n", record.Version())
	bytesWritten := int64(n)
	if err != nil {
		return bytesWritten, err
	}

	// Write WARC header
	bw, err := record.WarcHeader().Write(w)
	bytesWritten += bw
	if err != nil {
		return bytesWritten, err
	}

	// Write separator
	n, err = w.Write([]byte(CRLF))
	bytesWritten += int64(n)
	if err != nil {
		return bytesWritten, err
	}

	// Write WARC content
	r, err := record.Block().RawBytes()
	if err != nil {
		return bytesWritten, err
	}
	bw, err = io.Copy(w, r)
	//bw, err = record.Block().RawBytes().WriteTo(w)
	bytesWritten += bw
	if err != nil {
		return bytesWritten, err
	}

	// Write end of record separator
	n, err = w.Write([]byte(CRLFCRLF))
	bytesWritten += int64(n)
	if err != nil {
		return bytesWritten, err
	}

	return bytesWritten, err
}
