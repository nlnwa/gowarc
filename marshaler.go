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

package gowarc

import (
	"fmt"
	"io"
)

type Marshaler interface {
	// Marshal converts a WARC record to its serialized form.
	Marshal(record WarcRecord) (WarcRecord, int64, error)
}

type WarcFileMarshaler struct {
	opts       *options
	LastOffset int64
	writer     io.Writer
}

func NewWarcFileMarshaler(opts *options) *WarcFileMarshaler {
	u := &WarcFileMarshaler{
		opts:   opts,
		writer: nil,
	}
	return u
}

func (m *WarcFileMarshaler) Marshal(record WarcRecord) (WarcRecord, int64, error) {
	size, err := m.writeRecord(m.writer, record)
	return nil, size, err
}

func (m *WarcFileMarshaler) writeRecord(w io.Writer, record WarcRecord) (int64, error) {
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
	n, err = w.Write([]byte(crlf))
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
	n, err = w.Write([]byte(crlfcrlf))
	bytesWritten += int64(n)
	if err != nil {
		return bytesWritten, err
	}

	return bytesWritten, err
}
