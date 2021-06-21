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
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

type funcNameGenerator struct {
	f func() string
}

func (fng *funcNameGenerator) NewWarcfileName() string {
	return fng.f()
}

func createTestRecord() WarcRecord {
	builder := NewRecordBuilder(Response)
	_, err := builder.WriteString("HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n" +
		"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n" +
		"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the content")
	if err != nil {
		panic(err)
	}
	builder.AddWarcHeader(WarcRecordID, "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>")
	builder.AddWarcHeader(WarcDate, "2006-01-02T15:04:05Z")
	builder.AddWarcHeader(ContentLength, "257")
	builder.AddWarcHeader(ContentType, "application/http;msgtype=response")
	builder.AddWarcHeader(WarcBlockDigest, "sha1:B285747AD7CC57AA74BCE2E30B453C8D1CB71BA4")

	wr, _, err := builder.Finalize()
	if err != nil {
		panic(err)
	}
	return wr
}

func TestWarcFileWriter_Write(t *testing.T) {
	type args struct {
		fileName string
		compress bool
		record   WarcRecord
	}
	tests := []struct {
		name            string
		args            args
		wantFileSize    int64
		wantWrittenSize int64
		wantErr         bool
	}{
		{
			"Write uncompressed",
			args{
				fileName: "foo.warc",
				compress: false,
				record:   createTestRecord(),
			},
			528,
			528,
			false,
		},
		{
			"Write compressed",
			args{
				fileName: "foo.warc",
				compress: true,
				record:   createTestRecord(),
			},
			390,
			528,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nameGenerator := &funcNameGenerator{
				f: func() string { return tt.args.fileName },
			}

			w := NewWarcFileWriter(WithCompression(tt.args.compress), WithFileNameGenerator(nameGenerator))
			defer w.Close()
			defer func() { os.Remove(w.currentFile.Name()) }()
			size, err := w.Write(tt.args.record)

			assert := assert.New(t)
			if tt.wantErr {
				assert.Error(err)
			} else {
				assert.NoError(err)
			}

			fi, _ := w.currentFile.Stat()
			//fmt.Printf("Size: %v %v\n", size, fi.Size())
			assert.Equal(tt.wantWrittenSize, size, "Writers reported size")
			assert.Equal(tt.wantFileSize, fi.Size(), "File size")
		})
	}
}
