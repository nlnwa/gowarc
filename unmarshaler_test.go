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
	"bufio"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"strconv"
	"strings"
	"testing"
)

func Test_unmarshaler_Unmarshal(t *testing.T) {
	type expected struct {
		version    *version
		recordType recordType
		headers    *warcFields
		content    []byte
	}
	tests := []struct {
		name          string
		input         string
		want          expected
		wantBytesRead int64
		wantErr       bool
	}{
		{"1", "WARC/1.0\r\n" +
			"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
			"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
			"WARC-Filename: temp-20170306040353.warc.gz\r\n" +
			"WARC-Type: warcinfo\r\n" +
			"Content-Type: application/warc-fields\r\n" +
			"Content-Length: 249\r\n" +
			"\r\n" +
			"software: Webrecorder Platform v3.7\r\n" +
			"format: WARC File Format 1.0\r\n" +
			"creator: temp-MJFXHZ4S\r\n" +
			"isPartOf: Temporary%20Collection\r\n" +
			"json-metadata: {\"title\": \"Temporary Collection\", \"size\": 2865, \"created_at\": 1488772924, \"type\": \"collection\", \"desc\": \"\"}\r\n" +
			"\r\n\r\n",
			expected{
				version:    V1_0,
				recordType: Warcinfo,
				headers: &warcFields{
					{WarcDate, "2017-03-06T04:03:53Z"},
					{WarcRecordID, "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					{WarcFilename, "temp-20170306040353.warc.gz"},
					{WarcType, "warcinfo"},
					{ContentType, "application/warc-fields"},
					{ContentLength, "249"},
				},
				content: []byte("software: Webrecorder Platform v3.7\r\nformat: WARC File Format 1.0\r\ncreator: temp-MJFXHZ4S\r\nisPartOf: Temporary%20Collection\r\njson-metadata: {\"title\": \"Temporary Collection\", \"size\": 2865, \"created_at\": 1488772924, \"type\": \"collection\", \"desc\": \"\"}\r\n"),
			}, 0, false},
		{"2", "WARC/1.0\n" +
			"WARC-Date: 2017-03-06T04:03:53Z\n" +
			"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\n" +
			"WARC-Filename: temp-20170306040353.warc.gz\n" +
			"WARC-Type: warcinfo\n" +
			"Content-Type: application/warc-fields\n" +
			"Content-Length: 244\n" +
			"\n" +
			"software: Webrecorder Platform v3.7\n" +
			"format: WARC File Format 1.0\n" +
			"creator: temp-MJFXHZ4S\n" +
			"isPartOf: Temporary%20Collection\n" +
			"json-metadata: {\"title\": \"Temporary Collection\", \"size\": 2865, \"created_at\": 1488772924, \"type\": \"collection\", \"desc\": \"\"}\n" +
			"\n\n",
			expected{
				version:    V1_0,
				recordType: Warcinfo,
				headers: &warcFields{
					{WarcDate, "2017-03-06T04:03:53Z"},
					{WarcRecordID, "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					{WarcFilename, "temp-20170306040353.warc.gz"},
					{WarcType, "warcinfo"},
					{ContentType, "application/warc-fields"},
					{ContentLength, "244"},
				},
				content: []byte("software: Webrecorder Platform v3.7\nformat: WARC File Format 1.0\ncreator: temp-MJFXHZ4S\nisPartOf: Temporary%20Collection\njson-metadata: {\"title\": \"Temporary Collection\", \"size\": 2865, \"created_at\": 1488772924, \"type\": \"collection\", \"desc\": \"\"}\n"),
			}, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := NewUnmarshaler(NewOptions())
			data := bufio.NewReader(strings.NewReader(tt.input))
			gotRecord, gotBytesRead, err := u.Unmarshal(data)

			assert := assert.New(t)

			fmt.Printf("ERR: %v\n", err)
			if (err != nil) != tt.wantErr {
				assert.Error(err, tt.wantErr)
				//t.Errorf("Unmarshal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			assert.Equal(tt.want.version, gotRecord.Version())
			assert.Equal(tt.want.recordType, gotRecord.Type())

			//assert.ElementsMatch(tt.want.headers, gotRecord.(*warcRecord).headers.(*warcHeader).WarcFields.(*warcFields).values)
			assert.ElementsMatch(*gotRecord.(*warcRecord).headers, *tt.want.headers)
			r, err := gotRecord.Block().RawBytes()
			assert.Nil(err)
			content, err := ioutil.ReadAll(r)
			assert.Nil(err)

			contentLength, _ := strconv.Atoi(gotRecord.WarcHeader().Get(ContentLength))
			assert.Equal(contentLength, len(content))
			assert.Equal(tt.want.content, content)

			//if !reflect.DeepEqual(gotRecord, tt.wantRecord) {
			//	ww := NewWriter(NewOptions(WithCompression(false)))
			//	gotBytesWritten, err := ww.WriteRecord(os.Stdout, gotRecord)
			//	fmt.Printf("\n******************* %v %v\n, %v\n", gotBytesRead, gotBytesWritten, err)
			//
			//	t.Errorf("Unmarshal() gotRecord = %v, want %v", gotRecord.WarcHeader(), tt.wantRecord)
			//}
			if gotBytesRead != tt.wantBytesRead {
				t.Errorf("Unmarshal() gotBytesRead = %v, want %v", gotBytesRead, tt.wantBytesRead)
			}
		})
	}
}
