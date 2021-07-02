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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"strconv"
	"strings"
	"testing"
)

func Test_unmarshaler_Unmarshal(t *testing.T) {
	type want struct {
		version    *version
		recordType recordType
		headers    *WarcFields
		blockType  interface{}
		content    string
		validation *Validation
		cached     bool
	}
	tests := []struct {
		name       string
		opts       []WarcRecordOption
		input      string
		want       want
		wantOffset int64
		wantErr    bool
	}{
		{
			"valid warcinfo record",
			[]WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail), WithUnknownRecordTypePolicy(ErrIgnore)},
			"WARC/1.0\r\n" +
				"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
				"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
				"WARC-Filename: temp-20170306040353.warc.gz\r\n" +
				"WARC-Type: warcinfo\r\n" +
				"Content-Type: application/warc-fields\r\n" +
				"Warc-Block-Digest: sha1:AF4D582B4FFC017D07A947D841E392A821F754F3\r\n" +
				"Content-Length: 240\r\n" +
				"\r\n" +
				"software: Veidemann v1.0\r\n" +
				"format: WARC File Format 1.1\r\n" +
				"creator: temp-MJFXHZ4S\r\n" +
				"isPartOf: Temporary%20Collection\r\n" +
				"json-metadata: {\"title\": \"Temporary Collection\", \"size\": 2865, \"created_at\": 1488772924, \"type\": \"collection\", \"desc\": \"\"}\r\n\r\n",
			want{
				version:    V1_0,
				recordType: Warcinfo,
				headers: &WarcFields{
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcType, Value: "warcinfo"},
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcFilename, Value: "temp-20170306040353.warc.gz"},
					&nameValue{Name: ContentType, Value: "application/warc-fields"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:AF4D582B4FFC017D07A947D841E392A821F754F3"},
					&nameValue{Name: ContentLength, Value: "240"},
				},
				blockType: &warcFieldsBlock{},
				content: "software: Veidemann v1.0\r\n" +
					"format: WARC File Format 1.1\r\n" +
					"creator: temp-MJFXHZ4S\r\n" +
					"isPartOf: Temporary%20Collection\r\n" +
					"json-metadata: {\"title\": \"Temporary Collection\", \"size\": 2865, \"created_at\": 1488772924, \"type\": \"collection\", \"desc\": \"\"}\r\n\r\n",
				validation: &Validation{},
				cached:     true,
			},
			0,
			false,
		},
		{
			"valid response record",
			[]WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail), WithUnknownRecordTypePolicy(ErrIgnore)},
			"WARC/1.0\r\n" +
				"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
				"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
				"WARC-Type: response\r\n" +
				"Content-Type: application/http;msgtype=response\r\n" +
				"Warc-Block-Digest: sha1:B285747AD7CC57AA74BCE2E30B453C8D1CB71BA4\r\n" +
				"Content-Length: 257\r\n" +
				"\r\n" +
				"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n" +
				"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n" +
				"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the content",
			want{
				V1_0,
				Response,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcType, Value: "response"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:B285747AD7CC57AA74BCE2E30B453C8D1CB71BA4"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
					&nameValue{Name: ContentLength, Value: "257"},
				},
				&httpResponseBlock{},
				"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n" +
					"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n" +
					"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the content",
				&Validation{},
				false,
			},
			0,
			false,
		},
		{
			"valid request record",
			[]WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail), WithUnknownRecordTypePolicy(ErrIgnore)},
			"WARC/1.0\r\n" +
				"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
				"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
				"WARC-Type: request\r\n" +
				"Content-Type: application/http;msgtype=request\r\n" +
				"Warc-Block-Digest: sha1:F45C9D37F9F7E5F822C86444F51D6CB252B7B33B\r\n" +
				"Content-Length: 262\r\n" +
				"\r\n" +
				"GET / HTTP/1.0\n" +
				"Host: example.com\n" +
				"Accept-Language: en-US,en;q=0.8,ru;q=0.6\n" +
				"Referer: http://example.com/foo.html\n" +
				"Connection: close\n" +
				"User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36\n",
			want{
				V1_0,
				Request,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcType, Value: "request"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=request"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:F45C9D37F9F7E5F822C86444F51D6CB252B7B33B"},
					&nameValue{Name: ContentLength, Value: "262"},
				},
				&httpRequestBlock{},
				"GET / HTTP/1.0\n" +
					"Host: example.com\n" +
					"Accept-Language: en-US,en;q=0.8,ru;q=0.6\n" +
					"Referer: http://example.com/foo.html\n" +
					"Connection: close\n" +
					"User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36\n",
				&Validation{},
				false,
			},
			0,
			false,
		},
		{
			"valid metadata record",
			[]WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail), WithUnknownRecordTypePolicy(ErrIgnore)},
			"WARC/1.0\r\n" +
				"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
				"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
				"WARC-Type: metadata\r\n" +
				"WARC-Concurrent-To: <urn:uuid:e7c9eff8-f5bc-4aeb-b3d2-9d3df99afb30>\r\n" +
				"Content-Type: application/warc-fields\r\n" +
				"Warc-Block-Digest: sha1:6D924D4C99268BE486042E655B06A83133EFEB59\r\n" +
				"Content-Length: 64\r\n" +
				"\r\n" +
				"via: http://www.example.com/\r\n" +
				"hopsFromSeed: P\r\n" +
				"fetchTimeMs: 47\r\n",
			want{
				V1_0,
				Metadata,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcType, Value: "metadata"},
					&nameValue{Name: ContentType, Value: "application/warc-fields"},
					&nameValue{Name: WarcConcurrentTo, Value: "<urn:uuid:e7c9eff8-f5bc-4aeb-b3d2-9d3df99afb30>"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:6D924D4C99268BE486042E655B06A83133EFEB59"},
					&nameValue{Name: ContentLength, Value: "64"},
				},
				&warcFieldsBlock{},
				"via: http://www.example.com/\r\n" +
					"hopsFromSeed: P\r\n" +
					"fetchTimeMs: 47\r\n",
				&Validation{},
				true,
			},
			0,
			false,
		},
		{
			"valid resource record",
			[]WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail), WithUnknownRecordTypePolicy(ErrIgnore)},
			"WARC/1.0\r\n" +
				"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
				"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
				"WARC-Type: resource\r\n" +
				"Content-Type: text/html\r\n" +
				"WARC-Target-URI: file://var/www/htdoc/index.html\r\n" +
				"WARC-Concurrent-To: <urn:uuid:e7c9eff8-f5bc-4aeb-b3d2-9d3df99afb30>\r\n" +
				"WARC-Payload-Digest: sha1:307E7DFCAF9A8EA4C4E86A11BCAA83AC6698017F\r\n" +
				"WARC-Block-Digest: sha1:307E7DFCAF9A8EA4C4E86A11BCAA83AC6698017F\r\n" +
				"Content-Length: 42\r\n" +
				"\r\n" +
				"<html><head></head>\n" +
				"<body></body>\n" +
				"</html>\n",
			want{
				V1_0,
				Resource,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcType, Value: "resource"},
					&nameValue{Name: ContentType, Value: "text/html"},
					&nameValue{Name: WarcTargetURI, Value: "file://var/www/htdoc/index.html"},
					&nameValue{Name: WarcConcurrentTo, Value: "<urn:uuid:e7c9eff8-f5bc-4aeb-b3d2-9d3df99afb30>"},
					&nameValue{Name: WarcPayloadDigest, Value: "sha1:307E7DFCAF9A8EA4C4E86A11BCAA83AC6698017F"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:307E7DFCAF9A8EA4C4E86A11BCAA83AC6698017F"},
					&nameValue{Name: ContentLength, Value: "42"},
				},
				&genericBlock{},
				"<html><head></head>\n" +
					"<body></body>\n" +
					"</html>\n",
				&Validation{},
				false,
			},
			0,
			false,
		},
		{
			"valid revisit record",
			[]WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail), WithUnknownRecordTypePolicy(ErrIgnore)},
			"WARC/1.0\r\n" +
				"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
				"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
				"WARC-Type: revisit\r\n" +
				"Content-Type: message/http\r\n" +
				"WARC-Target-URI: http://www.example.org/images/logo.jpg\r\n" +
				"WARC-Profile: http://netpreserve.org/warc/1.1/server-not-modified\r\n" +
				"WARC-Refers-To: <urn:uuid:92283950-ef2f-4d72-b224-f54c6ec90bb0>\r\n" +
				"WARC-Refers-To-Target-URI: http://www.example.org/images/logo.jpg\r\n" +
				"WARC-Refers-To-Date: 2016-09-19T17:20:24Z\r\n" +
				"Warc-Block-Digest: sha1:7B71E2CE461E4685EED55612850EE0CBB3876EDF\r\n" +
				"Content-Length: 195\r\n" +
				"\r\n" +
				"HTTP/1.x 304 Not Modified\n" +
				"Date: Tue, 06 Mar 2017 00:43:35 GMT\n" +
				"Server: Apache/2.0.54 (Ubuntu) PHP/5.0.5-2ubuntu1.4 Connection: Keep-Alive\n" +
				"Keep-Alive: timeout=15, max=100\n" +
				"ETag: \"3e45-67e-2ed02ec0\"\n",
			want{
				V1_0,
				Revisit,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcType, Value: "revisit"},
					&nameValue{Name: WarcTargetURI, Value: "http://www.example.org/images/logo.jpg"},
					&nameValue{Name: WarcProfile, Value: "http://netpreserve.org/warc/1.1/server-not-modified"},
					&nameValue{Name: WarcRefersTo, Value: "<urn:uuid:92283950-ef2f-4d72-b224-f54c6ec90bb0>"},
					&nameValue{Name: WarcRefersToTargetURI, Value: "http://www.example.org/images/logo.jpg"},
					&nameValue{Name: WarcRefersToDate, Value: "2016-09-19T17:20:24Z"},
					&nameValue{Name: ContentType, Value: "message/http"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:7B71E2CE461E4685EED55612850EE0CBB3876EDF"},
					&nameValue{Name: ContentLength, Value: "195"},
				},
				&genericBlock{},
				"HTTP/1.x 304 Not Modified\n" +
					"Date: Tue, 06 Mar 2017 00:43:35 GMT\n" +
					"Server: Apache/2.0.54 (Ubuntu) PHP/5.0.5-2ubuntu1.4 Connection: Keep-Alive\n" +
					"Keep-Alive: timeout=15, max=100\n" +
					"ETag: \"3e45-67e-2ed02ec0\"\n",
				&Validation{},
				false,
			},
			0,
			false,
		},
		{
			"valid conversion record",
			[]WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail), WithUnknownRecordTypePolicy(ErrIgnore)},
			"WARC/1.0\r\n" +
				"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
				"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
				"WARC-Type: conversion\r\n" +
				"WARC-Target-URI: http://www.example.org/index.html\r\n" +
				"WARC-Refers-To: <urn:uuid:92283950-ef2f-4d72-b224-f54c6ec90bb0>\r\n" +
				"Content-Type: text/plain\r\n" +
				"Warc-Block-Digest: sha1:581F7F1CA3D3EB023438808309678ED6D03E2895\r\n" +
				"Content-Length: 10\r\n" +
				"\r\n" +
				"body text\n",
			want{
				V1_0,
				Conversion,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcTargetURI, Value: "http://www.example.org/index.html"},
					&nameValue{Name: WarcRefersTo, Value: "<urn:uuid:92283950-ef2f-4d72-b224-f54c6ec90bb0>"},
					&nameValue{Name: WarcType, Value: "conversion"},
					&nameValue{Name: ContentType, Value: "text/plain"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:581F7F1CA3D3EB023438808309678ED6D03E2895"},
					&nameValue{Name: ContentLength, Value: "10"},
				},
				&genericBlock{},
				"body text\n",
				&Validation{},
				false,
			},
			0,
			false,
		},
		{
			"valid continuation record",
			[]WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail), WithUnknownRecordTypePolicy(ErrIgnore)},
			"WARC/1.0\r\n" +
				"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
				"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
				"WARC-Target-URI: http://www.example.org/index.html\r\n" +
				"WARC-Type: continuation\r\n" +
				"Content-Type: text/plain\r\n" +
				"WARC-Segment-Origin-ID: <urn:uuid:39509228-ae2f-11b2-763a-aa4c6ec90bb0>\r\n" +
				"WARC-Segment-Number: 2\r\n" +
				"WARC-Segment-Total-Length: 1982\r\n" +
				"WARC-Block-Digest: sha1:62B805E388394FF22747D1B10476EA04309CB5A8\r\n" +
				"WARC-Payload-Digest: sha1:CCHXETFVJD2MUZY6ND6SS7ZENMWF7KQ2\r\n" +
				"Content-Length: 22\r\n" +
				"\r\n" +
				"... last part of data\n",
			want{
				V1_0,
				Continuation,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcTargetURI, Value: "http://www.example.org/index.html"},
					&nameValue{Name: WarcType, Value: "continuation"},
					&nameValue{Name: WarcSegmentOriginID, Value: "<urn:uuid:39509228-ae2f-11b2-763a-aa4c6ec90bb0>"},
					&nameValue{Name: WarcSegmentNumber, Value: "2"},
					&nameValue{Name: WarcSegmentTotalLength, Value: "1982"},
					&nameValue{Name: ContentType, Value: "text/plain"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:62B805E388394FF22747D1B10476EA04309CB5A8"},
					&nameValue{Name: WarcPayloadDigest, Value: "sha1:CCHXETFVJD2MUZY6ND6SS7ZENMWF7KQ2"},
					&nameValue{Name: ContentLength, Value: "22"},
				},
				&genericBlock{},
				"... last part of data\n",
				&Validation{},
				false,
			},
			0,
			false,
		},
		{
			"valid unknown record type",
			[]WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail), WithUnknownRecordTypePolicy(ErrIgnore)},
			"WARC/1.0\r\n" +
				"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
				"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
				"WARC-Target-URI: http://www.example.org/index.html\r\n" +
				"WARC-Type: myType\r\n" +
				"My-Field: MyValue\r\n" +
				"Content-Type: text/plain\r\n" +
				"Warc-Block-Digest: sha1:7FE70820E08A1AAC0EF224D9C66AB66831CC4AB1\r\n" +
				"Content-Length: 8\r\n" +
				"\r\n" +
				"content\n",
			want{
				V1_0,
				0,
				&WarcFields{
					&nameValue{Name: WarcType, Value: "myType"},
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcTargetURI, Value: "http://www.example.org/index.html"},
					&nameValue{Name: "My-Field", Value: "MyValue"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:7FE70820E08A1AAC0EF224D9C66AB66831CC4AB1"},
					&nameValue{Name: ContentType, Value: "text/plain"},
					&nameValue{Name: ContentLength, Value: "8"},
				},
				&genericBlock{},
				"content\n",
				&Validation{},
				false,
			},
			0,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)
			require := require.New(t)

			u := NewUnmarshaler(tt.opts...)
			data := bufio.NewReader(strings.NewReader(tt.input))
			gotRecord, gotOffset, validation, err := u.Unmarshal(data)
			if tt.wantErr {
				require.Error(err)
			} else {
				require.NoError(err)
			}
			assert.True(validation.Valid(), validation.String())

			assert.Equal(tt.want.version, gotRecord.Version(), "Record version")
			assert.Equal(tt.want.recordType, gotRecord.Type(), "Record type")
			assert.IsType(tt.want.blockType, gotRecord.Block(), "Block type")
			assert.Equal(tt.want.cached, gotRecord.Block().IsCached(), "IsCached")

			assert.ElementsMatch(*tt.want.headers, *gotRecord.(*warcRecord).headers)
			r, err := gotRecord.Block().RawBytes()
			assert.Nil(err)
			content, err := ioutil.ReadAll(r)
			assert.Nil(err)

			contentLength, _ := strconv.Atoi(gotRecord.WarcHeader().Get(ContentLength))
			assert.Equal(contentLength, len(content), "ContentLength")
			assert.Equal(tt.want.content, string(content), "Content")
			assert.Equal(tt.wantOffset, gotOffset, "Offset")
		})
	}
}
