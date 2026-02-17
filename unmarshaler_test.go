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
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/klauspost/compress/gzip"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_unmarshaler_Unmarshal(t *testing.T) {
	type want struct {
		version    *WarcVersion
		recordType RecordType
		headers    *WarcFields
		blockType  any
		content    string
		validation []error
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
				"Content-Length: 238\r\n" +
				"\r\n" +
				"software: Veidemann v1.0\r\n" +
				"format: WARC File Format 1.1\r\n" +
				"creator: temp-MJFXHZ4S\r\n" +
				"isPartOf: Temporary%20Collection\r\n" +
				"json-metadata: {\"title\": \"Temporary Collection\", \"size\": 2865, \"created_at\": 1488772924, \"type\": \"collection\", \"desc\": \"\"}\r\n" +
				"\r\n\r\n",
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
					&nameValue{Name: ContentLength, Value: "238"},
				},
				blockType: &warcFieldsBlock{},
				content: "software: Veidemann v1.0\r\n" +
					"format: WARC File Format 1.1\r\n" +
					"creator: temp-MJFXHZ4S\r\n" +
					"isPartOf: Temporary%20Collection\r\n" +
					"json-metadata: {\"title\": \"Temporary Collection\", \"size\": 2865, \"created_at\": 1488772924, \"type\": \"collection\", \"desc\": \"\"}\r\n",
				validation: []error(nil),
				cached:     true,
			},
			0,
			false,
		},
		{
			"valid response record",
			[]WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail), WithUnknownRecordTypePolicy(ErrIgnore), WithAddMissingDigest(false)},
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
				"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the content" +
				"\r\n\r\n",
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
				[]error(nil),
				true,
			},
			0,
			false,
		},
		{
			"valid dns response record",
			[]WarcRecordOption{WithSpecViolationPolicy(ErrWarn), WithSyntaxErrorPolicy(ErrWarn),
				WithAddMissingDigest(true),
				WithFixSyntaxErrors(false),
				WithFixDigest(false),
				WithAddMissingContentLength(false),
				WithAddMissingRecordId(false),
				WithFixContentLength(false)},
			"WARC/1.0\r\n" +
				"WARC-Type: response\r\n" +
				"WARC-Target-URI: dns:ergoterapeutene.org\r\n" +
				"WARC-Date: 2019-11-13T23:23:34Z\r\n" +
				"WARC-IP-Address: 127.0.0.1\r\n" +
				"WARC-Record-ID: <urn:uuid:ac971c52-f8da-434c-809b-e401d915d945>\r\n" +
				"Content-Type: text/dns\r\n" +
				"Content-Length: 60\r\n" +
				"\r\n" +
				"20191113232334\n" +
				"ergoterapeutene.org.\t300\tIN\tA\t195.159.29.211\n" +
				"\r\n\r\n",
			want{
				V1_0,
				Response,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2019-11-13T23:23:34Z"},
					&nameValue{Name: WarcTargetURI, Value: "dns:ergoterapeutene.org"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:ac971c52-f8da-434c-809b-e401d915d945>"},
					&nameValue{Name: WarcIPAddress, Value: "127.0.0.1"},
					&nameValue{Name: WarcType, Value: "response"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:4C44B5E46JR5DGLKCD7W3IIL2YQNPCZ6"},
					&nameValue{Name: ContentType, Value: "text/dns"},
					&nameValue{Name: ContentLength, Value: "60"},
				},
				&genericBlock{},
				"20191113232334\n" +
					"ergoterapeutene.org.\t300\tIN\tA\t195.159.29.211\n",
				[]error(nil),
				true,
			},
			0,
			false,
		},
		{
			"valid request record",
			[]WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail), WithUnknownRecordTypePolicy(ErrIgnore), WithAddMissingDigest(false)},
			"WARC/1.0\r\n" +
				"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
				"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
				"WARC-Type: request\r\n" +
				"Content-Type: application/http;msgtype=request\r\n" +
				"Warc-Block-Digest: sha1:A3781FF1FC3FB52318F623E22C85D63D74C12932\r\n" +
				"Content-Length: 263\r\n" +
				"\r\n" +
				"GET / HTTP/1.0\n" +
				"Host: example.com\n" +
				"Accept-Language: en-US,en;q=0.8,ru;q=0.6\n" +
				"Referer: http://example.com/foo.html\n" +
				"Connection: close\n" +
				"User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36\n\n" +
				"\r\n\r\n",
			want{
				V1_0,
				Request,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcType, Value: "request"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=request"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:A3781FF1FC3FB52318F623E22C85D63D74C12932"},
					&nameValue{Name: ContentLength, Value: "263"},
				},
				&httpRequestBlock{},
				"GET / HTTP/1.0\n" +
					"Host: example.com\n" +
					"Accept-Language: en-US,en;q=0.8,ru;q=0.6\n" +
					"Referer: http://example.com/foo.html\n" +
					"Connection: close\n" +
					"User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36\n\n",
				[]error(nil),
				true,
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
				"fetchTimeMs: 47\r\n" +
				"\r\n\r\n",
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
				[]error(nil),
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
				"</html>\n" +
				"\r\n\r\n",
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
				[]error(nil),
				true,
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
				"ETag: \"3e45-67e-2ed02ec0\"\n" +
				"\r\n\r\n",
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
				&revisitBlock{},
				"HTTP/1.x 304 Not Modified\n" +
					"Date: Tue, 06 Mar 2017 00:43:35 GMT\n" +
					"Server: Apache/2.0.54 (Ubuntu) PHP/5.0.5-2ubuntu1.4 Connection: Keep-Alive\n" +
					"Keep-Alive: timeout=15, max=100\n" +
					"ETag: \"3e45-67e-2ed02ec0\"\n",
				[]error(nil),
				true,
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
				"body text\n" +
				"\r\n\r\n",
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
				[]error(nil),
				true,
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
				"... last part of data\n" +
				"\r\n\r\n",
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
				[]error(nil),
				true,
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
				"content\n" +
				"\r\n\r\n",
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
				[]error(nil),
				true,
			},
			0,
			false,
		},
		{
			"metadata record missing end marker",
			[]WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail), WithUnknownRecordTypePolicy(ErrIgnore), WithAddMissingDigest(false)},
			"WARC/1.0\r\n" +
				"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
				"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
				"WARC-Type: metadata\r\n" +
				"Content-Type: application/warc-fields\r\n" +
				"Content-Length: 10\r\n" +
				"\r\n" +
				"foo: bar\r\n",
			want{
				V1_0,
				Metadata,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcType, Value: "metadata"},
					&nameValue{Name: ContentType, Value: "application/warc-fields"},
					&nameValue{Name: ContentLength, Value: "10"},
				},
				&warcFieldsBlock{},
				"foo: bar\r\n",
				[]error(nil),
				true,
			},
			0,
			true,
		},
		{
			"metadata record only newline end marker",
			[]WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail), WithUnknownRecordTypePolicy(ErrIgnore), WithAddMissingDigest(false)},
			"WARC/1.0\r\n" +
				"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
				"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
				"WARC-Type: metadata\r\n" +
				"Content-Type: application/warc-fields\r\n" +
				"Content-Length: 10\r\n" +
				"\r\n" +
				"foo: bar\r\n" +
				"\n\n",
			want{
				V1_0,
				Metadata,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcType, Value: "metadata"},
					&nameValue{Name: ContentType, Value: "application/warc-fields"},
					&nameValue{Name: ContentLength, Value: "10"},
				},
				&warcFieldsBlock{},
				"foo: bar\r\n",
				[]error(nil),
				true,
			},
			0,
			true,
		},
		{
			"metadata record only one end marker",
			[]WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail), WithUnknownRecordTypePolicy(ErrIgnore), WithAddMissingDigest(false)},
			"WARC/1.0\r\n" +
				"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
				"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
				"WARC-Type: metadata\r\n" +
				"Content-Type: application/warc-fields\r\n" +
				"Content-Length: 10\r\n" +
				"\r\n" +
				"foo: bar\r\n" +
				"\r\n",
			want{
				V1_0,
				Metadata,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcType, Value: "metadata"},
					&nameValue{Name: ContentType, Value: "application/warc-fields"},
					&nameValue{Name: ContentLength, Value: "10"},
				},
				&warcFieldsBlock{},
				"foo: bar\r\n",
				[]error(nil),
				true,
			},
			0,
			true,
		},
		{
			"metadata record only one newline end marker",
			[]WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail), WithUnknownRecordTypePolicy(ErrIgnore), WithAddMissingDigest(false)},
			"WARC/1.0\r\n" +
				"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
				"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
				"WARC-Type: metadata\r\n" +
				"Content-Type: application/warc-fields\r\n" +
				"Content-Length: 10\r\n" +
				"\r\n" +
				"foo: bar\r\n" +
				"\n",
			want{
				V1_0,
				Metadata,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcType, Value: "metadata"},
					&nameValue{Name: ContentType, Value: "application/warc-fields"},
					&nameValue{Name: ContentLength, Value: "10"},
				},
				&warcFieldsBlock{},
				"foo: bar\r\n",
				[]error(nil),
				true,
			},
			0,
			true,
		},
		{
			"metadata record missing carriage return in warc-fields block",
			[]WarcRecordOption{
				WithSpecViolationPolicy(ErrWarn),
				WithSyntaxErrorPolicy(ErrWarn),
				WithAddMissingDigest(false),
				WithFixSyntaxErrors(false),
				WithFixDigest(false),
				WithAddMissingContentLength(false),
				WithAddMissingRecordId(false),
				WithFixContentLength(false),
			},
			"WARC/1.0\r\n" +
				"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
				"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
				"WARC-Type: metadata\r\n" +
				"Content-Type: application/warc-fields\r\n" +
				"Content-Length: 18\r\n" +
				"WARC-Block-Digest: sha1:QYG3QQJ4ULYPJGSJL34IS3U7VUAJFSKY\r\n" +
				"\r\n" +
				"foo: bar\n" +
				"food:bar\n" +
				"\r\n" +
				"\r\n",
			want{
				V1_0,
				Metadata,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcType, Value: "metadata"},
					&nameValue{Name: ContentType, Value: "application/warc-fields"},
					&nameValue{Name: ContentLength, Value: "18"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:QYG3QQJ4ULYPJGSJL34IS3U7VUAJFSKY"},
				},
				&warcFieldsBlock{},
				"foo: bar\nfood:bar\n",
				[]error(nil),
				true,
			},
			0,
			false,
		},
		{
			"metadata record missing carriage return in warc-fields block with fix syntax errors",
			[]WarcRecordOption{
				WithSpecViolationPolicy(ErrWarn),
				WithSyntaxErrorPolicy(ErrWarn),
				WithAddMissingDigest(true),
				WithFixSyntaxErrors(true),
				WithFixDigest(true),
				WithAddMissingContentLength(false),
				WithAddMissingRecordId(false),
				WithFixContentLength(true),
				WithFixWarcFieldsBlockErrors(true),
			},
			"WARC/1.0\r\n" +
				"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
				"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
				"WARC-Type: metadata\r\n" +
				"Content-Type: application/warc-fields\r\n" +
				"Content-Length: 18\r\n" +
				"WARC-Block-Digest: sha1:QYG3QQJ4ULYPJGSJL34IS3U7VUAJFSKY\r\n" +
				"\r\n" +
				"foo: bar\n" +
				"food:bar\n" +
				"\r\n" +
				"\r\n",
			want{
				V1_0,
				Metadata,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcType, Value: "metadata"},
					&nameValue{Name: ContentType, Value: "application/warc-fields"},
					&nameValue{Name: ContentLength, Value: "21"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:U2AN4MFP7IITXSOLYH2QTIPVDNJOHBFO"},
				},
				&warcFieldsBlock{},
				"Foo: bar\r\nFood: bar\r\n",
				[]error{
					fmt.Errorf("content length mismatch. header: 18, actual: 21"),
					fmt.Errorf("block: %w", fmt.Errorf("wrong digest: expected sha1:QYG3QQJ4ULYPJGSJL34IS3U7VUAJFSKY, computed: sha1:U2AN4MFP7IITXSOLYH2QTIPVDNJOHBFO")),
				},
				true,
			},
			0,
			false,
		},
		{
			"metadata record missing carriage return in warc-fields block with BlockeErrorPolicy warn",
			[]WarcRecordOption{
				WithSpecViolationPolicy(ErrWarn),
				WithSyntaxErrorPolicy(ErrWarn),
				WithBlockErrorPolicy(ErrWarn),
				WithAddMissingDigest(false),
				WithFixSyntaxErrors(false),
				WithFixDigest(false),
				WithAddMissingContentLength(false),
				WithAddMissingRecordId(false),
				WithFixContentLength(false),
			},
			"WARC/1.0\r\n" +
				"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
				"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
				"WARC-Type: metadata\r\n" +
				"Content-Type: application/warc-fields\r\n" +
				"Content-Length: 18\r\n" +
				"WARC-Block-Digest: sha1:QYG3QQJ4ULYPJGSJL34IS3U7VUAJFSKY\r\n" +
				"\r\n" +
				"foo: bar\n" +
				"food:bar\n" +
				"\r\n" +
				"\r\n",
			want{
				V1_0,
				Metadata,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcType, Value: "metadata"},
					&nameValue{Name: ContentType, Value: "application/warc-fields"},
					&nameValue{Name: ContentLength, Value: "18"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:QYG3QQJ4ULYPJGSJL34IS3U7VUAJFSKY"},
				},
				&warcFieldsBlock{},
				"foo: bar\nfood:bar\n",
				[]error{
					newWrappedSyntaxError("error in warc fields block", newSyntaxErrorAtLine("missing carriage return", 1)),
					newWrappedSyntaxError("error in warc fields block", newSyntaxErrorAtLine("missing carriage return", 2)),
				},
				true,
			},
			0,
			false,
		},
		{
			"metadata record missing carriage return in warc-fields block with fix syntax errors and BlockeErrorPolicy warn",
			[]WarcRecordOption{
				WithSpecViolationPolicy(ErrWarn),
				WithSyntaxErrorPolicy(ErrWarn),
				WithBlockErrorPolicy(ErrWarn),
				WithAddMissingDigest(true),
				WithFixSyntaxErrors(true),
				WithFixDigest(true),
				WithAddMissingContentLength(false),
				WithAddMissingRecordId(false),
				WithFixContentLength(true),
				WithFixWarcFieldsBlockErrors(true),
			},
			"WARC/1.0\r\n" +
				"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
				"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
				"WARC-Type: metadata\r\n" +
				"Content-Type: application/warc-fields\r\n" +
				"Content-Length: 18\r\n" +
				"WARC-Block-Digest: sha1:QYG3QQJ4ULYPJGSJL34IS3U7VUAJFSKY\r\n" +
				"\r\n" +
				"foo: bar\n" +
				"food:bar\n" +
				"\r\n" +
				"\r\n",
			want{
				V1_0,
				Metadata,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcType, Value: "metadata"},
					&nameValue{Name: ContentType, Value: "application/warc-fields"},
					&nameValue{Name: ContentLength, Value: "21"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:U2AN4MFP7IITXSOLYH2QTIPVDNJOHBFO"},
				},
				&warcFieldsBlock{},
				"Foo: bar\r\nFood: bar\r\n",
				[]error{
					newWrappedSyntaxError("error in warc fields block", newSyntaxErrorAtLine("missing carriage return", 1)),
					newWrappedSyntaxError("error in warc fields block", newSyntaxErrorAtLine("missing carriage return", 2)),
					fmt.Errorf("content length mismatch. header: 18, actual: 21"),
					fmt.Errorf("block: %w", fmt.Errorf("wrong digest: expected sha1:QYG3QQJ4ULYPJGSJL34IS3U7VUAJFSKY, computed: sha1:U2AN4MFP7IITXSOLYH2QTIPVDNJOHBFO")),
				},
				true,
			},
			0,
			false,
		},
		{
			"short response record",
			[]WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail), WithUnknownRecordTypePolicy(ErrIgnore)},
			"WARC/1.0\r\n" +
				"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
				"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
				"WARC-Type: response\r\n" +
				"Content-Type: application/http;msgtype=response\r\n" +
				"Content-Length: 257\r\n" +
				"\r\n" +
				"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n" +
				"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n" +
				"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the " +
				"\r\n\r\n",
			want{
				V1_0,
				Response,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcType, Value: "response"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
					&nameValue{Name: ContentLength, Value: "257"},
				},
				&httpResponseBlock{},
				"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n" +
					"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n" +
					"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the " +
					"\r\n\r\n",
				[]error(nil),
				false,
			},
			0,
			true,
		},
		{
			"request record missing end of http header marker - warn",
			[]WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrWarn), WithUnknownRecordTypePolicy(ErrIgnore), WithAddMissingDigest(false), WithFixSyntaxErrors(false)},
			"WARC/1.1\r\n" +
				"WARC-Type: request\r\n" +
				"WARC-Target-URI: http://www.archive.org/images/logoc.jpg\r\n" +
				"WARC-Warcinfo-ID: <urn:uuid:d7ae5c10-e6b3-4d27-967d-34780c58ba39>\r\n" +
				"WARC-Date: 2016-09-19T17:20:24Z\r\n" +
				"Content-Length: 240\r\n" +
				"WARC-Record-ID: <urn:uuid:4885803b-eebd-4b27-a090-144450c11594>\r\n" +
				"Content-Type: application/http;msgtype=request\r\n" +
				"WARC-Concurrent-To: <urn:uuid:92283950-ef2f-4d72-b224-f54c6ec90bb0>\r\n" +
				"\r\n" +
				"GET /images/logoc.jpg HTTP/1.0\r\n" +
				"User-Agent: Mozilla/5.0 (compatible; heritrix/1.10.0)\r\n" +
				"From: stack@example.org\r\n" +
				"Connection: close\r\n" +
				"Referer: http://www.archive.org/\r\n" +
				"Host: www.archive.org\r\n" +
				"Cookie: PHPSESSID=009d7bb11022f80605aa87e18224d824\r\n" +
				"\r\n\r\n",
			want{
				V1_1,
				Request,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2016-09-19T17:20:24Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:4885803b-eebd-4b27-a090-144450c11594>"},
					&nameValue{Name: WarcType, Value: "request"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=request"},
					&nameValue{Name: ContentLength, Value: "240"},
					&nameValue{Name: WarcTargetURI, Value: "http://www.archive.org/images/logoc.jpg"},
					&nameValue{Name: WarcWarcinfoID, Value: "<urn:uuid:d7ae5c10-e6b3-4d27-967d-34780c58ba39>"},
					&nameValue{Name: WarcConcurrentTo, Value: "<urn:uuid:92283950-ef2f-4d72-b224-f54c6ec90bb0>"},
				},
				&httpRequestBlock{},
				"GET /images/logoc.jpg HTTP/1.0\r\n" +
					"User-Agent: Mozilla/5.0 (compatible; heritrix/1.10.0)\r\n" +
					"From: stack@example.org\r\n" +
					"Connection: close\r\n" +
					"Referer: http://www.archive.org/\r\n" +
					"Host: www.archive.org\r\n" +
					"Cookie: PHPSESSID=009d7bb11022f80605aa87e18224d824\r\n",
				[]error{errMissingEndOfHeaders},
				true,
			},
			0,
			false,
		},
		{
			"request record missing end of http header marker - fail",
			[]WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail), WithUnknownRecordTypePolicy(ErrIgnore)},
			"WARC/1.1\r\n" +
				"WARC-Type: request\r\n" +
				"WARC-Target-URI: http://www.archive.org/images/logoc.jpg\r\n" +
				"WARC-Warcinfo-ID: <urn:uuid:d7ae5c10-e6b3-4d27-967d-34780c58ba39>\r\n" +
				"WARC-Date: 2016-09-19T17:20:24Z\r\n" +
				"Content-Length: 240\r\n" +
				"WARC-Record-ID: <urn:uuid:4885803b-eebd-4b27-a090-144450c11594>\r\n" +
				"Content-Type: application/http;msgtype=request\r\n" +
				"WARC-Concurrent-To: <urn:uuid:92283950-ef2f-4d72-b224-f54c6ec90bb0>\r\n" +
				"\r\n" +
				"GET /images/logoc.jpg HTTP/1.0\r\n" +
				"User-Agent: Mozilla/5.0 (compatible; heritrix/1.10.0)\r\n" +
				"From: stack@example.org\r\n" +
				"Connection: close\r\n" +
				"Referer: http://www.archive.org/\r\n" +
				"Host: www.archive.org\r\n" +
				"Cookie: PHPSESSID=009d7bb11022f80605aa87e18224d824\r\n" +
				"\r\n\r\n",
			want{
				V1_1,
				Request,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2016-09-19T17:20:24Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:4885803b-eebd-4b27-a090-144450c11594>"},
					&nameValue{Name: WarcType, Value: "request"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=request"},
					&nameValue{Name: ContentLength, Value: "240"},
					&nameValue{Name: WarcTargetURI, Value: "http://www.archive.org/images/logoc.jpg"},
					&nameValue{Name: WarcWarcinfoID, Value: "<urn:uuid:d7ae5c10-e6b3-4d27-967d-34780c58ba39>"},
					&nameValue{Name: WarcConcurrentTo, Value: "<urn:uuid:92283950-ef2f-4d72-b224-f54c6ec90bb0>"},
				},
				&httpRequestBlock{},
				"GET /images/logoc.jpg HTTP/1.0\r\n" +
					"User-Agent: Mozilla/5.0 (compatible; heritrix/1.10.0)\r\n" +
					"From: stack@example.org\r\n" +
					"Connection: close\r\n" +
					"Referer: http://www.archive.org/\r\n" +
					"Host: www.archive.org\r\n" +
					"Cookie: PHPSESSID=009d7bb11022f80605aa87e18224d824\r\n\r\n",
				[]error(nil),
				false,
			},
			0,
			true,
		},
		{
			"truncated record",
			[]WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail), WithUnknownRecordTypePolicy(ErrIgnore), WithAddMissingDigest(false)},
			"WARC/1.0\r\n" +
				"WARC-Type: revisit\r\n" +
				"WARC-Target-URI: https://www.google.com:443/\r\n" +
				"WARC-Profile: http://netpreserve.org/warc/1.1/server-not-modified\r\n" +
				"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
				"Content-Length: 0\r\n" +
				"Content-Type: application/http;msgtype=response\r\n" +
				"WARC-Date: 2022-11-11T00:01:40Z\r\n",
			want{
				V1_0,
				Revisit,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2022-11-11T00:01:40Z"},
					&nameValue{Name: WarcProfile, Value: "http://netpreserve.org/warc/1.1/server-not-modified"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcTargetURI, Value: "https://www.google.com:443/"},
					&nameValue{Name: WarcType, Value: "revisit"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
					&nameValue{Name: ContentLength, Value: "0"},
				},
				&revisitBlock{},
				"",
				[]error(nil),
				true,
			},
			0,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)
			require := require.New(t)

			u := NewUnmarshaler(tt.opts...)
			data := bufio.NewReader(strings.NewReader(tt.input))
			gotRecord, gotOffset, validation, err1 := u.Unmarshal(data)

			if err1 != nil && tt.wantErr {
				return
			}

			if !tt.wantErr {
				require.NoError(err1)
			}

			assert.Equal(tt.want.version, gotRecord.Version(), "Record version")
			assert.Equal(tt.want.recordType, gotRecord.Type(), "Record type")
			assert.IsType(tt.want.blockType, gotRecord.Block(), "Block type")
			assert.Equal(tt.want.cached, gotRecord.Block().IsCached(), "IsCached")

			assert.ElementsMatch(*tt.want.headers, *gotRecord.(*warcRecord).headers)
			r, err := gotRecord.Block().RawBytes()
			assert.Nil(err)
			content, err := io.ReadAll(r)
			assert.Nil(err)

			assert.Equal(tt.want.content, string(content), "Content")
			assert.Equal(tt.wantOffset, gotOffset, "Offset")

			v2, err2 := gotRecord.ValidateDigest()
			if !tt.wantErr {
				require.NoError(err2)
			}
			validation = append(validation, v2...)

			err3 := gotRecord.Close()
			if tt.wantErr {
				require.Error(errors.Join(err1, err2, err3))
			} else {
				require.NoError(err3)
			}

			assert.Equal(tt.want.validation, validation, "Want:\n %s\nGot:\n %s", tt.want.validation, fmt.Sprint(validation))
		})
	}
}

var unmarshallerBenchmarkResult any

func Test_unmarshaler_Unmarshal_GarbageBeforeRecord_ErrFail(t *testing.T) {
	input := "GARBAGE" +
		"WARC/1.0\r\n" +
		"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
		"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
		"WARC-Type: warcinfo\r\n" +
		"Content-Type: application/warc-fields\r\n" +
		"Content-Length: 0\r\n" +
		"\r\n" +
		"\r\n\r\n"

	u := NewUnmarshaler(WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail))
	data := bufio.NewReader(strings.NewReader(input))
	_, _, _, err := u.Unmarshal(data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected start of record")
}

func Test_unmarshaler_Unmarshal_GarbageBeforeRecord_ErrWarn(t *testing.T) {
	input := "XX" +
		"WARC/1.0\r\n" +
		"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
		"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
		"WARC-Type: warcinfo\r\n" +
		"Content-Type: application/warc-fields\r\n" +
		"Content-Length: 0\r\n" +
		"\r\n" +
		"\r\n\r\n"

	u := NewUnmarshaler(WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrWarn))
	data := bufio.NewReader(strings.NewReader(input))
	rec, offset, validation, err := u.Unmarshal(data)
	require.NoError(t, err)
	defer rec.Close()
	assert.Equal(t, int64(2), offset)
	assert.False(t, len(validation) == 0)
	assert.Contains(t, fmt.Sprint(validation), "bytes after expected offset")
}

func Test_unmarshaler_Unmarshal_UnsupportedVersion_ErrFail(t *testing.T) {
	input := "WARC/9.9\r\n" +
		"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
		"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
		"WARC-Type: warcinfo\r\n" +
		"Content-Type: application/warc-fields\r\n" +
		"Content-Length: 0\r\n" +
		"\r\n" +
		"\r\n\r\n"

	u := NewUnmarshaler(WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail))
	data := bufio.NewReader(strings.NewReader(input))
	_, _, _, err := u.Unmarshal(data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported WARC version")
}

func Test_unmarshaler_Unmarshal_UnsupportedVersion_ErrWarn(t *testing.T) {
	input := "WARC/9.9\r\n" +
		"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
		"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
		"WARC-Type: warcinfo\r\n" +
		"Content-Type: application/warc-fields\r\n" +
		"Content-Length: 0\r\n" +
		"\r\n" +
		"\r\n\r\n"

	u := NewUnmarshaler(WithSpecViolationPolicy(ErrWarn), WithSyntaxErrorPolicy(ErrWarn),
		WithUnknownRecordTypePolicy(ErrIgnore))
	data := bufio.NewReader(strings.NewReader(input))
	rec, _, validation, err := u.Unmarshal(data)
	require.NoError(t, err)
	defer rec.Close()
	assert.False(t, len(validation) == 0)
	assert.Contains(t, fmt.Sprint(validation), "unsupported WARC version")
}

func Test_unmarshaler_Unmarshal_MissingCRLF_ErrFail(t *testing.T) {
	input := "WARC/1.0\n" +
		"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
		"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
		"WARC-Type: warcinfo\r\n" +
		"Content-Type: application/warc-fields\r\n" +
		"Content-Length: 0\r\n" +
		"\r\n" +
		"\r\n\r\n"

	u := NewUnmarshaler(WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail))
	data := bufio.NewReader(strings.NewReader(input))
	_, _, _, err := u.Unmarshal(data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing carriage return")
}

func Test_unmarshaler_Unmarshal_MissingCRLF_ErrWarn(t *testing.T) {
	input := "WARC/1.0\n" +
		"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
		"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
		"WARC-Type: warcinfo\r\n" +
		"Content-Type: application/warc-fields\r\n" +
		"Content-Length: 0\r\n" +
		"\r\n" +
		"\r\n\r\n"

	u := NewUnmarshaler(WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrWarn))
	data := bufio.NewReader(strings.NewReader(input))
	rec, _, validation, err := u.Unmarshal(data)
	require.NoError(t, err)
	defer rec.Close()
	assert.False(t, len(validation) == 0)
	assert.Contains(t, fmt.Sprint(validation), "missing carriage return")
}

func Test_unmarshaler_Unmarshal_GzipRecord(t *testing.T) {
	record := "WARC/1.0\r\n" +
		"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
		"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
		"WARC-Type: warcinfo\r\n" +
		"Content-Type: application/warc-fields\r\n" +
		"Content-Length: 0\r\n" +
		"\r\n" +
		"\r\n\r\n"

	buf := &bytes.Buffer{}
	z := gzip.NewWriter(buf)
	_, err := z.Write([]byte(record))
	require.NoError(t, err)
	require.NoError(t, z.Close())

	u := NewUnmarshaler(WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail))
	data := bufio.NewReader(bytes.NewReader(buf.Bytes()))
	rec, _, _, err := u.Unmarshal(data)
	require.NoError(t, err)
	defer rec.Close()
	assert.Equal(t, V1_0, rec.Version())
	assert.Equal(t, Warcinfo, rec.Type())
}

func Test_unmarshaler_Unmarshal_GzipRecord_Reuse(t *testing.T) {
	record := "WARC/1.0\r\n" +
		"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
		"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
		"WARC-Type: warcinfo\r\n" +
		"Content-Type: application/warc-fields\r\n" +
		"Content-Length: 0\r\n" +
		"\r\n" +
		"\r\n\r\n"

	// Create two gzip-compressed records
	buf := &bytes.Buffer{}
	z := gzip.NewWriter(buf)
	_, _ = z.Write([]byte(record))
	_ = z.Close()
	z2 := gzip.NewWriter(buf)
	_, _ = z2.Write([]byte(record))
	_ = z2.Close()

	u := NewUnmarshaler(WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail))
	data := bufio.NewReader(bytes.NewReader(buf.Bytes()))

	// First unmarshal creates gz reader
	rec1, _, _, err := u.Unmarshal(data)
	require.NoError(t, err)
	rec1.Close()

	// Second unmarshal reuses gz reader
	rec2, _, _, err := u.Unmarshal(data)
	require.NoError(t, err)
	rec2.Close()
}

func Test_unmarshaler_Unmarshal_ShortInput(t *testing.T) {
	u := NewUnmarshaler()
	data := bufio.NewReader(strings.NewReader("WA"))
	_, _, _, err := u.Unmarshal(data)
	require.Error(t, err)
}

func Test_unmarshaler_Unmarshal_MissingVersion(t *testing.T) {
	input := "NOT-WARC\r\n"
	u := NewUnmarshaler(WithSpecViolationPolicy(ErrIgnore), WithSyntaxErrorPolicy(ErrIgnore))
	// Pad enough bytes so Peek(5) works
	data := bufio.NewReader(strings.NewReader(input))
	_, _, _, err := u.Unmarshal(data)
	require.Error(t, err)
}

func Test_unmarshaler_Unmarshal_EndOfRecordMarker_Missing(t *testing.T) {
	input := "WARC/1.0\r\n" +
		"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
		"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
		"WARC-Type: warcinfo\r\n" +
		"Content-Type: application/warc-fields\r\n" +
		"Content-Length: 0\r\n" +
		"\r\n"
	// Missing \r\n\r\n at the end

	u := NewUnmarshaler(WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail))
	data := bufio.NewReader(strings.NewReader(input))
	_, _, _, err := u.Unmarshal(data)
	require.Error(t, err)
}

func Test_unmarshaler_Unmarshal_EndOfRecordMarker_LFOnly(t *testing.T) {
	input := "WARC/1.0\r\n" +
		"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
		"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
		"WARC-Type: warcinfo\r\n" +
		"Content-Type: application/warc-fields\r\n" +
		"Content-Length: 0\r\n" +
		"\r\n" +
		"\n"

	u := NewUnmarshaler(WithSpecViolationPolicy(ErrWarn), WithSyntaxErrorPolicy(ErrWarn))
	data := bufio.NewReader(strings.NewReader(input))
	rec, _, validation, err := u.Unmarshal(data)
	require.NoError(t, err)
	defer rec.Close()
	assert.False(t, len(validation) == 0)
	assert.Contains(t, fmt.Sprint(validation), "missing carriage return in end of record marker")
}

func Test_unmarshaler_Unmarshal_EndOfRecordMarker_LFLFOnly(t *testing.T) {
	input := "WARC/1.0\r\n" +
		"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
		"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
		"WARC-Type: warcinfo\r\n" +
		"Content-Type: application/warc-fields\r\n" +
		"Content-Length: 0\r\n" +
		"\r\n" +
		"\n\n"

	u := NewUnmarshaler(WithSpecViolationPolicy(ErrWarn), WithSyntaxErrorPolicy(ErrWarn))
	data := bufio.NewReader(strings.NewReader(input))
	rec, _, validation, err := u.Unmarshal(data)
	require.NoError(t, err)
	defer rec.Close()
	assert.False(t, len(validation) == 0)
	assert.Contains(t, fmt.Sprint(validation), "missing carriage return in end of record marker")
}

func Test_unmarshaler_Unmarshal_EndOfRecordMarker_TooFewBytes(t *testing.T) {
	input := "WARC/1.0\r\n" +
		"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
		"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
		"WARC-Type: warcinfo\r\n" +
		"Content-Type: application/warc-fields\r\n" +
		"Content-Length: 0\r\n" +
		"\r\n" +
		"\r\n"

	u := NewUnmarshaler(WithSpecViolationPolicy(ErrWarn), WithSyntaxErrorPolicy(ErrWarn))
	data := bufio.NewReader(strings.NewReader(input))
	rec, _, validation, err := u.Unmarshal(data)
	require.NoError(t, err)
	defer rec.Close()
	assert.False(t, len(validation) == 0)
	assert.Contains(t, fmt.Sprint(validation), "too few bytes in end of record marker")
}

func BenchmarkUnmarshaler_Unmarshal_compressed(b *testing.B) {
	record := "WARC/1.0\r\n" +
		"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
		"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
		"WARC-Type: response\r\n" +
		"Content-Type: application/http;msgtype=response\r\n" +
		"Warc-Block-Digest: sha1:B285747AD7CC57AA74BCE2E30B453C8D1CB71BA4\r\n" +
		"Content-Length: 257\r\n" +
		"\r\n" +
		"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n" +
		"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n" +
		"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the content"

	recordCompressed := &bytes.Buffer{}

	z := gzip.NewWriter(recordCompressed)
	_, _ = z.Write([]byte(record))
	_ = z.Close()

	u := NewUnmarshaler(WithNoValidation())

	for n := 0; n < b.N; n++ {
		data := bufio.NewReader(bytes.NewReader(recordCompressed.Bytes()))
		gotRecord, _, _, _ := u.Unmarshal(data)
		unmarshallerBenchmarkResult = gotRecord.Close()
	}
}

func Test_Unmarshal_CorruptGzip(t *testing.T) {
	// Feed valid gzip magic bytes but corrupt body so gzip.NewReader fails
	corruptGzip := []byte{0x1f, 0x8b, 0x08, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	u := NewUnmarshaler(WithSpecViolationPolicy(ErrIgnore), WithSyntaxErrorPolicy(ErrIgnore))
	_, _, _, err := u.Unmarshal(bufio.NewReader(bytes.NewReader(corruptGzip)))
	assert.Error(t, err)
}

func Test_Unmarshal_GzipNonWARCContent(t *testing.T) {
	// Gzip stream that decompresses to non-WARC content (not starting with "WARC/")
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, _ = gz.Write([]byte("NOT A WARC RECORD\r\n\r\n"))
	_ = gz.Close()

	u := NewUnmarshaler(WithSpecViolationPolicy(ErrIgnore), WithSyntaxErrorPolicy(ErrIgnore))
	_, _, _, err := u.Unmarshal(bufio.NewReader(&buf))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing record version")
}

func Test_Unmarshal_MissingCR_ErrFail(t *testing.T) {
	// Version line with LF only (no CR), and errSyntax=ErrFail should fail
	record := "WARC/1.1\n" + // LF only, no CR
		"WARC-Type: warcinfo\r\n" +
		"WARC-Date: 2006-09-19T17:29:44Z\r\n" +
		"WARC-Record-ID: <urn:uuid:d7ae5c10-e6b3-4d27-967d-34780c58ba39>\r\n" +
		"Content-Type: application/warc-fields\r\n" +
		"Content-Length: 0\r\n" +
		"\r\n" +
		"\r\n\r\n"

	u := NewUnmarshaler(WithSyntaxErrorPolicy(ErrFail))
	_, _, _, err := u.Unmarshal(bufio.NewReader(strings.NewReader(record)))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing carriage return")
}

func Test_Unmarshal_MissingCR_ErrWarn(t *testing.T) {
	// Version line with LF only (no CR), errSyntax=ErrWarn adds validation error
	record := "WARC/1.1\n" +
		"WARC-Type: warcinfo\r\n" +
		"WARC-Date: 2006-09-19T17:29:44Z\r\n" +
		"WARC-Record-ID: <urn:uuid:d7ae5c10-e6b3-4d27-967d-34780c58ba39>\r\n" +
		"Content-Type: application/warc-fields\r\n" +
		"Content-Length: 0\r\n" +
		"\r\n" +
		"\r\n\r\n"

	u := NewUnmarshaler(WithSyntaxErrorPolicy(ErrWarn), WithSpecViolationPolicy(ErrIgnore))
	rec, _, v, err := u.Unmarshal(bufio.NewReader(strings.NewReader(record)))
	require.NoError(t, err)
	defer rec.Close()
	assert.NotEmpty(t, v) // Should have a validation warning about missing CR
}

func Test_Unmarshal_EndOfRecordMarker_SingleLF(t *testing.T) {
	// End-of-record marker is a single LF instead of CRLFCRLF
	record := "WARC/1.1\r\n" +
		"WARC-Type: warcinfo\r\n" +
		"WARC-Date: 2006-09-19T17:29:44Z\r\n" +
		"WARC-Record-ID: <urn:uuid:d7ae5c10-e6b3-4d27-967d-34780c58ba39>\r\n" +
		"Content-Type: application/warc-fields\r\n" +
		"Content-Length: 0\r\n" +
		"\r\n" +
		"\n"

	u := NewUnmarshaler(WithSpecViolationPolicy(ErrWarn), WithSyntaxErrorPolicy(ErrIgnore))
	rec, _, v, err := u.Unmarshal(bufio.NewReader(strings.NewReader(record)))
	require.NoError(t, err)
	defer rec.Close()
	assert.NotEmpty(t, v) // validation should flag the bad marker
}

func Test_Unmarshal_EndOfRecordMarker_DoubleLF(t *testing.T) {
	// End-of-record marker is \n\n instead of \r\n\r\n
	record := "WARC/1.1\r\n" +
		"WARC-Type: warcinfo\r\n" +
		"WARC-Date: 2006-09-19T17:29:44Z\r\n" +
		"WARC-Record-ID: <urn:uuid:d7ae5c10-e6b3-4d27-967d-34780c58ba39>\r\n" +
		"Content-Type: application/warc-fields\r\n" +
		"Content-Length: 0\r\n" +
		"\r\n" +
		"\n\n"

	u := NewUnmarshaler(WithSpecViolationPolicy(ErrWarn), WithSyntaxErrorPolicy(ErrIgnore))
	rec, _, v, err := u.Unmarshal(bufio.NewReader(strings.NewReader(record)))
	require.NoError(t, err)
	defer rec.Close()
	assert.NotEmpty(t, v)
}

func Test_Unmarshal_EndOfRecordMarker_TooFew(t *testing.T) {
	// End-of-record has fewer than 4 bytes but more than 2 (e.g., 3 bytes like \r\n\r)
	record := "WARC/1.1\r\n" +
		"WARC-Type: warcinfo\r\n" +
		"WARC-Date: 2006-09-19T17:29:44Z\r\n" +
		"WARC-Record-ID: <urn:uuid:d7ae5c10-e6b3-4d27-967d-34780c58ba39>\r\n" +
		"Content-Type: application/warc-fields\r\n" +
		"Content-Length: 0\r\n" +
		"\r\n" +
		"\r\n\r" // only 3 bytes

	u := NewUnmarshaler(WithSpecViolationPolicy(ErrWarn), WithSyntaxErrorPolicy(ErrIgnore))
	rec, _, v, err := u.Unmarshal(bufio.NewReader(strings.NewReader(record)))
	require.NoError(t, err)
	defer rec.Close()
	assert.NotEmpty(t, v)
}

func Test_Unmarshal_EndOfRecordMarker_ErrFail(t *testing.T) {
	// Bad end-of-record marker with ErrFail should return error
	record := "WARC/1.1\r\n" +
		"WARC-Type: warcinfo\r\n" +
		"WARC-Date: 2006-09-19T17:29:44Z\r\n" +
		"WARC-Record-ID: <urn:uuid:d7ae5c10-e6b3-4d27-967d-34780c58ba39>\r\n" +
		"Content-Type: application/warc-fields\r\n" +
		"Content-Length: 0\r\n" +
		"\r\n" +
		"\n\n"

	u := NewUnmarshaler(WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrIgnore))
	_, _, _, err := u.Unmarshal(bufio.NewReader(strings.NewReader(record)))
	assert.Error(t, err)
}

func Test_Unmarshal_GzipReuseReset(t *testing.T) {
	// First unmarshal a valid gzip record, then a corrupt one. Tests gz.Reset() error path.
	record := "WARC/1.1\r\n" +
		"WARC-Type: warcinfo\r\n" +
		"WARC-Date: 2006-09-19T17:29:44Z\r\n" +
		"WARC-Record-ID: <urn:uuid:d7ae5c10-e6b3-4d27-967d-34780c58ba39>\r\n" +
		"Content-Type: application/warc-fields\r\n" +
		"Content-Length: 0\r\n" +
		"\r\n" +
		"\r\n\r\n"

	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	_, _ = gz.Write([]byte(record))
	_ = gz.Close()

	u := NewUnmarshaler(WithSpecViolationPolicy(ErrIgnore), WithSyntaxErrorPolicy(ErrIgnore))
	// First call initializes u.gz
	rec, _, _, err := u.Unmarshal(bufio.NewReader(bytes.NewReader(compressed.Bytes())))
	require.NoError(t, err)
	_ = rec.Close()

	// Second call with corrupt gzip triggers gz.Reset() error path
	corruptGzip := []byte{0x1f, 0x8b, 0x08, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	_, _, _, err = u.Unmarshal(bufio.NewReader(bytes.NewReader(corruptGzip)))
	assert.Error(t, err)
}

func Test_Unmarshal_ValidateDigestError(t *testing.T) {
	// Create a record with wrong block digest — ValidateDigest should produce error with ErrFail
	record := "WARC/1.1\r\n" +
		"WARC-Type: warcinfo\r\n" +
		"WARC-Date: 2006-09-19T17:29:44Z\r\n" +
		"WARC-Record-ID: <urn:uuid:d7ae5c10-e6b3-4d27-967d-34780c58ba39>\r\n" +
		"Content-Type: application/warc-fields\r\n" +
		"WARC-Block-Digest: sha1:0000000000000000000000000000000000000000\r\n" +
		"Content-Length: 0\r\n" +
		"\r\n" +
		"\r\n\r\n"

	u := NewUnmarshaler(WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrIgnore))
	_, _, _, err := u.Unmarshal(bufio.NewReader(strings.NewReader(record)))
	assert.Error(t, err)
}
