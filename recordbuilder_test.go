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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecordBuilder(t *testing.T) {
	type args struct {
		opts       []WarcRecordOption
		recordType RecordType
		headers    *WarcFields
		data       string
	}
	type want struct {
		headers    *WarcFields
		blockType  any
		data       string
		validation *Validation
		cached     bool
	}
	tests := []struct {
		name    string
		args    args
		want    want
		wantErr bool
	}{
		{
			"valid warcinfo record",
			args{
				[]WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail), WithUnknownRecordTypePolicy(ErrIgnore)},
				Warcinfo,
				&WarcFields{
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcFilename, Value: "temp-20170306040353.warc.gz"},
					&nameValue{Name: ContentType, Value: "application/warc-fields"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:AF4D582B4FFC017D07A947D841E392A821F754F3"},
					&nameValue{Name: ContentLength, Value: "238"},
				},
				"software: Veidemann v1.0\r\n" +
					"format: WARC File Format 1.1\r\n" +
					"creator: temp-MJFXHZ4S\r\n" +
					"isPartOf: Temporary%20Collection\r\n" +
					"json-metadata: {\"title\": \"Temporary Collection\", \"size\": 2865, \"created_at\": 1488772924, \"type\": \"collection\", \"desc\": \"\"}\r\n",
			},
			want{
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcFilename, Value: "temp-20170306040353.warc.gz"},
					&nameValue{Name: WarcType, Value: "warcinfo"},
					&nameValue{Name: ContentType, Value: "application/warc-fields"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:AF4D582B4FFC017D07A947D841E392A821F754F3"},
					&nameValue{Name: ContentLength, Value: "238"},
				},
				&warcFieldsBlock{},
				"software: Veidemann v1.0\r\n" +
					"format: WARC File Format 1.1\r\n" +
					"creator: temp-MJFXHZ4S\r\n" +
					"isPartOf: Temporary%20Collection\r\n" +
					"json-metadata: {\"title\": \"Temporary Collection\", \"size\": 2865, \"created_at\": 1488772924, \"type\": \"collection\", \"desc\": \"\"}\r\n",
				&Validation{},
				true,
			},
			false,
		},
		{
			"valid response record",
			args{
				[]WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail), WithUnknownRecordTypePolicy(ErrIgnore)},
				Response,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:71ABF824132CA55D4D46D32E73B4D33E2354E619"},
					&nameValue{Name: WarcPayloadDigest, Value: "sha1:C37FFB221569C553A2476C22C7DAD429F3492977"},
					&nameValue{Name: ContentLength, Value: "259"},
				},
				"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n" +
					"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n" +
					"Content-Length: 19\nConnection: close\nContent-Type: text/plain\r\n\r\nThis is the content",
			},
			want{
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcType, Value: "response"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:71ABF824132CA55D4D46D32E73B4D33E2354E619"},
					&nameValue{Name: WarcPayloadDigest, Value: "sha1:C37FFB221569C553A2476C22C7DAD429F3492977"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
					&nameValue{Name: ContentLength, Value: "259"},
				},
				&httpResponseBlock{},
				"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n" +
					"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n" +
					"Content-Length: 19\nConnection: close\nContent-Type: text/plain\r\n\r\nThis is the content",
				&Validation{},
				true,
			},
			false,
		},
		{
			"valid request record",
			args{
				[]WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail), WithUnknownRecordTypePolicy(ErrIgnore), WithFixDigest(false), WithAddMissingDigest(false)},
				Request,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=request"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:A3781FF1FC3FB52318F623E22C85D63D74C12932"},
					&nameValue{Name: ContentLength, Value: "263"},
				},
				"GET / HTTP/1.0\n" +
					"Host: example.com\n" +
					"Accept-Language: en-US,en;q=0.8,ru;q=0.6\n" +
					"Referer: http://example.com/foo.html\n" +
					"Connection: close\n" +
					"User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36\n\n",
			},
			want{
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
				&Validation{},
				true,
			},
			false,
		},
		{
			"invalid request record - missing newline - warn",
			args{
				[]WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrWarn), WithUnknownRecordTypePolicy(ErrIgnore), WithFixDigest(false), WithAddMissingDigest(false), WithFixSyntaxErrors(false)},
				Request,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=request"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:F45C9D37F9F7E5F822C86444F51D6CB252B7B33B"},
					&nameValue{Name: ContentLength, Value: "262"},
				},
				"GET / HTTP/1.0\n" +
					"Host: example.com\n" +
					"Accept-Language: en-US,en;q=0.8,ru;q=0.6\n" +
					"Referer: http://example.com/foo.html\n" +
					"Connection: close\n" +
					"User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36\n",
			},
			want{
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
				&Validation{errMissingEndOfHeaders},
				true,
			},
			false,
		},
		{
			"invalid request record - missing newline - fail",
			args{
				[]WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail), WithUnknownRecordTypePolicy(ErrIgnore), WithFixDigest(false), WithAddMissingDigest(false)},
				Request,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=request"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:F45C9D37F9F7E5F822C86444F51D6CB252B7B33B"},
					&nameValue{Name: ContentLength, Value: "262"},
				},
				"GET / HTTP/1.0\n" +
					"Host: example.com\n" +
					"Accept-Language: en-US,en;q=0.8,ru;q=0.6\n" +
					"Referer: http://example.com/foo.html\n" +
					"Connection: close\n" +
					"User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36\n",
			},
			want{
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
				true,
			},
			true,
		},
		{
			"valid metadata record",
			args{
				[]WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail), WithUnknownRecordTypePolicy(ErrIgnore)},
				Metadata,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: ContentType, Value: "application/warc-fields"},
					&nameValue{Name: WarcConcurrentTo, Value: "<urn:uuid:e7c9eff8-f5bc-4aeb-b3d2-9d3df99afb30>"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:6D924D4C99268BE486042E655B06A83133EFEB59"},
					&nameValue{Name: ContentLength, Value: "64"},
				},
				"via: http://www.example.com/\r\n" +
					"hopsFromSeed: P\r\n" +
					"fetchTimeMs: 47\r\n",
			},
			want{
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
			false,
		},
		{
			"valid resource record",
			args{
				[]WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail), WithUnknownRecordTypePolicy(ErrIgnore)},
				Resource,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcTargetURI, Value: "file://var/www/htdoc/index.html"},
					&nameValue{Name: ContentType, Value: "text/html"},
					&nameValue{Name: WarcConcurrentTo, Value: "<urn:uuid:e7c9eff8-f5bc-4aeb-b3d2-9d3df99afb30>"},
					&nameValue{Name: WarcPayloadDigest, Value: "sha1:307E7DFCAF9A8EA4C4E86A11BCAA83AC6698017F"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:307E7DFCAF9A8EA4C4E86A11BCAA83AC6698017F"},
					&nameValue{Name: ContentLength, Value: "42"},
				},
				"<html><head></head>\n" +
					"<body></body>\n" +
					"</html>\n",
			},
			want{
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
				true,
			},
			false,
		},
		{
			"valid revisit record",
			args{
				[]WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail), WithUnknownRecordTypePolicy(ErrIgnore)},
				Revisit,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcTargetURI, Value: "http://www.example.org/images/logo.jpg"},
					&nameValue{Name: WarcProfile, Value: "http://netpreserve.org/warc/1.1/server-not-modified"},
					&nameValue{Name: WarcRefersTo, Value: "<urn:uuid:92283950-ef2f-4d72-b224-f54c6ec90bb0>"},
					&nameValue{Name: WarcRefersToTargetURI, Value: "http://www.example.org/images/logo.jpg"},
					&nameValue{Name: WarcRefersToDate, Value: "2016-09-19T17:20:24Z"},
					&nameValue{Name: ContentType, Value: "message/http"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:7B71E2CE461E4685EED55612850EE0CBB3876EDF"},
					&nameValue{Name: ContentLength, Value: "195"},
				},
				"HTTP/1.x 304 Not Modified\n" +
					"Date: Tue, 06 Mar 2017 00:43:35 GMT\n" +
					"Server: Apache/2.0.54 (Ubuntu) PHP/5.0.5-2ubuntu1.4 Connection: Keep-Alive\n" +
					"Keep-Alive: timeout=15, max=100\n" +
					"ETag: \"3e45-67e-2ed02ec0\"\n",
			},
			want{
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
				&Validation{},
				true,
			},
			false,
		},
		{
			"valid conversion record",
			args{
				[]WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail), WithUnknownRecordTypePolicy(ErrIgnore)},
				Conversion,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcTargetURI, Value: "http://www.example.org/index.html"},
					&nameValue{Name: WarcRefersTo, Value: "<urn:uuid:92283950-ef2f-4d72-b224-f54c6ec90bb0>"},
					&nameValue{Name: ContentType, Value: "text/plain"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:581F7F1CA3D3EB023438808309678ED6D03E2895"},
					&nameValue{Name: ContentLength, Value: "10"},
				},
				"body text\n",
			},
			want{
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
				true,
			},
			false,
		},
		{
			"valid continuation record",
			args{
				[]WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail), WithUnknownRecordTypePolicy(ErrIgnore)},
				Continuation,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcTargetURI, Value: "http://www.example.org/index.html"},
					&nameValue{Name: WarcSegmentOriginID, Value: "<urn:uuid:39509228-ae2f-11b2-763a-aa4c6ec90bb0>"},
					&nameValue{Name: WarcSegmentNumber, Value: "2"},
					&nameValue{Name: WarcSegmentTotalLength, Value: "1902"},
					&nameValue{Name: ContentType, Value: "text/plain"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:62B805E388394FF22747D1B10476EA04309CB5A8"},
					&nameValue{Name: WarcPayloadDigest, Value: "sha1:CCHXETFVJD2MUZY6ND6SS7ZENMWF7KQ2"},
					&nameValue{Name: ContentLength, Value: "22"},
				},
				"... last part of data\n",
			},
			want{
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcTargetURI, Value: "http://www.example.org/index.html"},
					&nameValue{Name: WarcType, Value: "continuation"},
					&nameValue{Name: WarcSegmentOriginID, Value: "<urn:uuid:39509228-ae2f-11b2-763a-aa4c6ec90bb0>"},
					&nameValue{Name: WarcSegmentNumber, Value: "2"},
					&nameValue{Name: WarcSegmentTotalLength, Value: "1902"},
					&nameValue{Name: ContentType, Value: "text/plain"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:62B805E388394FF22747D1B10476EA04309CB5A8"},
					&nameValue{Name: WarcPayloadDigest, Value: "sha1:CCHXETFVJD2MUZY6ND6SS7ZENMWF7KQ2"},
					&nameValue{Name: ContentLength, Value: "22"},
				},
				&genericBlock{},
				"... last part of data\n",
				&Validation{},
				true,
			},
			false,
		},
		{
			"valid unknown record type",
			args{
				[]WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail), WithUnknownRecordTypePolicy(ErrIgnore)},
				0,
				&WarcFields{
					&nameValue{Name: WarcType, Value: "myType"},
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcTargetURI, Value: "http://www.example.org/index.html"},
					&nameValue{Name: "My-Field", Value: "MyValue"},
					&nameValue{Name: ContentType, Value: "text/plain"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:7FE70820E08A1AAC0EF224D9C66AB66831CC4AB1"},
					&nameValue{Name: ContentLength, Value: "8"},
				},
				"content\n",
			},
			want{
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
				true,
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)

			rb := NewRecordBuilder(tt.args.recordType, tt.args.opts...)
			for _, nv := range *tt.args.headers {
				rb.AddWarcHeader(nv.Name, nv.Value)
			}
			_, err := rb.WriteString(tt.args.data)
			assert.NoError(err)
			wr, validation, err := rb.Build()
			if err == nil {
				defer wr.Close() //nolint
			}

			if tt.wantErr {
				assert.Error(err)
				return
			} else {
				assert.NoError(err)
			}

			assert.ElementsMatch([]*nameValue(*tt.want.headers), []*nameValue(*wr.WarcHeader()))
			assert.IsType(tt.want.blockType, wr.Block())
			assert.Equal(tt.want.validation, validation)
			r, err := wr.Block().RawBytes()
			assert.Nil(err)
			b, err := io.ReadAll(r)
			assert.Nil(err)
			assert.Equal(tt.want.data, string(b))

			assert.Equal(tt.want.cached, wr.Block().IsCached())
		})
	}
}

func TestRecordBuilder_AddWarcHeader(t *testing.T) {
	loc, err := time.LoadLocation("Europe/Oslo")
	assert.NoError(t, err)
	tm, err := time.ParseInLocation(time.ANSIC, "Mon Mar  6 05:03:53 2017", loc)
	assert.NoError(t, err)
	rb := NewRecordBuilder(Warcinfo)
	rb.AddWarcHeader(WarcRecordID, "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>")
	rb.AddWarcHeaderTime(WarcDate, tm)
	rb.AddWarcHeaderInt64(ContentLength, int64(238))
	rb.AddWarcHeaderInt(WarcSegmentNumber, 2)
	record, validation, err := rb.Build()
	assert.NoError(t, err)
	defer record.Close() //nolint

	rb = NewRecordBuilder(Warcinfo)
	rb.AddWarcHeader(WarcRecordID, "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>")
	rb.AddWarcHeader(WarcDate, "2017-03-06T04:03:53Z")
	rb.AddWarcHeader(ContentLength, "238")
	rb.AddWarcHeader(WarcSegmentNumber, "2")
	expected, expectedValidation, err := rb.Build()
	assert.NoError(t, err)
	defer record.Close() //nolint

	assert.ElementsMatch(t, []*nameValue(*expected.WarcHeader()), []*nameValue(*record.WarcHeader()))
	assert.Equal(t, expectedValidation, validation)
}

func TestRecordBuilder_Write(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantLen int
	}{
		{"short content", []byte("Hello"), 5},
		{"empty content", []byte{}, 0},
		{"binary content", []byte{0x00, 0x01, 0x02}, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rb := NewRecordBuilder(Warcinfo, WithSpecViolationPolicy(ErrIgnore), WithSyntaxErrorPolicy(ErrIgnore))
			rb.AddWarcHeader(WarcRecordID, "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>")
			rb.AddWarcHeader(WarcDate, "2024-01-01T00:00:00Z")
			rb.AddWarcHeader(ContentType, ApplicationWarcFields)

			n, err := rb.Write(tt.data)
			assert.NoError(t, err)
			assert.Equal(t, tt.wantLen, n)

			record, _, err := rb.Build()
			assert.NoError(t, err)
			defer record.Close()

			r, err := record.Block().RawBytes()
			assert.NoError(t, err)
			content, _ := io.ReadAll(r)
			assert.Equal(t, tt.data, content)
		})
	}
}

func TestRecordBuilder_ReadFrom(t *testing.T) {
	tests := []struct {
		name    string
		data    string
		wantLen int64
	}{
		{"short string", "Hello World", 11},
		{"empty string", "", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rb := NewRecordBuilder(Warcinfo, WithSpecViolationPolicy(ErrIgnore), WithSyntaxErrorPolicy(ErrIgnore))
			rb.AddWarcHeader(WarcRecordID, "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>")
			rb.AddWarcHeader(WarcDate, "2024-01-01T00:00:00Z")
			rb.AddWarcHeader(ContentType, ApplicationWarcFields)

			n, err := rb.ReadFrom(strings.NewReader(tt.data))
			assert.NoError(t, err)
			assert.Equal(t, tt.wantLen, n)

			record, _, err := rb.Build()
			assert.NoError(t, err)
			defer record.Close()

			r, err := record.Block().RawBytes()
			assert.NoError(t, err)
			content, _ := io.ReadAll(r)
			assert.Equal(t, tt.data, string(content))
		})
	}
}

func TestRecordBuilder_Size(t *testing.T) {
	tests := []struct {
		name     string
		data     string
		wantSize int64
	}{
		{"empty", "", 0},
		{"five bytes", "Hello", 5},
		{"longer content", "Hello World!!", 13},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rb := NewRecordBuilder(Warcinfo, WithSpecViolationPolicy(ErrIgnore), WithSyntaxErrorPolicy(ErrIgnore))
			rb.AddWarcHeader(WarcRecordID, "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>")
			rb.AddWarcHeader(WarcDate, "2024-01-01T00:00:00Z")
			rb.AddWarcHeader(ContentType, ApplicationWarcFields)

			assert.Equal(t, int64(0), rb.Size())

			_, _ = rb.WriteString(tt.data)
			assert.Equal(t, tt.wantSize, rb.Size())

			record, _, err := rb.Build()
			assert.NoError(t, err)
			defer record.Close()
		})
	}
}

func TestRecordBuilder_SetRecordType(t *testing.T) {
	tests := []struct {
		name       string
		initial    RecordType
		override   RecordType
		wantHeader string
	}{
		{"warcinfo to metadata", Warcinfo, Metadata, "metadata"},
		{"resource to response", Resource, Response, "response"},
		{"response to request", Response, Request, "request"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rb := NewRecordBuilder(tt.initial, WithSpecViolationPolicy(ErrIgnore), WithSyntaxErrorPolicy(ErrIgnore))
			rb.AddWarcHeader(WarcRecordID, "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>")
			rb.AddWarcHeader(WarcDate, "2024-01-01T00:00:00Z")
			rb.AddWarcHeader(ContentType, ApplicationWarcFields)

			rb.SetRecordType(tt.override)

			record, _, err := rb.Build()
			assert.NoError(t, err)
			defer record.Close()

			assert.Equal(t, tt.override, record.Type())
			assert.Equal(t, tt.wantHeader, record.WarcHeader().Get(WarcType))
		})
	}
}

func TestRecordBuilder_Build_FailedRecordIdFunc(t *testing.T) {
	rb := NewRecordBuilder(Warcinfo,
		WithSpecViolationPolicy(ErrIgnore), WithSyntaxErrorPolicy(ErrIgnore),
		WithAddMissingRecordId(true),
		WithRecordIdFunc(func() (string, error) {
			return "", fmt.Errorf("id gen failed")
		}),
	)
	rb.AddWarcHeader(WarcDate, "2024-01-01T00:00:00Z")
	rb.AddWarcHeader(ContentType, ApplicationWarcFields)
	rb.AddWarcHeader(ContentLength, "0")

	_, _, err := rb.Build()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "id gen failed")
}

func TestRecordBuilder_Build_ValidationFailure(t *testing.T) {
	// Missing required WARC-Type when recordType is 0
	rb := NewRecordBuilder(0,
		WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail),
	)
	rb.AddWarcHeader(WarcRecordID, "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>")
	rb.AddWarcHeader(WarcDate, "2024-01-01T00:00:00Z")
	rb.AddWarcHeader(ContentLength, "0")

	_, validation, err := rb.Build()
	require.Error(t, err)
	assert.NotNil(t, validation)
}

func TestRecordBuilder_AddWarcHeaderTime_V1_0(t *testing.T) {
	rb := NewRecordBuilder(Warcinfo,
		WithVersion(V1_0),
		WithSpecViolationPolicy(ErrIgnore), WithSyntaxErrorPolicy(ErrIgnore),
	)
	now := time.Date(2024, 1, 1, 12, 30, 45, 123456789, time.UTC)
	rb.AddWarcHeaderTime(WarcDate, now)
	rb.AddWarcHeader(WarcRecordID, "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>")
	rb.AddWarcHeader(ContentType, ApplicationWarcFields)
	rb.AddWarcHeader(ContentLength, "0")

	rec, _, err := rb.Build()
	require.NoError(t, err)
	defer rec.Close()

	// V1_0 should use RFC3339 (no nanoseconds)
	assert.Equal(t, "2024-01-01T12:30:45Z", rec.WarcHeader().Get(WarcDate))
}

func TestRecordBuilder_AddWarcHeaderTime_V1_1(t *testing.T) {
	rb := NewRecordBuilder(Warcinfo,
		WithVersion(V1_1),
		WithSpecViolationPolicy(ErrIgnore), WithSyntaxErrorPolicy(ErrIgnore),
	)
	now := time.Date(2024, 1, 1, 12, 30, 45, 123456789, time.UTC)
	rb.AddWarcHeaderTime(WarcDate, now)
	rb.AddWarcHeader(WarcRecordID, "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>")
	rb.AddWarcHeader(ContentType, ApplicationWarcFields)
	rb.AddWarcHeader(ContentLength, "0")

	rec, _, err := rb.Build()
	require.NoError(t, err)
	defer rec.Close()

	// V1_1 should use RFC3339Nano
	assert.Equal(t, "2024-01-01T12:30:45.123456789Z", rec.WarcHeader().Get(WarcDate))
}

func TestRecordBuilder_NoRecordType(t *testing.T) {
	rb := NewRecordBuilder(0,
		WithSpecViolationPolicy(ErrIgnore), WithSyntaxErrorPolicy(ErrIgnore),
		WithUnknownRecordTypePolicy(ErrIgnore),
	)
	rb.AddWarcHeader(WarcRecordID, "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>")
	rb.AddWarcHeader(WarcDate, "2024-01-01T00:00:00Z")
	rb.AddWarcHeader(ContentLength, "0")

	rec, _, err := rb.Build()
	require.NoError(t, err)
	defer rec.Close()
	assert.Equal(t, RecordType(0), rec.Type())
}

func TestRecordBuilder_Close(t *testing.T) {
	rb := NewRecordBuilder(Warcinfo)
	_, err := rb.WriteString("test")
	require.NoError(t, err)
	assert.Equal(t, int64(4), rb.Size())
	require.NoError(t, rb.Close())
}

func TestRecordBuilder_Build_RecordIdFuncError(t *testing.T) {
	// Use a custom recordIdFunc that returns an error
	rb := NewRecordBuilder(Warcinfo,
		WithRecordIdFunc(func() (string, error) {
			return "", fmt.Errorf("id generation failed")
		}),
		WithAddMissingRecordId(true),
	)
	_, err := rb.WriteString("test")
	require.NoError(t, err)
	rb.AddWarcHeader(WarcDate, "2024-01-01T00:00:00Z")
	rb.AddWarcHeader(ContentType, "application/warc-fields")
	rb.AddWarcHeader(ContentLength, "4")

	// Build should fail because recordIdFunc errors
	_, _, err = rb.Build()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "id generation failed")
}
