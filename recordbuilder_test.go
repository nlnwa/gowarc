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
	"io/ioutil"
	"testing"
)

func TestRecordBuilder(t *testing.T) {
	type args struct {
		opts       *options
		recordType *recordType
		headers    *warcFields
		data       string
	}
	type want struct {
		headers    *warcFields
		blockType  interface{}
		data       string
		validation *Validation
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
				NewOptions(WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail)),
				WARCINFO,
				&warcFields{
					&NameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&NameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&NameValue{Name: WarcFilename, Value: "temp-20170306040353.warc.gz"},
					&NameValue{Name: ContentType, Value: "application/warc-fields"},
					&NameValue{Name: WarcBlockDigest, Value: "sha1:AF4D582B4FFC017D07A947D841E392A821F754F3"},
					&NameValue{Name: ContentLength, Value: "238"},
				},
				"software: Veidemann v1.0\r\n" +
					"format: WARC File Format 1.1\r\n" +
					"creator: temp-MJFXHZ4S\r\n" +
					"isPartOf: Temporary%20Collection\r\n" +
					"json-metadata: {\"title\": \"Temporary Collection\", \"size\": 2865, \"created_at\": 1488772924, \"type\": \"collection\", \"desc\": \"\"}\r\n",
			},
			want{
				&warcFields{
					&NameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&NameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&NameValue{Name: WarcFilename, Value: "temp-20170306040353.warc.gz"},
					&NameValue{Name: WarcType, Value: "warcinfo"},
					&NameValue{Name: ContentType, Value: "application/warc-fields"},
					&NameValue{Name: WarcBlockDigest, Value: "sha1:AF4D582B4FFC017D07A947D841E392A821F754F3"},
					&NameValue{Name: ContentLength, Value: "238"},
				},
				&warcFieldsBlock{},
				"software: Veidemann v1.0\r\n" +
					"format: WARC File Format 1.1\r\n" +
					"creator: temp-MJFXHZ4S\r\n" +
					"isPartOf: Temporary%20Collection\r\n" +
					"json-metadata: {\"title\": \"Temporary Collection\", \"size\": 2865, \"created_at\": 1488772924, \"type\": \"collection\", \"desc\": \"\"}\r\n",
				&Validation{},
			},
			false,
		},
		{
			"valid response record",
			args{
				NewOptions(WithCompression(false)),
				RESPONSE,
				&warcFields{
					&NameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&NameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&NameValue{Name: ContentType, Value: "application/http;msgtype=response"},
					&NameValue{Name: WarcBlockDigest, Value: "sha1:B285747AD7CC57AA74BCE2E30B453C8D1CB71BA4"},
					&NameValue{Name: ContentLength, Value: "257"},
				},
				"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n" +
					"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n" +
					"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the content",
			},
			want{
				&warcFields{
					&NameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&NameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&NameValue{Name: WarcType, Value: "response"},
					&NameValue{Name: WarcBlockDigest, Value: "sha1:B285747AD7CC57AA74BCE2E30B453C8D1CB71BA4"},
					&NameValue{Name: ContentType, Value: "application/http;msgtype=response"},
					&NameValue{Name: ContentLength, Value: "257"},
				},
				&httpResponseBlock{},
				"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n" +
					"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n" +
					"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the content",
				&Validation{},
			},
			false,
		},
		{
			"valid request record",
			args{
				NewOptions(WithCompression(false)),
				REQUEST,
				&warcFields{
					&NameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&NameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&NameValue{Name: ContentType, Value: "application/http;msgtype=request"},
					&NameValue{Name: WarcBlockDigest, Value: "sha1:F45C9D37F9F7E5F822C86444F51D6CB252B7B33B"},
					&NameValue{Name: ContentLength, Value: "262"},
				},
				"GET / HTTP/1.0\n" +
					"Host: example.com\n" +
					"Accept-Language: en-US,en;q=0.8,ru;q=0.6\n" +
					"Referer: http://example.com/foo.html\n" +
					"Connection: close\n" +
					"User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36\n",
			},
			want{
				&warcFields{
					&NameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&NameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&NameValue{Name: WarcType, Value: "request"},
					&NameValue{Name: ContentType, Value: "application/http;msgtype=request"},
					&NameValue{Name: WarcBlockDigest, Value: "sha1:F45C9D37F9F7E5F822C86444F51D6CB252B7B33B"},
					&NameValue{Name: ContentLength, Value: "262"},
				},
				&httpRequestBlock{},
				"GET / HTTP/1.0\n" +
					"Host: example.com\n" +
					"Accept-Language: en-US,en;q=0.8,ru;q=0.6\n" +
					"Referer: http://example.com/foo.html\n" +
					"Connection: close\n" +
					"User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36\n",
				&Validation{},
			},
			false,
		},
		{
			"valid metadata record",
			args{
				NewOptions(WithCompression(false)),
				METADATA,
				&warcFields{
					&NameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&NameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&NameValue{Name: ContentType, Value: "text/anvl"},
					&NameValue{Name: WarcConcurrentTo, Value: "<urn:uuid:e7c9eff8-f5bc-4aeb-b3d2-9d3df99afb30>"},
					&NameValue{Name: WarcBlockDigest, Value: "sha1:857C4C4B401FFBAB3D2EBA6BE566F849C87915F7"},
					&NameValue{Name: ContentLength, Value: "61"},
				},
				"via: http://www.example.com/\n" +
					"hopsFromSeed: P\n" +
					"fetchTimeMs: 47\n",
			},
			want{
				&warcFields{
					&NameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&NameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&NameValue{Name: WarcType, Value: "metadata"},
					&NameValue{Name: ContentType, Value: "text/anvl"},
					&NameValue{Name: WarcConcurrentTo, Value: "<urn:uuid:e7c9eff8-f5bc-4aeb-b3d2-9d3df99afb30>"},
					&NameValue{Name: WarcBlockDigest, Value: "sha1:857C4C4B401FFBAB3D2EBA6BE566F849C87915F7"},
					&NameValue{Name: ContentLength, Value: "61"},
				},
				&genericBlock{},
				"via: http://www.example.com/\n" +
					"hopsFromSeed: P\n" +
					"fetchTimeMs: 47\n",
				&Validation{},
			},
			false,
		},
		{
			"valid resource record",
			args{
				NewOptions(WithCompression(false)),
				RESOURCE,
				&warcFields{
					&NameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&NameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&NameValue{Name: WarcTargetURI, Value: "file://var/www/htdoc/index.html"},
					&NameValue{Name: ContentType, Value: "text/html"},
					&NameValue{Name: WarcConcurrentTo, Value: "<urn:uuid:e7c9eff8-f5bc-4aeb-b3d2-9d3df99afb30>"},
					&NameValue{Name: WarcPayloadDigest, Value: "sha1:307E7DFCAF9A8EA4C4E86A11BCAA83AC6698017F"},
					&NameValue{Name: WarcBlockDigest, Value: "sha1:307E7DFCAF9A8EA4C4E86A11BCAA83AC6698017F"},
					&NameValue{Name: ContentLength, Value: "42"},
				},
				"<html><head></head>\n" +
					"<body></body>\n" +
					"</html>\n",
			},
			want{
				&warcFields{
					&NameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&NameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&NameValue{Name: WarcType, Value: "resource"},
					&NameValue{Name: ContentType, Value: "text/html"},
					&NameValue{Name: WarcTargetURI, Value: "file://var/www/htdoc/index.html"},
					&NameValue{Name: WarcConcurrentTo, Value: "<urn:uuid:e7c9eff8-f5bc-4aeb-b3d2-9d3df99afb30>"},
					&NameValue{Name: WarcPayloadDigest, Value: "sha1:307E7DFCAF9A8EA4C4E86A11BCAA83AC6698017F"},
					&NameValue{Name: WarcBlockDigest, Value: "sha1:307E7DFCAF9A8EA4C4E86A11BCAA83AC6698017F"},
					&NameValue{Name: ContentLength, Value: "42"},
				},
				&genericBlock{},
				"<html><head></head>\n" +
					"<body></body>\n" +
					"</html>\n",
				&Validation{},
			},
			false,
		},
		{
			"valid revisit record",
			args{
				NewOptions(WithCompression(false)),
				REVISIT,
				&warcFields{
					&NameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&NameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&NameValue{Name: WarcTargetURI, Value: "http://www.example.org/images/logo.jpg"},
					&NameValue{Name: WarcProfile, Value: "http://netpreserve.org/warc/1.1/server-not-modified"},
					&NameValue{Name: WarcRefersTo, Value: "<urn:uuid:92283950-ef2f-4d72-b224-f54c6ec90bb0>"},
					&NameValue{Name: WarcRefersToTargetURI, Value: "http://www.example.org/images/logo.jpg"},
					&NameValue{Name: WarcRefersToDate, Value: "2016-09-19T17:20:24Z"},
					&NameValue{Name: ContentType, Value: "message/http"},
					&NameValue{Name: WarcBlockDigest, Value: "sha1:7B71E2CE461E4685EED55612850EE0CBB3876EDF"},
					&NameValue{Name: ContentLength, Value: "195"},
				},
				"HTTP/1.x 304 Not Modified\n" +
					"Date: Tue, 06 Mar 2017 00:43:35 GMT\n" +
					"Server: Apache/2.0.54 (Ubuntu) PHP/5.0.5-2ubuntu1.4 Connection: Keep-Alive\n" +
					"Keep-Alive: timeout=15, max=100\n" +
					"ETag: \"3e45-67e-2ed02ec0\"\n",
			},
			want{
				&warcFields{
					&NameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&NameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&NameValue{Name: WarcType, Value: "revisit"},
					&NameValue{Name: WarcTargetURI, Value: "http://www.example.org/images/logo.jpg"},
					&NameValue{Name: WarcProfile, Value: "http://netpreserve.org/warc/1.1/server-not-modified"},
					&NameValue{Name: WarcRefersTo, Value: "<urn:uuid:92283950-ef2f-4d72-b224-f54c6ec90bb0>"},
					&NameValue{Name: WarcRefersToTargetURI, Value: "http://www.example.org/images/logo.jpg"},
					&NameValue{Name: WarcRefersToDate, Value: "2016-09-19T17:20:24Z"},
					&NameValue{Name: ContentType, Value: "message/http"},
					&NameValue{Name: WarcBlockDigest, Value: "sha1:7B71E2CE461E4685EED55612850EE0CBB3876EDF"},
					&NameValue{Name: ContentLength, Value: "195"},
				},
				&genericBlock{},
				"HTTP/1.x 304 Not Modified\n" +
					"Date: Tue, 06 Mar 2017 00:43:35 GMT\n" +
					"Server: Apache/2.0.54 (Ubuntu) PHP/5.0.5-2ubuntu1.4 Connection: Keep-Alive\n" +
					"Keep-Alive: timeout=15, max=100\n" +
					"ETag: \"3e45-67e-2ed02ec0\"\n",
				&Validation{},
			},
			false,
		},
		{
			"valid conversion record",
			args{
				NewOptions(WithCompression(false)),
				CONVERSION,
				&warcFields{
					&NameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&NameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&NameValue{Name: WarcTargetURI, Value: "http://www.example.org/index.html"},
					&NameValue{Name: WarcRefersTo, Value: "<urn:uuid:92283950-ef2f-4d72-b224-f54c6ec90bb0>"},
					&NameValue{Name: ContentType, Value: "text/plain"},
					&NameValue{Name: WarcBlockDigest, Value: "sha1:581F7F1CA3D3EB023438808309678ED6D03E2895"},
					&NameValue{Name: ContentLength, Value: "10"},
				},
				"body text\n",
			},
			want{
				&warcFields{
					&NameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&NameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&NameValue{Name: WarcTargetURI, Value: "http://www.example.org/index.html"},
					&NameValue{Name: WarcRefersTo, Value: "<urn:uuid:92283950-ef2f-4d72-b224-f54c6ec90bb0>"},
					&NameValue{Name: WarcType, Value: "conversion"},
					&NameValue{Name: ContentType, Value: "text/plain"},
					&NameValue{Name: WarcBlockDigest, Value: "sha1:581F7F1CA3D3EB023438808309678ED6D03E2895"},
					&NameValue{Name: ContentLength, Value: "10"},
				},
				&genericBlock{},
				"body text\n",
				&Validation{},
			},
			false,
		},
		{
			"valid continuation record",
			args{
				NewOptions(WithCompression(false)),
				CONTINUATION,
				&warcFields{
					&NameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&NameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&NameValue{Name: WarcTargetURI, Value: "http://www.example.org/index.html"},
					&NameValue{Name: WarcSegmentOriginID, Value: "<urn:uuid:39509228-ae2f-11b2-763a-aa4c6ec90bb0>"},
					&NameValue{Name: WarcSegmentNumber, Value: "2"},
					&NameValue{Name: WarcSegmentTotalLength, Value: "1902"},
					&NameValue{Name: ContentType, Value: "text/plain"},
					&NameValue{Name: WarcBlockDigest, Value: "sha1:62B805E388394FF22747D1B10476EA04309CB5A8"},
					&NameValue{Name: WarcPayloadDigest, Value: "sha1:CCHXETFVJD2MUZY6ND6SS7ZENMWF7KQ2"},
					&NameValue{Name: ContentLength, Value: "22"},
				},
				"... last part of data\n",
			},
			want{
				&warcFields{
					&NameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&NameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&NameValue{Name: WarcTargetURI, Value: "http://www.example.org/index.html"},
					&NameValue{Name: WarcType, Value: "continuation"},
					&NameValue{Name: WarcSegmentOriginID, Value: "<urn:uuid:39509228-ae2f-11b2-763a-aa4c6ec90bb0>"},
					&NameValue{Name: WarcSegmentNumber, Value: "2"},
					&NameValue{Name: WarcSegmentTotalLength, Value: "1902"},
					&NameValue{Name: ContentType, Value: "text/plain"},
					&NameValue{Name: WarcBlockDigest, Value: "sha1:62B805E388394FF22747D1B10476EA04309CB5A8"},
					&NameValue{Name: WarcPayloadDigest, Value: "sha1:CCHXETFVJD2MUZY6ND6SS7ZENMWF7KQ2"},
					&NameValue{Name: ContentLength, Value: "22"},
				},
				&genericBlock{},
				"... last part of data\n",
				&Validation{},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rb := NewRecordBuilder(tt.args.opts, tt.args.recordType)
			for _, nv := range *tt.args.headers {
				rb.AddWarcHeader(nv.Name, nv.Value)
			}
			rb.WriteString(tt.args.data)
			wr, validation, err := rb.Finalize()
			if err == nil {
				defer wr.Close()
			}

			assert := assert.New(t)
			if tt.wantErr {
				assert.Error(err)
			} else {
				assert.NoError(err)
			}

			assert.ElementsMatch([]*NameValue(*tt.want.headers), []*NameValue(*wr.WarcHeader()))
			assert.IsType(tt.want.blockType, wr.Block())
			assert.Equal(tt.want.validation, validation)
			r, err := wr.Block().RawBytes()
			assert.Nil(err)
			b, err := ioutil.ReadAll(r)
			assert.Nil(err)
			assert.Equal(tt.want.data, string(b))

			//if !reflect.DeepEqual(got, tt.want) {
			//	t.Errorf("NewResponseRecord() got = %v, want %v", got, tt.want)
			//}

			//w := NewWriter(tt.args.opts)
			//fmt.Printf(">>>>>>>>>>>>>>>>>>>>>\n")
			//n, err := w.WriteRecord(os.Stdout, wr)
			//fmt.Printf("<<<<<<<<<<<<<<<<<<<<<\n")
			//fmt.Printf("Bytes written: %v, BlockType %T, Err: %v\n", n, wr.Block(), err)
			//
			//resp := wr.Block().(HttpResponseBlock).HttpHeader()
			//fmt.Printf("Http header: %v, Err: %v\n", resp, err)
		})
	}
}
