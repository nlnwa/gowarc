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
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_warcRecord_ToRevisitRecord(t *testing.T) {
	type want struct {
		headers *WarcFields
		data    string
	}
	tests := []struct {
		name    string
		record  WarcRecord
		ref     *RevisitRef
		want    want
		wantErr bool
	}{
		{
			"ServerNotModified profile",
			createRecord1(Response,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:b285747ad7cc57aa74bce2e30b453c8d1cb71ba4"},
					&nameValue{Name: WarcPayloadDigest, Value: "sha1:c37ffb221569c553a2476c22c7dad429f3492977"},
					&nameValue{Name: ContentLength, Value: "257"},
				},
				"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n"+
					"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n"+
					"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the content"),
			&RevisitRef{Profile: ProfileServerNotModifiedV1_1, TargetRecordId: "targetId"},
			want{
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcType, Value: "revisit"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:bf9d96d3f3f230ce8e2c6a3e5e1d51a81016b55e"},
					&nameValue{Name: WarcPayloadDigest, Value: "sha1:c37ffb221569c553a2476c22c7dad429f3492977"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
					&nameValue{Name: ContentLength, Value: "238"},
					&nameValue{Name: WarcProfile, Value: ProfileServerNotModifiedV1_1},
					&nameValue{Name: WarcRefersTo, Value: "<targetId>"},
					&nameValue{Name: WarcTruncated, Value: "length"},
				},
				"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n" +
					"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n" +
					"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\n",
			},
			false,
		},
		{
			"ServerNotModified profile missing payload digest\"",
			createRecord1(Response,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:B285747AD7CC57AA74BCE2E30B453C8D1CB71BA4"},
					&nameValue{Name: ContentLength, Value: "257"},
				},
				"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n"+
					"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n"+
					"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the content"),
			&RevisitRef{Profile: ProfileServerNotModifiedV1_1, TargetRecordId: "targetId"},
			want{
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcType, Value: "revisit"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:bf9d96d3f3f230ce8e2c6a3e5e1d51a81016b55e"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
					&nameValue{Name: ContentLength, Value: "238"},
					&nameValue{Name: WarcProfile, Value: ProfileServerNotModifiedV1_1},
					&nameValue{Name: WarcRefersTo, Value: "<targetId>"},
					&nameValue{Name: WarcTruncated, Value: "length"},
				},
				"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n" +
					"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n" +
					"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\n",
			},
			false,
		},
		{
			"IdenticalPayloadDigest profile",
			createRecord1(Response,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:b285747ad7cc57aa74bce2e30b453c8d1cb71ba4"},
					&nameValue{Name: WarcPayloadDigest, Value: "sha1:c37ffb221569c553a2476c22c7dad429f3492977"},
					&nameValue{Name: ContentLength, Value: "257"},
				},
				"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n"+
					"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n"+
					"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the content"),
			&RevisitRef{Profile: ProfileIdenticalPayloadDigestV1_1, TargetRecordId: "targetId"},
			want{
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcType, Value: "revisit"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:bf9d96d3f3f230ce8e2c6a3e5e1d51a81016b55e"},
					&nameValue{Name: WarcPayloadDigest, Value: "sha1:c37ffb221569c553a2476c22c7dad429f3492977"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
					&nameValue{Name: ContentLength, Value: "238"},
					&nameValue{Name: WarcProfile, Value: ProfileIdenticalPayloadDigestV1_1},
					&nameValue{Name: WarcRefersTo, Value: "<targetId>"},
					&nameValue{Name: WarcTruncated, Value: "length"},
				},
				"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n" +
					"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n" +
					"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\n",
			},
			false,
		},
		{
			"IdenticalPayloadDigest profile missing payload digest",
			createRecord1(Response,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:B285747AD7CC57AA74BCE2E30B453C8D1CB71BA4"},
					&nameValue{Name: ContentLength, Value: "257"},
				},
				"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n"+
					"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n"+
					"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the content"),
			&RevisitRef{Profile: ProfileIdenticalPayloadDigestV1_1, TargetRecordId: "targetId"},
			want{},
			true,
		},
		{
			"IdenticalPayloadDigest profile resource record missing payload digest",
			createRecord1(Resource,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: ContentType, Value: "text/plain"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:c37ffb221569c553a2476c22c7dad429f3492977"},
					&nameValue{Name: ContentLength, Value: "19"},
				},
				"This is the content"),
			&RevisitRef{Profile: ProfileIdenticalPayloadDigestV1_1, TargetRecordId: "targetId"},
			want{
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcType, Value: "revisit"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:da39a3ee5e6b4b0d3255bfef95601890afd80709"},
					&nameValue{Name: WarcPayloadDigest, Value: "sha1:c37ffb221569c553a2476c22c7dad429f3492977"},
					&nameValue{Name: ContentType, Value: "text/plain"},
					&nameValue{Name: ContentLength, Value: "0"},
					&nameValue{Name: WarcProfile, Value: ProfileIdenticalPayloadDigestV1_1},
					&nameValue{Name: WarcRefersTo, Value: "<targetId>"},
					&nameValue{Name: WarcTruncated, Value: "length"},
				},
				"",
			},
			false,
		},
		{
			"IdenticalPayloadDigest profile metadata record",
			createRecord1(Resource,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: ContentType, Value: "text/plain"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:c37ffb221569c553a2476c22c7dad429f3492977"},
					&nameValue{Name: WarcPayloadDigest, Value: "sha1:c37ffb221569c553a2476c22c7dad429f3492977"},
					&nameValue{Name: ContentLength, Value: "19"},
				},
				"This is the content"),
			&RevisitRef{Profile: ProfileIdenticalPayloadDigestV1_1, TargetRecordId: "targetId"},
			want{
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcType, Value: "revisit"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:da39a3ee5e6b4b0d3255bfef95601890afd80709"},
					&nameValue{Name: WarcPayloadDigest, Value: "sha1:c37ffb221569c553a2476c22c7dad429f3492977"},
					&nameValue{Name: ContentType, Value: "text/plain"},
					&nameValue{Name: ContentLength, Value: "0"},
					&nameValue{Name: WarcProfile, Value: ProfileIdenticalPayloadDigestV1_1},
					&nameValue{Name: WarcRefersTo, Value: "<targetId>"},
					&nameValue{Name: WarcTruncated, Value: "length"},
				},
				"",
			},
			false,
		},
		{
			"IdenticalPayloadDigest profile metadata record missing payload digest",
			createRecord1(Metadata,
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: ContentType, Value: "text/plain"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:C37FFB221569C553A2476C22C7DAD429F3492977"},
					&nameValue{Name: ContentLength, Value: "19"},
				},
				"This is the content"),
			&RevisitRef{Profile: ProfileIdenticalPayloadDigestV1_1, TargetRecordId: "targetId"},
			want{},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)
			if !tt.record.Block().IsCached() {
				assert.NoError(tt.record.Block().Cache())
			}
			defer func() { _ = tt.record.Close() }()

			got, err := tt.record.ToRevisitRecord(tt.ref)
			if tt.wantErr {
				assert.Error(err)
				assert.Nil(got)
				return
			} else {
				require.NoError(t, err)
			}

			assert.Equal(Revisit.String(), got.Type().String())
			assert.ElementsMatch([]*nameValue(*tt.want.headers), []*nameValue(*got.WarcHeader()))
			assert.IsType(&revisitBlock{}, got.Block())
			r, err := got.Block().RawBytes()
			assert.Nil(err)
			b, err := io.ReadAll(r)
			assert.Nil(err)
			assert.Equal(tt.want.data, string(b))

			assert.True(got.Block().IsCached())
		})
	}
}

func Test_warcRecord_Merge(t *testing.T) {
	type want struct {
		recordType  RecordType
		headers     *WarcFields
		data        string
		httpHeaders *http.Header
		cached      bool
	}
	tests := []struct {
		name             string
		revisitRecord    WarcRecord
		referencedRecord []WarcRecord
		want             want
		wantErr          bool
	}{
		{
			"ServerNotModified profile",
			createRecord1(Revisit,
				&WarcFields{
					&nameValue{Name: WarcTargetURI, Value: "http://example.com"},
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
					&nameValue{Name: ContentLength, Value: "238"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:bf9d96d3f3f230ce8e2c6a3e5e1d51a81016b55e"},
					&nameValue{Name: WarcPayloadDigest, Value: "sha1:c37ffb221569c553a2476c22c7dad429f3492977"},
					&nameValue{Name: WarcProfile, Value: ProfileServerNotModifiedV1_1},
					&nameValue{Name: WarcRefersTo, Value: "<urn:uuid:fff0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcRefersToTargetURI, Value: "http://example.com"},
					&nameValue{Name: WarcRefersToDate, Value: "2016-09-19T18:03:53Z"},
					&nameValue{Name: WarcTruncated, Value: "length"},
				},
				"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n"+
					"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n"+
					"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\n"),
			[]WarcRecord{createRecord1(Response,
				&WarcFields{
					&nameValue{Name: WarcTargetURI, Value: "http://example.com"},
					&nameValue{Name: WarcDate, Value: "2016-09-19T18:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:fff0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:6E9D6B234FEEBBF1AB618707217E577C3B83448A"},
					&nameValue{Name: ContentLength, Value: "236"},
				},
				"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n"+
					"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02fff\"\n"+
					"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the content")},
			want{
				Response,
				&WarcFields{
					&nameValue{Name: WarcTargetURI, Value: "http://example.com"},
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcType, Value: "response"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:6e9d6b234feebbf1ab618707217e577c3b83448a"},
					&nameValue{Name: WarcPayloadDigest, Value: "sha1:c37ffb221569c553a2476c22c7dad429f3492977"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
					&nameValue{Name: ContentLength, Value: "257"},
				},
				"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n" +
					"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n" +
					"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the content",
				&http.Header{
					"Date":           []string{"Tue, 19 Sep 2016 17:18:40 GMT"},
					"Server":         []string{"Apache/2.0.54 (Ubuntu)"},
					"Last-Modified":  []string{"Mon, 16 Jun 2013 22:28:51 GMT"},
					"Etag":           []string{"\"3e45-67e-2ed02ec0\""},
					"Accept-Ranges":  []string{"bytes"},
					"Content-Length": []string{"19"},
					"Content-Type":   []string{"text/plain"},
				},
				true,
			},
			false,
		},
		{
			"IdenticalPayloadDigest profile",
			createRecord1(Revisit,
				&WarcFields{
					&nameValue{Name: WarcTargetURI, Value: "http://foo.com"},
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
					&nameValue{Name: ContentLength, Value: "238"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:bf9d96d3f3f230ce8e2c6a3e5e1d51a81016b55e"},
					&nameValue{Name: WarcPayloadDigest, Value: "sha1:c37ffb221569c553a2476c22c7dad429f3492977"},
					&nameValue{Name: WarcProfile, Value: ProfileIdenticalPayloadDigestV1_1},
					&nameValue{Name: WarcRefersTo, Value: "<urn:uuid:fff0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcRefersToTargetURI, Value: "http://example.com"},
					&nameValue{Name: WarcRefersToDate, Value: "2016-09-19T18:03:53Z"},
					&nameValue{Name: WarcTruncated, Value: "length"},
				},
				"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n"+
					"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n"+
					"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\n"),
			[]WarcRecord{createRecord1(Response,
				&WarcFields{
					&nameValue{Name: WarcTargetURI, Value: "http://example.com"},
					&nameValue{Name: WarcDate, Value: "2016-09-19T18:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:fff0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:6E9D6B234FEEBBF1AB618707217E577C3B83448A"},
					&nameValue{Name: ContentLength, Value: "236"},
				},
				"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n"+
					"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02fff\"\n"+
					"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the content")},
			want{
				Response,
				&WarcFields{
					&nameValue{Name: WarcTargetURI, Value: "http://foo.com"},
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcType, Value: "response"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:6e9d6b234feebbf1ab618707217e577c3b83448a"},
					&nameValue{Name: WarcPayloadDigest, Value: "sha1:c37ffb221569c553a2476c22c7dad429f3492977"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
					&nameValue{Name: ContentLength, Value: "257"},
				},
				"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n" +
					"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n" +
					"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the content",
				&http.Header{
					"Date":           []string{"Tue, 19 Sep 2016 17:18:40 GMT"},
					"Server":         []string{"Apache/2.0.54 (Ubuntu)"},
					"Last-Modified":  []string{"Mon, 16 Jun 2013 22:28:51 GMT"},
					"Etag":           []string{"\"3e45-67e-2ed02ec0\""},
					"Accept-Ranges":  []string{"bytes"},
					"Content-Length": []string{"19"},
					"Content-Type":   []string{"text/plain"},
				},
				true,
			},
			false,
		},
		{
			"Missing empty line - IdenticalPayloadDigest profile",
			createRecord1(Revisit,
				&WarcFields{
					&nameValue{Name: WarcTargetURI, Value: "http://foo.com"},
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
					&nameValue{Name: ContentLength, Value: "237"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:d1ea0889024bd99516d23ca2ad5e30e850977c84"},
					&nameValue{Name: WarcPayloadDigest, Value: "sha1:c37ffb221569c553a2476c22c7dad429f3492977"},
					&nameValue{Name: WarcProfile, Value: ProfileIdenticalPayloadDigestV1_1},
					&nameValue{Name: WarcRefersTo, Value: "<urn:uuid:fff0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcRefersToTargetURI, Value: "http://example.com"},
					&nameValue{Name: WarcRefersToDate, Value: "2016-09-19T18:03:53Z"},
					&nameValue{Name: WarcTruncated, Value: "length"},
				},
				"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n"+
					"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n"+
					"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n"),
			[]WarcRecord{createRecord1(Response,
				&WarcFields{
					&nameValue{Name: WarcTargetURI, Value: "http://example.com"},
					&nameValue{Name: WarcDate, Value: "2016-09-19T18:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:fff0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:6e9d6b234feebbf1ab618707217e577c3b83448a"},
					&nameValue{Name: ContentLength, Value: "236"},
				},
				"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n"+
					"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02fff\"\n"+
					"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the content")},
			want{
				Response,
				&WarcFields{
					&nameValue{Name: WarcTargetURI, Value: "http://foo.com"},
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcType, Value: "response"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:6e9d6b234feebbf1ab618707217e577c3b83448a"},
					&nameValue{Name: WarcPayloadDigest, Value: "sha1:c37ffb221569c553a2476c22c7dad429f3492977"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
					&nameValue{Name: ContentLength, Value: "256"},
				},
				"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n" +
					"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n" +
					"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\r\nThis is the content",
				&http.Header{
					"Date":           []string{"Tue, 19 Sep 2016 17:18:40 GMT"},
					"Server":         []string{"Apache/2.0.54 (Ubuntu)"},
					"Last-Modified":  []string{"Mon, 16 Jun 2013 22:28:51 GMT"},
					"Etag":           []string{"\"3e45-67e-2ed02ec0\""},
					"Accept-Ranges":  []string{"bytes"},
					"Content-Length": []string{"19"},
					//"Connection":     []string{"close"},
					"Content-Type": []string{"text/plain"},
				},
				true,
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)
			defer func() { _ = tt.revisitRecord.Close() }()
			defer func() {
				for _, r := range tt.referencedRecord {
					_ = r.Close()
				}
			}()

			rr, err := tt.revisitRecord.RevisitRef()
			assert.NoError(err)
			revisitRef, err := tt.referencedRecord[0].CreateRevisitRef(rr.Profile)
			assert.NoError(err)
			assert.Equal(revisitRef, rr)

			got, err := tt.revisitRecord.Merge(tt.referencedRecord...)
			if tt.wantErr {
				assert.Error(err)
				assert.Nil(got)
				return
			} else {
				require.NoError(t, err)
			}

			assert.Equal(tt.want.recordType.String(), got.Type().String())
			assert.ElementsMatch([]*nameValue(*tt.want.headers), []*nameValue(*got.WarcHeader()))
			assert.IsType(&httpResponseBlock{}, got.Block())
			respBlock := got.Block().(*httpResponseBlock)
			assert.Equal(tt.want.httpHeaders, respBlock.httpHeader)

			r, err := got.Block().RawBytes()
			assert.Nil(err)
			b, err := io.ReadAll(r)
			assert.Nil(err)
			assert.Equal(tt.want.data, string(b))

			assert.Equal(tt.want.cached, got.Block().IsCached())
		})
	}
}

func Test_warcRecord_RecordId(t *testing.T) {
	tests := []struct {
		name     string
		headerID string
		wantID   string
	}{
		{
			"urn uuid",
			"<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>",
			"urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008",
		},
		{
			"plain uri",
			"<http://example.com/id/1>",
			"http://example.com/id/1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := createRecord1(Warcinfo,
				&WarcFields{
					&nameValue{Name: WarcRecordID, Value: tt.headerID},
					&nameValue{Name: WarcDate, Value: "2024-01-01T00:00:00Z"},
					&nameValue{Name: ContentType, Value: ApplicationWarcFields},
					&nameValue{Name: ContentLength, Value: "0"},
				}, "")
			defer func() { assert.NoError(t, record.Close()) }()

			assert.Equal(t, tt.wantID, record.RecordId())
		})
	}
}

func Test_warcRecord_ContentLength(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    int64
		wantErr bool
	}{
		{"zero", "", 0, false},
		{"five bytes", "Hello", 5, false},
		{"nineteen bytes", "This is 19 bytes!..", 19, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := createRecord1(Warcinfo,
				&WarcFields{
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:00000000-0000-0000-0000-000000000001>"},
					&nameValue{Name: WarcDate, Value: "2024-01-01T00:00:00Z"},
					&nameValue{Name: ContentType, Value: ApplicationWarcFields},
					&nameValue{Name: ContentLength, Value: fmt.Sprintf("%d", len(tt.content))},
				}, tt.content)
			defer func() { assert.NoError(t, record.Close()) }()

			got, err := record.ContentLength()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func Test_warcRecord_Date(t *testing.T) {
	tests := []struct {
		name     string
		date     string
		wantYear int
		wantErr  bool
	}{
		{"valid date", "2024-06-15T10:30:00Z", 2024, false},
		{"year 2000", "2000-01-01T00:00:00Z", 2000, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := createRecord1(Warcinfo,
				&WarcFields{
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:00000000-0000-0000-0000-000000000002>"},
					&nameValue{Name: WarcDate, Value: tt.date},
					&nameValue{Name: ContentType, Value: ApplicationWarcFields},
					&nameValue{Name: ContentLength, Value: "0"},
				}, "")
			defer func() { assert.NoError(t, record.Close()) }()

			got, err := record.Date()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantYear, got.Year())
			}
		})
	}
}

func Test_warcVersion_MajorMinor(t *testing.T) {
	tests := []struct {
		name      string
		version   *WarcVersion
		wantMajor uint8
		wantMinor uint8
		wantStr   string
	}{
		{"v1.0", V1_0, 1, 0, "WARC/1.0"},
		{"v1.1", V1_1, 1, 1, "WARC/1.1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantMajor, tt.version.Major())
			assert.Equal(t, tt.wantMinor, tt.version.Minor())
			assert.Equal(t, tt.wantStr, tt.version.String())
		})
	}
}

func createRecord1(recordType RecordType, headers *WarcFields, data string) WarcRecord {
	rb := NewRecordBuilder(recordType, WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrWarn),
		WithUnknownRecordTypePolicy(ErrIgnore), WithFixDigest(false), WithAddMissingDigest(false),
		WithDefaultDigestEncoding(Base16))
	for _, nv := range *headers {
		rb.AddWarcHeader(nv.Name, nv.Value)
	}
	if _, err := rb.WriteString(data); err != nil {
		panic(err)
	}

	wr, _, err := rb.Build()
	if err != nil {
		panic(err)
	}
	return wr
}

func Test_warcRecord_Merge_ErrorPaths(t *testing.T) {
	tests := []struct {
		name    string
		record  func() WarcRecord
		refs    func() []WarcRecord
		wantErr string
	}{
		{
			"segmented record",
			func() WarcRecord {
				return createRecord1(Response, &WarcFields{
					&nameValue{Name: WarcDate, Value: "2024-01-01T00:00:00Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:00000000-0000-0000-0000-000000000001>"},
					&nameValue{Name: ContentType, Value: "text/plain"},
					&nameValue{Name: ContentLength, Value: "0"},
					&nameValue{Name: WarcSegmentNumber, Value: "1"},
				}, "")
			},
			func() []WarcRecord { return nil },
			"merging of segmented records is not implemented",
		},
		{
			"non-revisit non-segmented record",
			func() WarcRecord {
				return createRecord1(Response, &WarcFields{
					&nameValue{Name: WarcDate, Value: "2024-01-01T00:00:00Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:00000000-0000-0000-0000-000000000001>"},
					&nameValue{Name: ContentType, Value: "text/plain"},
					&nameValue{Name: ContentLength, Value: "0"},
				}, "")
			},
			func() []WarcRecord { return nil },
			"merging is only possible for revisit records or segmented records",
		},
		{
			"revisit with zero referenced records",
			func() WarcRecord {
				return createRecord1(Revisit, &WarcFields{
					&nameValue{Name: WarcDate, Value: "2024-01-01T00:00:00Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:00000000-0000-0000-0000-000000000001>"},
					&nameValue{Name: ContentType, Value: "text/plain"},
					&nameValue{Name: ContentLength, Value: "0"},
					&nameValue{Name: WarcProfile, Value: ProfileServerNotModifiedV1_1},
				}, "")
			},
			func() []WarcRecord { return nil },
			"revisit merge requires exactly one referenced record",
		},
		{
			"revisit with two referenced records",
			func() WarcRecord {
				return createRecord1(Revisit, &WarcFields{
					&nameValue{Name: WarcDate, Value: "2024-01-01T00:00:00Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:00000000-0000-0000-0000-000000000001>"},
					&nameValue{Name: ContentType, Value: "text/plain"},
					&nameValue{Name: ContentLength, Value: "0"},
					&nameValue{Name: WarcProfile, Value: ProfileServerNotModifiedV1_1},
				}, "")
			},
			func() []WarcRecord {
				r1 := createRecord1(Response, &WarcFields{
					&nameValue{Name: WarcDate, Value: "2024-01-01T00:00:00Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:00000000-0000-0000-0000-000000000002>"},
					&nameValue{Name: ContentType, Value: "text/plain"},
					&nameValue{Name: ContentLength, Value: "0"},
				}, "")
				r2 := createRecord1(Response, &WarcFields{
					&nameValue{Name: WarcDate, Value: "2024-01-01T00:00:00Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:00000000-0000-0000-0000-000000000003>"},
					&nameValue{Name: ContentType, Value: "text/plain"},
					&nameValue{Name: ContentLength, Value: "0"},
				}, "")
				return []WarcRecord{r1, r2}
			},
			"revisit merge requires exactly one referenced record",
		},
		{
			"revisit with wrong block type (skipParseBlock)",
			func() WarcRecord {
				rb := NewRecordBuilder(Revisit,
					WithSpecViolationPolicy(ErrIgnore), WithSyntaxErrorPolicy(ErrIgnore),
					WithSkipParseBlock(), WithAddMissingDigest(false), WithFixDigest(false))
				rb.AddWarcHeader(WarcRecordID, "<urn:uuid:00000000-0000-0000-0000-000000000001>")
				rb.AddWarcHeader(WarcDate, "2024-01-01T00:00:00Z")
				rb.AddWarcHeader(ContentType, "text/plain")
				rb.AddWarcHeader(ContentLength, "0")
				rb.AddWarcHeader(WarcProfile, ProfileServerNotModifiedV1_1)
				wr, _, err := rb.Build()
				if err != nil {
					panic(err)
				}
				return wr
			},
			func() []WarcRecord {
				return []WarcRecord{createRecord1(Response, &WarcFields{
					&nameValue{Name: WarcDate, Value: "2024-01-01T00:00:00Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:00000000-0000-0000-0000-000000000002>"},
					&nameValue{Name: ContentType, Value: "text/plain"},
					&nameValue{Name: ContentLength, Value: "0"},
				}, "")}
			},
			"revisit block type incompatible with merge",
		},
		{
			"revisit merge with non-http block",
			func() WarcRecord {
				return createRecord1(Revisit, &WarcFields{
					&nameValue{Name: WarcDate, Value: "2024-01-01T00:00:00Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:00000000-0000-0000-0000-000000000001>"},
					&nameValue{Name: ContentType, Value: "text/plain"},
					&nameValue{Name: ContentLength, Value: "0"},
					&nameValue{Name: WarcProfile, Value: ProfileServerNotModifiedV1_1},
				}, "")
			},
			func() []WarcRecord {
				return []WarcRecord{createRecord1(Warcinfo, &WarcFields{
					&nameValue{Name: WarcDate, Value: "2024-01-01T00:00:00Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:00000000-0000-0000-0000-000000000002>"},
					&nameValue{Name: ContentType, Value: ApplicationWarcFields},
					&nameValue{Name: ContentLength, Value: "0"},
				}, "")}
			},
			"merge only supports http request and response blocks",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := tt.record()
			defer func() { assert.NoError(t, rec.Close()) }()
			refs := tt.refs()
			defer func() {
				for _, r := range refs {
					assert.NoError(t, r.Close())
				}
			}()
			_, err := rec.Merge(refs...)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func Test_warcRecord_Merge_WithRequestBlock(t *testing.T) {
	revisitRecord := createRecord1(Revisit, &WarcFields{
		&nameValue{Name: WarcTargetURI, Value: "http://example.com"},
		&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
		&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
		&nameValue{Name: ContentType, Value: "application/http;msgtype=request"},
		&nameValue{Name: ContentLength, Value: "76"},
		&nameValue{Name: WarcProfile, Value: ProfileServerNotModifiedV1_1},
		&nameValue{Name: WarcRefersTo, Value: "<urn:uuid:fff0cecc-0221-11e7-adb1-0242ac120008>"},
		&nameValue{Name: WarcTruncated, Value: "length"},
	}, "GET / HTTP/1.0\nHost: example.com\nUser-Agent: TestBot/1.0\nAccept: text/html\n\n")
	defer func() { assert.NoError(t, revisitRecord.Close()) }()

	referencedRecord := createRecord1(Request, &WarcFields{
		&nameValue{Name: WarcTargetURI, Value: "http://example.com"},
		&nameValue{Name: WarcDate, Value: "2016-09-19T18:03:53Z"},
		&nameValue{Name: WarcRecordID, Value: "<urn:uuid:fff0cecc-0221-11e7-adb1-0242ac120008>"},
		&nameValue{Name: ContentType, Value: "application/http;msgtype=request"},
		&nameValue{Name: ContentLength, Value: "69"},
	}, "GET / HTTP/1.0\nHost: example.com\nUser-Agent: OldBot/1.0\nAccept: */*\n\n")
	defer func() { assert.NoError(t, referencedRecord.Close()) }()

	got, err := revisitRecord.Merge(referencedRecord)
	require.NoError(t, err)
	assert.Equal(t, Request, got.Type())
	assert.IsType(t, &httpRequestBlock{}, got.Block())
}

func Test_warcRecord_ToRevisitRecord_Errors(t *testing.T) {
	tests := []struct {
		name    string
		ref     *RevisitRef
		wantErr string
	}{
		{
			"unknown profile",
			&RevisitRef{Profile: "http://example.com/unknown-profile"},
			"unknown revisit profile",
		},
		{
			"IdenticalPayloadDigest without payload digest",
			&RevisitRef{Profile: ProfileIdenticalPayloadDigestV1_0},
			"payload digest required for identical-payload-digest profile",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Record without payload digest
			record := createRecord1(Response, &WarcFields{
				&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
				&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
				&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
				&nameValue{Name: WarcBlockDigest, Value: "sha1:B285747AD7CC57AA74BCE2E30B453C8D1CB71BA4"},
				&nameValue{Name: ContentLength, Value: "257"},
			}, "HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n"+
				"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n"+
				"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the content")
			defer func() { assert.NoError(t, record.Close()) }()

			_ = record.Block().Cache()
			_, err := record.ToRevisitRecord(tt.ref)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func Test_warcRecord_RevisitRef_NonRevisit(t *testing.T) {
	record := createRecord1(Response, &WarcFields{
		&nameValue{Name: WarcDate, Value: "2024-01-01T00:00:00Z"},
		&nameValue{Name: WarcRecordID, Value: "<urn:uuid:00000000-0000-0000-0000-000000000001>"},
		&nameValue{Name: ContentType, Value: "text/plain"},
		&nameValue{Name: ContentLength, Value: "0"},
	}, "")
	defer func() { assert.NoError(t, record.Close()) }()

	_, err := record.RevisitRef()
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrNotRevisitRecord)
}

func Test_warcRecord_CreateRevisitRef_FromRevisit(t *testing.T) {
	record := createRecord1(Revisit, &WarcFields{
		&nameValue{Name: WarcDate, Value: "2024-01-01T00:00:00Z"},
		&nameValue{Name: WarcRecordID, Value: "<urn:uuid:00000000-0000-0000-0000-000000000001>"},
		&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
		&nameValue{Name: ContentLength, Value: "0"},
		&nameValue{Name: WarcProfile, Value: ProfileServerNotModifiedV1_1},
	}, "")
	defer func() { assert.NoError(t, record.Close()) }()

	_, err := record.CreateRevisitRef(ProfileServerNotModifiedV1_1)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrIsRevisitRecord)
}

func Test_warcRecord_ValidateDigest_Paths(t *testing.T) {
	t.Run("content length mismatch with fixContentLength", func(t *testing.T) {
		rb := NewRecordBuilder(Response,
			WithSpecViolationPolicy(ErrWarn),
			WithSyntaxErrorPolicy(ErrIgnore),
			WithFixDigest(true),
			WithFixContentLength(true),
			WithAddMissingDigest(true),
			WithDefaultDigestEncoding(Base16))
		rb.AddWarcHeader(WarcRecordID, "<urn:uuid:00000000-0000-0000-0000-000000000001>")
		rb.AddWarcHeader(WarcDate, "2024-01-01T00:00:00Z")
		rb.AddWarcHeader(ContentType, "text/plain")
		rb.AddWarcHeader(ContentLength, "999") // wrong length
		_, err := rb.WriteString("Hello")
		require.NoError(t, err)
		wr, v, err := rb.Build()
		require.NoError(t, err)
		defer func() { assert.NoError(t, wr.Close()) }()
		// Validation should have content length mismatch warning
		assert.NotEmpty(t, v)
		// Content-Length should be fixed
		cl, _ := wr.ContentLength()
		assert.Equal(t, int64(5), cl)
	})

	t.Run("content length mismatch ErrFail", func(t *testing.T) {
		rb := NewRecordBuilder(Response,
			WithSpecViolationPolicy(ErrFail),
			WithSyntaxErrorPolicy(ErrIgnore),
			WithFixDigest(false),
			WithFixContentLength(false),
			WithAddMissingDigest(false))
		rb.AddWarcHeader(WarcRecordID, "<urn:uuid:00000000-0000-0000-0000-000000000001>")
		rb.AddWarcHeader(WarcDate, "2024-01-01T00:00:00Z")
		rb.AddWarcHeader(ContentType, "text/plain")
		rb.AddWarcHeader(ContentLength, "999")
		_, err := rb.WriteString("Hello")
		require.NoError(t, err)
		_, _, err = rb.Build()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "content length mismatch")
	})
}

func Test_warcRecord_Close_NilCloser(t *testing.T) {
	record := createRecord1(Warcinfo, &WarcFields{
		&nameValue{Name: WarcDate, Value: "2024-01-01T00:00:00Z"},
		&nameValue{Name: WarcRecordID, Value: "<urn:uuid:00000000-0000-0000-0000-000000000001>"},
		&nameValue{Name: ContentType, Value: ApplicationWarcFields},
		&nameValue{Name: ContentLength, Value: "0"},
	}, "")
	// First close
	assert.NoError(t, record.Close())
	// Second close should also be fine (closer is nil)
	assert.NoError(t, record.Close())
}

func Test_warcRecord_String_Format(t *testing.T) {
	record := createRecord1(Response, &WarcFields{
		&nameValue{Name: WarcDate, Value: "2024-01-01T00:00:00Z"},
		&nameValue{Name: WarcRecordID, Value: "<urn:uuid:00000000-0000-0000-0000-000000000001>"},
		&nameValue{Name: ContentType, Value: "text/plain"},
		&nameValue{Name: ContentLength, Value: "5"},
	}, "Hello")
	defer func() { assert.NoError(t, record.Close()) }()

	s := record.String()
	assert.Contains(t, s, "WARC record")
	assert.Contains(t, s, "response")
	assert.Contains(t, s, "00000000-0000-0000-0000-000000000001")
}

func Test_warcRecord_ToRevisitRecord_WithTargetUriAndDate(t *testing.T) {
	record := createRecord1(Response, &WarcFields{
		&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
		&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
		&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
		&nameValue{Name: WarcBlockDigest, Value: "sha1:b285747ad7cc57aa74bce2e30b453c8d1cb71ba4"},
		&nameValue{Name: WarcPayloadDigest, Value: "sha1:c37ffb221569c553a2476c22c7dad429f3492977"},
		&nameValue{Name: ContentLength, Value: "257"},
	}, "HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n"+
		"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n"+
		"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the content")
	defer func() { assert.NoError(t, record.Close()) }()
	err := record.Block().Cache()
	require.NoError(t, err)

	ref := &RevisitRef{
		Profile:        ProfileIdenticalPayloadDigestV1_0,
		TargetRecordId: "targetId",
		TargetUri:      "http://example.com",
		TargetDate:     "2017-03-06T04:03:53Z",
	}
	revisit, err := record.ToRevisitRecord(ref)
	require.NoError(t, err)
	assert.Equal(t, Revisit, revisit.Type())
	assert.Equal(t, "http://example.com", revisit.WarcHeader().Get(WarcRefersToTargetURI))
	assert.Equal(t, "2017-03-06T04:03:53Z", revisit.WarcHeader().Get(WarcRefersToDate))
}

func Test_RecordType_String_Unknown(t *testing.T) {
	rt := RecordType(255)
	assert.Equal(t, "unknown", rt.String())
}

func Test_warcRecord_Merge_ParseHeadersError_Response(t *testing.T) {
	// Create a revisit record with malformed HTTP headers (not parseable as a response)
	revisitRecord := createRecord1(Revisit, &WarcFields{
		&nameValue{Name: WarcTargetURI, Value: "http://example.com"},
		&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
		&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
		&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
		&nameValue{Name: ContentLength, Value: "21"},
		&nameValue{Name: WarcProfile, Value: ProfileServerNotModifiedV1_1},
		&nameValue{Name: WarcRefersTo, Value: "<urn:uuid:fff0cecc-0221-11e7-adb1-0242ac120008>"},
	}, "GARBAGE NOT HTTP\r\n\r\n\n")
	defer func() { assert.NoError(t, revisitRecord.Close()) }()

	// Reference record with valid response block
	referencedRecord := createRecord1(Response, &WarcFields{
		&nameValue{Name: WarcTargetURI, Value: "http://example.com"},
		&nameValue{Name: WarcDate, Value: "2016-09-19T18:03:53Z"},
		&nameValue{Name: WarcRecordID, Value: "<urn:uuid:fff0cecc-0221-11e7-adb1-0242ac120008>"},
		&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
		&nameValue{Name: ContentLength, Value: "64"},
	}, "HTTP/1.1 200 OK\nContent-Type: text/plain\nContent-Length: 4\n\ntest")
	defer func() { assert.NoError(t, referencedRecord.Close()) }()

	// With ErrWarn syntax policy (from createRecord1), the merge should attempt CRLF append retry
	got, err := revisitRecord.Merge(referencedRecord)
	// The malformed headers will fail parseHeaders — since errSyntax is ErrWarn (not > ErrWarn),
	// it goes to the retry path with CRLF appended. That'll still fail.
	// The second parseHeaders error returns wr, err.
	assert.NotNil(t, got)
	assert.Error(t, err)
}

func Test_warcRecord_Merge_ParseHeadersError_Request(t *testing.T) {
	// Create a revisit record with malformed HTTP headers (not parseable as a request)
	revisitRecord := createRecord1(Revisit, &WarcFields{
		&nameValue{Name: WarcTargetURI, Value: "http://example.com"},
		&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
		&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
		&nameValue{Name: ContentType, Value: "application/http;msgtype=request"},
		&nameValue{Name: ContentLength, Value: "21"},
		&nameValue{Name: WarcProfile, Value: ProfileServerNotModifiedV1_1},
		&nameValue{Name: WarcRefersTo, Value: "<urn:uuid:fff0cecc-0221-11e7-adb1-0242ac120008>"},
	}, "GARBAGE NOT HTTP\r\n\r\n\n")
	defer func() { assert.NoError(t, revisitRecord.Close()) }()

	// Reference record with valid request block — need correct content length
	reqContent := "GET / HTTP/1.0\nHost: example.com\n\ndata"
	referencedRecord := createRecord1(Request, &WarcFields{
		&nameValue{Name: WarcTargetURI, Value: "http://example.com"},
		&nameValue{Name: WarcDate, Value: "2016-09-19T18:03:53Z"},
		&nameValue{Name: WarcRecordID, Value: "<urn:uuid:fff0cecc-0221-11e7-adb1-0242ac120008>"},
		&nameValue{Name: ContentType, Value: "application/http;msgtype=request"},
		&nameValue{Name: ContentLength, Value: fmt.Sprintf("%d", len(reqContent))},
	}, reqContent)
	defer func() { assert.NoError(t, referencedRecord.Close()) }()

	got, err := revisitRecord.Merge(referencedRecord)
	assert.NotNil(t, got)
	assert.Error(t, err)
}

func Test_warcRecord_Merge_ParseHeadersError_ErrFail(t *testing.T) {
	// Use ErrFail syntax policy so parseHeaders error immediately returns
	rb := NewRecordBuilder(Revisit,
		WithSpecViolationPolicy(ErrIgnore), WithSyntaxErrorPolicy(ErrFail),
		WithFixDigest(false), WithAddMissingDigest(false),
		WithDefaultDigestEncoding(Base16))
	rb.AddWarcHeader(WarcTargetURI, "http://example.com")
	rb.AddWarcHeader(WarcDate, "2017-03-06T04:03:53Z")
	rb.AddWarcHeader(WarcRecordID, "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>")
	rb.AddWarcHeader(ContentType, "application/http;msgtype=response")
	rb.AddWarcHeader(ContentLength, "21")
	rb.AddWarcHeader(WarcProfile, ProfileServerNotModifiedV1_1)
	rb.AddWarcHeader(WarcRefersTo, "<urn:uuid:fff0cecc-0221-11e7-adb1-0242ac120008>")
	_, _ = rb.WriteString("GARBAGE NOT HTTP\r\n\r\n\n")
	revisitRecord, _, err := rb.Build()
	require.NoError(t, err)
	defer func() { assert.NoError(t, revisitRecord.Close()) }()

	referencedRecord := createRecord1(Response, &WarcFields{
		&nameValue{Name: WarcTargetURI, Value: "http://example.com"},
		&nameValue{Name: WarcDate, Value: "2016-09-19T18:03:53Z"},
		&nameValue{Name: WarcRecordID, Value: "<urn:uuid:fff0cecc-0221-11e7-adb1-0242ac120008>"},
		&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
		&nameValue{Name: ContentLength, Value: "64"},
	}, "HTTP/1.1 200 OK\nContent-Type: text/plain\nContent-Length: 4\n\ntest")
	defer func() { assert.NoError(t, referencedRecord.Close()) }()

	// errSyntax=ErrFail, so first parseHeaders error returns immediately
	got, err := revisitRecord.Merge(referencedRecord)
	assert.NotNil(t, got)
	assert.Error(t, err)
}

func Test_warcRecord_Merge_RefBlockNotCached(t *testing.T) {
	// Create a revisit that will merge successfully, but the reference record block is not cached
	// This exercises the "else { wr.headers.Delete(WarcBlockDigest) }" branch
	revisitRecord := createRecord1(Revisit, &WarcFields{
		&nameValue{Name: WarcTargetURI, Value: "http://example.com"},
		&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
		&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
		&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
		&nameValue{Name: ContentLength, Value: "257"},
		&nameValue{Name: WarcProfile, Value: ProfileServerNotModifiedV1_1},
		&nameValue{Name: WarcRefersTo, Value: "<urn:uuid:fff0cecc-0221-11e7-adb1-0242ac120008>"},
		&nameValue{Name: WarcBlockDigest, Value: "sha1:b285747ad7cc57aa74bce2e30b453c8d1cb71ba4"},
	}, "HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n"+
		"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n"+
		"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the content")
	defer func() { assert.NoError(t, revisitRecord.Close()) }()

	// Build a raw WARC record and unmarshal from a non-seekable reader to get an uncached block.
	// NopCloser wraps the reader WITHOUT io.Seeker, so httpResponseBlock.IsCached() returns false.
	rawWARC := "WARC/1.1\r\n" +
		"WARC-Type: response\r\n" +
		"WARC-Target-URI: http://example.com\r\n" +
		"WARC-Date: 2016-09-19T18:03:53Z\r\n" +
		"WARC-Record-ID: <urn:uuid:fff0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
		"Content-Type: application/http;msgtype=response\r\n" +
		"Content-Length: 257\r\n" +
		"WARC-Block-Digest: sha1:b285747ad7cc57aa74bce2e30b453c8d1cb71ba4\r\n" +
		"\r\n" +
		"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n" +
		"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n" +
		"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the content" +
		"\r\n\r\n"
	nonSeekableReader := io.NopCloser(strings.NewReader(rawWARC))
	u := NewUnmarshaler(
		WithSpecViolationPolicy(ErrIgnore),
		WithSyntaxErrorPolicy(ErrIgnore),
	)
	referencedRecord, _, _, err := u.Unmarshal(bufio.NewReader(nonSeekableReader))
	require.NoError(t, err)
	defer func() { assert.NoError(t, referencedRecord.Close()) }()

	// Confirm the block is NOT cached (non-seekable stream)
	assert.False(t, referencedRecord.Block().IsCached())

	got, err := revisitRecord.Merge(referencedRecord)
	require.NoError(t, err)
	assert.Equal(t, Response, got.Type())
	// WarcBlockDigest should have been deleted since ref block wasn't cached
	assert.False(t, got.WarcHeader().Has(WarcBlockDigest))
}

func Test_warcRecord_ValidateDigest_WrongPayloadDigest(t *testing.T) {
	// Test with wrong payload digest: covers payload validation ErrWarn path
	builder := NewRecordBuilder(Response,
		WithSpecViolationPolicy(ErrWarn), WithSyntaxErrorPolicy(ErrWarn),
		WithFixDigest(false), WithAddMissingDigest(false),
		WithDefaultDigestEncoding(Base16))
	_, err := builder.WriteString("HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ntest")
	require.NoError(t, err)
	builder.AddWarcHeader(WarcRecordID, "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>")
	builder.AddWarcHeader(WarcDate, "2006-01-02T15:04:05Z")
	builder.AddWarcHeader(ContentType, "application/http;msgtype=response")
	builder.AddWarcHeader(ContentLength, "42")
	builder.AddWarcHeader(WarcPayloadDigest, "sha1:0000000000000000000000000000000000000000")

	rec, v, err := builder.Build()
	require.NoError(t, err) // ErrWarn should not fail
	defer func() { assert.NoError(t, rec.Close()) }()
	// Validation should contain the wrong payload digest error
	assert.NotEmpty(t, v)
	found := false
	for _, e := range v {
		if e != nil {
			errStr := e.Error()
			if len(errStr) > 0 {
				found = true
			}
		}
	}
	assert.True(t, found, "expected validation errors for wrong payload digest")
}

func Test_warcRecord_ValidateDigest_FixDigest(t *testing.T) {
	// Test with wrong digest + fixDigest=true: covers the fixDigest block+payload paths
	builder := NewRecordBuilder(Response,
		WithSpecViolationPolicy(ErrWarn), WithSyntaxErrorPolicy(ErrWarn),
		WithFixDigest(true), WithAddMissingDigest(false),
		WithDefaultDigestEncoding(Base16))
	_, err := builder.WriteString("HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ntest")
	require.NoError(t, err)
	builder.AddWarcHeader(WarcRecordID, "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>")
	builder.AddWarcHeader(WarcDate, "2006-01-02T15:04:05Z")
	builder.AddWarcHeader(ContentType, "application/http;msgtype=response")
	builder.AddWarcHeader(ContentLength, "42")
	builder.AddWarcHeader(WarcBlockDigest, "sha1:0000000000000000000000000000000000000000")
	builder.AddWarcHeader(WarcPayloadDigest, "sha1:0000000000000000000000000000000000000000")

	rec, _, err := builder.Build()
	require.NoError(t, err)
	defer func() { assert.NoError(t, rec.Close()) }()

	// Digests should have been fixed by the builder
	assert.NotEqual(t, "sha1:0000000000000000000000000000000000000000", rec.WarcHeader().Get(WarcBlockDigest))
	assert.NotEqual(t, "sha1:0000000000000000000000000000000000000000", rec.WarcHeader().Get(WarcPayloadDigest))
}

func Test_warcRecord_ValidateDigest_FixContentLength(t *testing.T) {
	// Test content length mismatch with fixContentLength=true
	builder := NewRecordBuilder(Response,
		WithSpecViolationPolicy(ErrWarn), WithSyntaxErrorPolicy(ErrWarn),
		WithFixDigest(false), WithAddMissingDigest(false),
		WithFixContentLength(true), WithDefaultDigestEncoding(Base16))
	_, err := builder.WriteString("HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ntest")
	require.NoError(t, err)
	builder.AddWarcHeader(WarcRecordID, "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>")
	builder.AddWarcHeader(WarcDate, "2006-01-02T15:04:05Z")
	builder.AddWarcHeader(ContentType, "application/http;msgtype=response")
	builder.AddWarcHeader(ContentLength, "999") // deliberately wrong

	rec, v, err := builder.Build()
	require.NoError(t, err)
	defer func() { assert.NoError(t, rec.Close()) }()

	// Content length should have been fixed
	assert.NotEmpty(t, v) // should have the mismatch warning
	assert.Equal(t, "42", rec.WarcHeader().Get(ContentLength))
}

func Test_warcRecord_ValidateDigest_PayloadDigest_ErrFail(t *testing.T) {
	// Test payload digest mismatch with ErrFail — should fail the build
	builder := NewRecordBuilder(Response,
		WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrIgnore),
		WithFixDigest(false), WithAddMissingDigest(false),
		WithDefaultDigestEncoding(Base16))
	_, err := builder.WriteString("HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ntest")
	require.NoError(t, err)
	builder.AddWarcHeader(WarcRecordID, "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>")
	builder.AddWarcHeader(WarcDate, "2006-01-02T15:04:05Z")
	builder.AddWarcHeader(ContentType, "application/http;msgtype=response")
	builder.AddWarcHeader(ContentLength, "42")
	builder.AddWarcHeader(WarcPayloadDigest, "sha1:0000000000000000000000000000000000000000")

	_, _, err = builder.Build()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "payload")
}

func Test_warcRecord_ValidateDigest_ContentLengthMismatch_ErrFail(t *testing.T) {
	// Test content length mismatch with ErrFail — should fail the build
	builder := NewRecordBuilder(Response,
		WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrIgnore),
		WithFixDigest(false), WithAddMissingDigest(false),
		WithDefaultDigestEncoding(Base16))
	_, err := builder.WriteString("HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ntest")
	require.NoError(t, err)
	builder.AddWarcHeader(WarcRecordID, "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>")
	builder.AddWarcHeader(WarcDate, "2006-01-02T15:04:05Z")
	builder.AddWarcHeader(ContentType, "application/http;msgtype=response")
	builder.AddWarcHeader(ContentLength, "999") // deliberately wrong

	_, _, err = builder.Build()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "content length mismatch")
}

func Test_warcRecord_ValidateDigest_AddMissingPayloadDigest(t *testing.T) {
	// Test addMissingDigest option for payload digest
	builder := NewRecordBuilder(Response,
		WithSpecViolationPolicy(ErrWarn), WithSyntaxErrorPolicy(ErrWarn),
		WithFixDigest(false), WithAddMissingDigest(true),
		WithDefaultDigestEncoding(Base16))
	_, err := builder.WriteString("HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ntest")
	require.NoError(t, err)
	builder.AddWarcHeader(WarcRecordID, "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>")
	builder.AddWarcHeader(WarcDate, "2006-01-02T15:04:05Z")
	builder.AddWarcHeader(ContentType, "application/http;msgtype=response")
	builder.AddWarcHeader(ContentLength, "42")
	// Deliberately NOT adding WarcPayloadDigest

	rec, _, err := builder.Build()
	require.NoError(t, err)
	defer func() { assert.NoError(t, rec.Close()) }()

	// addMissingDigest should have added both block and payload digests
	assert.NotEmpty(t, rec.WarcHeader().Get(WarcBlockDigest))
	assert.NotEmpty(t, rec.WarcHeader().Get(WarcPayloadDigest))
}

func Test_warcRecord_parseBlock_BadDigestAlgorithm(t *testing.T) {
	// Test with unsupported digest algorithm in WarcBlockDigest
	rb := NewRecordBuilder(Response,
		WithSpecViolationPolicy(ErrIgnore), WithSyntaxErrorPolicy(ErrIgnore),
		WithFixDigest(false), WithAddMissingDigest(false))
	_, _ = rb.WriteString("HTTP/1.1 200 OK\r\n\r\n")
	rb.AddWarcHeader(WarcRecordID, "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>")
	rb.AddWarcHeader(WarcDate, "2006-01-02T15:04:05Z")
	rb.AddWarcHeader(ContentType, "application/http;msgtype=response")
	rb.AddWarcHeader(ContentLength, "19")
	rb.AddWarcHeader(WarcBlockDigest, "blake2:abc")

	_, _, err := rb.Build()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported digest algorithm")
}

func Test_warcRecord_parseBlock_BadPayloadDigestAlgorithm(t *testing.T) {
	rb := NewRecordBuilder(Response,
		WithSpecViolationPolicy(ErrIgnore), WithSyntaxErrorPolicy(ErrIgnore),
		WithFixDigest(false), WithAddMissingDigest(false))
	_, _ = rb.WriteString("HTTP/1.1 200 OK\r\n\r\n")
	rb.AddWarcHeader(WarcRecordID, "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>")
	rb.AddWarcHeader(WarcDate, "2006-01-02T15:04:05Z")
	rb.AddWarcHeader(ContentType, "application/http;msgtype=response")
	rb.AddWarcHeader(ContentLength, "19")
	rb.AddWarcHeader(WarcPayloadDigest, "blake2:abc")

	_, _, err := rb.Build()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported digest algorithm")
}
