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
	"io"
	"net/http"
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
