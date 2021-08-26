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
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"testing"
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
					&nameValue{Name: WarcBlockDigest, Value: "sha1:B285747AD7CC57AA74BCE2E30B453C8D1CB71BA4"},
					&nameValue{Name: ContentLength, Value: "257"},
				},
				"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n"+
					"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n"+
					"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the content"),
			&RevisitRef{Profile: ProfileServerNotModified, TargetRecordId: "targetId"},
			want{
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcType, Value: "revisit"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:BF9D96D3F3F230CE8E2C6A3E5E1D51A81016B55E"},
					&nameValue{Name: WarcPayloadDigest, Value: "sha1:C37FFB221569C553A2476C22C7DAD429F3492977"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
					&nameValue{Name: ContentLength, Value: "238"},
					&nameValue{Name: WarcProfile, Value: ProfileServerNotModified},
					&nameValue{Name: WarcRefersTo, Value: "targetId"},
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
					&nameValue{Name: WarcBlockDigest, Value: "sha1:B285747AD7CC57AA74BCE2E30B453C8D1CB71BA4"},
					&nameValue{Name: WarcPayloadDigest, Value: "sha1:C37FFB221569C553A2476C22C7DAD429F3492977"},
					&nameValue{Name: ContentLength, Value: "257"},
				},
				"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n"+
					"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n"+
					"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the content"),
			&RevisitRef{Profile: ProfileIdenticalPayloadDigest, TargetRecordId: "targetId"},
			want{
				&WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcType, Value: "revisit"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:BF9D96D3F3F230CE8E2C6A3E5E1D51A81016B55E"},
					&nameValue{Name: WarcPayloadDigest, Value: "sha1:C37FFB221569C553A2476C22C7DAD429F3492977"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
					&nameValue{Name: ContentLength, Value: "238"},
					&nameValue{Name: WarcProfile, Value: ProfileIdenticalPayloadDigest},
					&nameValue{Name: WarcRefersTo, Value: "targetId"},
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
			&RevisitRef{Profile: ProfileIdenticalPayloadDigest, TargetRecordId: "targetId"},
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
			b, err := ioutil.ReadAll(r)
			assert.Nil(err)
			assert.Equal(tt.want.data, string(b))

			assert.True(got.Block().IsCached())
		})
	}
}

func Test_warcRecord_Merge(t *testing.T) {
	type want struct {
		recordType RecordType
		headers    *WarcFields
		data       string
		blockType  interface{}
		cached     bool
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
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
					&nameValue{Name: ContentLength, Value: "238"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:BF9D96D3F3F230CE8E2C6A3E5E1D51A81016B55E"},
					&nameValue{Name: WarcPayloadDigest, Value: "sha1:C37FFB221569C553A2476C22C7DAD429F3492977"},
					&nameValue{Name: WarcProfile, Value: ProfileServerNotModified},
					&nameValue{Name: WarcRefersTo, Value: "targetId"},
					&nameValue{Name: WarcTruncated, Value: "length"},
				},
				"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n"+
					"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n"+
					"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\n"),
			[]WarcRecord{createRecord1(Response,
				&WarcFields{
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
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcType, Value: "response"},
					&nameValue{Name: WarcBlockDigest, Value: "sha1:6E9D6B234FEEBBF1AB618707217E577C3B83448A"},
					&nameValue{Name: WarcPayloadDigest, Value: "sha1:C37FFB221569C553A2476C22C7DAD429F3492977"},
					&nameValue{Name: ContentType, Value: "application/http;msgtype=response"},
					&nameValue{Name: ContentLength, Value: "257"},
				},
				"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n" +
					"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n" +
					"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the content",
				&httpResponseBlock{},
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
			assert.IsType(tt.want.blockType, got.Block())
			r, err := got.Block().RawBytes()
			assert.Nil(err)
			b, err := ioutil.ReadAll(r)
			assert.Nil(err)
			assert.Equal(tt.want.data, string(b))

			assert.Equal(tt.want.cached, got.Block().IsCached())
		})
	}
}

func createRecord1(recordType RecordType, headers *WarcFields, data string) WarcRecord {
	rb := NewRecordBuilder(recordType, WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail), WithUnknownRecordTypePolicy(ErrIgnore), WithFixDigest(false))
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
