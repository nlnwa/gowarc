/*
 * Copyright 2020 National Library of Norway.
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

package loader

import (
	"github.com/nlnwa/gowarc/pkg/gowarc"
	"reflect"
	"testing"
)

func TestLoader_Get(t *testing.T) {
	type fields struct {
		StorageRefResolver func(warcId string) (storageRef string, err error)
		StorageLoader      func(storageRef string) (record *gowarc.WarcRecord, err error)
		NoUnpack           bool
	}
	type args struct {
		warcId string
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		wantRecord *gowarc.WarcRecord
		wantErr    bool
	}{
		{
			"base",
			fields{mockStorageRefResolver, FileStorageLoader, false},
			args{"urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008"},
			nil,
			false,
		},
		{
			"base",
			fields{mockStorageRefResolver, FileStorageLoader, false},
			args{"urn:uuid:a9c51e3e-0221-11e7-bf66-0242ac120005"},
			nil,
			false,
		},
		{
			"base",
			fields{mockStorageRefResolver, FileStorageLoader, false},
			args{"urn:uuid:e9a0ee48-0221-11e7-adb1-0242ac120008"},
			nil,
			false,
		},
		{
			"base",
			fields{mockStorageRefResolver, FileStorageLoader, false},
			args{"urn:uuid:a9c5c23a-0221-11e7-8fe3-0242ac120007"},
			nil,
			false,
		},
		{
			"base",
			fields{mockStorageRefResolver, FileStorageLoader, false},
			args{"urn:uuid:e6e395ca-0221-11e7-a18d-0242ac120005"},
			nil,
			false,
		},
		{
			"base",
			fields{mockStorageRefResolver, FileStorageLoader, false},
			args{"urn:uuid:e6e41fea-0221-11e7-8fe3-0242ac120007"},
			nil,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Loader{
				StorageRefResolver: tt.fields.StorageRefResolver,
				StorageLoader:      tt.fields.StorageLoader,
				NoUnpack:           tt.fields.NoUnpack,
			}
			gotRecord, err := l.Get(tt.args.warcId)
			if (err != nil) != tt.wantErr {
				t.Errorf("Loader.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotRecord, tt.wantRecord) {
				t.Errorf("Loader.Get() = \n%v, want %v", gotRecord, tt.wantRecord)
			}
		})
	}
}

func mockStorageRefResolver(warcId string) (storageRef string, err error) {
	switch warcId {
	case "urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008":
		storageRef = "warcfile:../../testdata/example.warc:0"
	case "urn:uuid:e9a0ee48-0221-11e7-adb1-0242ac120008":
		storageRef = "warcfile:../../testdata/example.warc:488"
	case "urn:uuid:a9c51e3e-0221-11e7-bf66-0242ac120005":
		storageRef = "warcfile:../../testdata/example.warc:1197"
	case "urn:uuid:a9c5c23a-0221-11e7-8fe3-0242ac120007":
		storageRef = "warcfile:../../testdata/example.warc:2566"
	case "urn:uuid:e6e395ca-0221-11e7-a18d-0242ac120005":
		storageRef = "warcfile:../../testdata/example.warc:3370"
	case "urn:uuid:e6e41fea-0221-11e7-8fe3-0242ac120007":
		storageRef = "warcfile:../../testdata/example.warc:4316"
	}
	return
}
