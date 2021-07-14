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
	"testing"
)

func TestValidateHeader(t *testing.T) {
	type args struct {
		header *WarcFields
		opts   *warcRecordOptions
	}
	tests := []struct {
		name    string
		args    args
		want    *WarcFields
		wantErr bool
	}{
		{
			"1",
			args{
				header: &WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
					&nameValue{Name: WarcFilename, Value: "temp-20170306040353.warc.gz"},
					&nameValue{Name: WarcType, Value: "warcinfo"},
					&nameValue{Name: ContentType, Value: "application/warc-fields"},
					&nameValue{Name: ContentLength, Value: "249"},
				},
				opts: newOptions(),
			},
			&WarcFields{
				&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
				&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
				&nameValue{Name: WarcFilename, Value: "temp-20170306040353.warc.gz"},
				&nameValue{Name: WarcType, Value: "warcinfo"},
				&nameValue{Name: ContentType, Value: "application/warc-fields"},
				&nameValue{Name: ContentLength, Value: "249"},
			},
			false,
		},
		{
			"2",
			args{
				header: &WarcFields{
					&nameValue{Name: WarcDate, Value: "2017-13-06T04:03:53Z"},
					&nameValue{Name: WarcRecordID, Value: "urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008"},
					&nameValue{Name: WarcFilename, Value: "temp-20170306040353.warc.gz"},
					&nameValue{Name: WarcType, Value: "warcinfoo"},
					&nameValue{Name: ContentType, Value: "application/warc-fields"},
					&nameValue{Name: ContentLength, Value: "249"},
				},
				opts: newOptions(),
			},
			&WarcFields{
				&nameValue{Name: WarcDate, Value: "2017-13-06T04:03:53Z"},
				&nameValue{Name: WarcRecordID, Value: "urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008"},
				&nameValue{Name: WarcFilename, Value: "temp-20170306040353.warc.gz"},
				&nameValue{Name: WarcType, Value: "warcinfoo"},
				&nameValue{Name: ContentType, Value: "application/warc-fields"},
				&nameValue{Name: ContentLength, Value: "249"},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validation := &Validation{}
			rt, err := validateHeader(tt.args.header, V1_1, validation, tt.args.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseWarcHeader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			fmt.Printf("Validation %v %v\n", rt, validation)
			// TODO: Fix test
			//if true {
			//	if !reflect.DeepEqual(got, tt.want) {
			//		t.Errorf("ParseWarcHeader() got:\n %v\nwant:\n %v", got, tt.want)
			//	}
			//}
		})
	}
}
