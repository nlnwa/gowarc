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

package warcfields

import (
	"bufio"
	"github.com/nlnwa/gowarc/warcoptions"
	"reflect"
	"strings"
	"testing"
)

func TestParseWarcHeader(t *testing.T) {
	type args struct {
		data string
		opts *warcoptions.WarcOptions
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
				data: "WARC-Date: 2017-03-06T04:03:53Z\r\n" +
					"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
					"WARC-Filename: temp-20170306040353.warc.gz\r\n" +
					"WARC-Type: warcinfo\r\n" +
					"Content-Type: application/warc-fields\r\n" +
					"Content-Length: 249\r\n\r\n",
				opts: &warcoptions.WarcOptions{Strict: false},
			},
			nil,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bufio.NewReader(strings.NewReader(tt.args.data))
			p := &Parser{Options: tt.args.opts}
			got, err := p.Parse(r)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseWarcHeader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseWarcHeader() got = %v, want %v", got, tt.want)
			}
		})
	}
}
