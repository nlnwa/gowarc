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
	"strings"
	"testing"
)

func TestParseWarcFields(t *testing.T) {
	type args struct {
		data string
		opts *warcRecordOptions
	}
	tests := []struct {
		name           string
		args           args
		want           *warcFields
		wantValidation *Validation
		wantErr        bool
	}{
		{
			"policy_ignore/valid",
			args{
				data: "WARC-Date: 2017-03-06T04:03:53Z\r\n" +
					"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
					"WARC-Filename: temp-20170306040353.warc.gz\r\n" +
					"WARC-Type: warcinfo\r\n" +
					"Content-Type: application/warc-fields\r\n" +
					"Content-Length: 249\r\n\r\n",
				opts: newOptions(WithSyntaxErrorPolicy(ErrIgnore)),
			},
			&warcFields{
				&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
				&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
				&nameValue{Name: WarcFilename, Value: "temp-20170306040353.warc.gz"},
				&nameValue{Name: WarcType, Value: "warcinfo"},
				&nameValue{Name: ContentType, Value: "application/warc-fields"},
				&nameValue{Name: ContentLength, Value: "249"},
			},
			&Validation{},
			false,
		},
		{
			"policy_ignore/missing carriage return",
			args{
				data: "WARC-Date: 2017-03-06T04:03:53Z\n" +
					"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\n" +
					"WARC-Filename: temp-20170306040353.warc.gz\n" +
					"WARC-Type: warcinfo\n" +
					"Content-Type: application/warc-fields\n" +
					"Content-Length: 249\n\n",
				opts: newOptions(WithSyntaxErrorPolicy(ErrIgnore)),
			},
			&warcFields{
				&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
				&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
				&nameValue{Name: WarcFilename, Value: "temp-20170306040353.warc.gz"},
				&nameValue{Name: WarcType, Value: "warcinfo"},
				&nameValue{Name: ContentType, Value: "application/warc-fields"},
				&nameValue{Name: ContentLength, Value: "249"},
			},
			&Validation{},
			false,
		},
		{
			"policy_ignore/missing colon",
			args{
				data: "WARC-Date: 2017-03-06T04:03:53Z\r\n" +
					"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
					"WARC-Filename: temp-20170306040353.warc.gz\r\n" +
					"WARC-Type: warcinfo\r\n" +
					"Content-Type application/warc-fields\r\n" +
					"Content-Length: 249\r\n\r\n",
				opts: newOptions(WithSyntaxErrorPolicy(ErrIgnore)),
			},
			&warcFields{
				&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
				&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
				&nameValue{Name: WarcFilename, Value: "temp-20170306040353.warc.gz"},
				&nameValue{Name: WarcType, Value: "warcinfo"},
				&nameValue{Name: ContentLength, Value: "249"},
			},
			&Validation{},
			false,
		},
		{
			"policy_ignore/missing last line ending",
			args{
				data: "WARC-Date: 2017-03-06T04:03:53Z\r\n" +
					"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
					"WARC-Filename: temp-20170306040353.warc.gz\r\n" +
					"WARC-Type: warcinfo\r\n" +
					"Content-Type: application/warc-fields\r\n" +
					"Content-Length: 249",
				opts: newOptions(WithSyntaxErrorPolicy(ErrIgnore)),
			},
			&warcFields{
				&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
				&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
				&nameValue{Name: WarcFilename, Value: "temp-20170306040353.warc.gz"},
				&nameValue{Name: WarcType, Value: "warcinfo"},
				&nameValue{Name: ContentType, Value: "application/warc-fields"},
				&nameValue{Name: ContentLength, Value: "249"},
			},
			&Validation{},
			false,
		},
		{
			"policy_warn/valid",
			args{
				data: "WARC-Date: 2017-03-06T04:03:53Z\r\n" +
					"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
					"WARC-Filename: temp-20170306040353.warc.gz\r\n" +
					"WARC-Type: warcinfo\r\n" +
					"Content-Type: application/warc-fields\r\n" +
					"Content-Length: 249\r\n\r\n",
				opts: newOptions(WithSyntaxErrorPolicy(ErrWarn)),
			},
			&warcFields{
				&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
				&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
				&nameValue{Name: WarcFilename, Value: "temp-20170306040353.warc.gz"},
				&nameValue{Name: WarcType, Value: "warcinfo"},
				&nameValue{Name: ContentType, Value: "application/warc-fields"},
				&nameValue{Name: ContentLength, Value: "249"},
			},
			&Validation{},
			false,
		},
		{
			"policy_warn/missing carriage return",
			args{
				data: "WARC-Date: 2017-03-06T04:03:53Z\n" +
					"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\n" +
					"WARC-Filename: temp-20170306040353.warc.gz\n" +
					"WARC-Type: warcinfo\n" +
					"Content-Type: application/warc-fields\n" +
					"Content-Length: 249\n\n",
				opts: newOptions(WithSyntaxErrorPolicy(ErrWarn)),
			},
			&warcFields{
				&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
				&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
				&nameValue{Name: WarcFilename, Value: "temp-20170306040353.warc.gz"},
				&nameValue{Name: WarcType, Value: "warcinfo"},
				&nameValue{Name: ContentType, Value: "application/warc-fields"},
				&nameValue{Name: ContentLength, Value: "249"},
			},
			&Validation{
				&SyntaxError{msg: "missing carriage return", line: 1},
				&SyntaxError{msg: "missing carriage return", line: 2},
				&SyntaxError{msg: "missing carriage return", line: 3},
				&SyntaxError{msg: "missing carriage return", line: 4},
				&SyntaxError{msg: "missing carriage return", line: 5},
				&SyntaxError{msg: "missing carriage return", line: 6},
			},
			false,
		},
		{
			"policy_warn/missing colon",
			args{
				data: "WARC-Date: 2017-03-06T04:03:53Z\r\n" +
					"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
					"WARC-Filename: temp-20170306040353.warc.gz\r\n" +
					"WARC-Type: warcinfo\r\n" +
					"Content-Type application/warc-fields\r\n" +
					"Content-Length: 249\r\n\r\n",
				opts: newOptions(WithSyntaxErrorPolicy(ErrWarn)),
			},
			&warcFields{
				&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
				&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
				&nameValue{Name: WarcFilename, Value: "temp-20170306040353.warc.gz"},
				&nameValue{Name: WarcType, Value: "warcinfo"},
				&nameValue{Name: ContentLength, Value: "249"},
			},
			&Validation{
				&SyntaxError{msg: "could not parse header line. Missing ':' in Content-Type application/warc-fields", line: 5},
			},
			false,
		},
		{
			"policy_warn/missing last line ending",
			args{
				data: "WARC-Date: 2017-03-06T04:03:53Z\r\n" +
					"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
					"WARC-Filename: temp-20170306040353.warc.gz\r\n" +
					"WARC-Type: warcinfo\r\n" +
					"Content-Type: application/warc-fields\r\n" +
					"Content-Length: 249",
				opts: newOptions(WithSyntaxErrorPolicy(ErrWarn)),
			},
			&warcFields{
				&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
				&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
				&nameValue{Name: WarcFilename, Value: "temp-20170306040353.warc.gz"},
				&nameValue{Name: WarcType, Value: "warcinfo"},
				&nameValue{Name: ContentType, Value: "application/warc-fields"},
				&nameValue{Name: ContentLength, Value: "249"},
			},
			&Validation{
				&SyntaxError{msg: "missing newline", line: 6},
			},
			false,
		},
		{
			"policy_fail/valid",
			args{
				data: "WARC-Date: 2017-03-06T04:03:53Z\r\n" +
					"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
					"WARC-Filename: temp-20170306040353.warc.gz\r\n" +
					"WARC-Type: warcinfo\r\n" +
					"Content-Type: application/warc-fields\r\n" +
					"Content-Length: 249\r\n\r\n",
				opts: newOptions(WithSyntaxErrorPolicy(ErrFail)),
			},
			&warcFields{
				&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
				&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
				&nameValue{Name: WarcFilename, Value: "temp-20170306040353.warc.gz"},
				&nameValue{Name: WarcType, Value: "warcinfo"},
				&nameValue{Name: ContentType, Value: "application/warc-fields"},
				&nameValue{Name: ContentLength, Value: "249"},
			},
			&Validation{},
			false,
		},
		{
			"policy_fail/missing carriage return",
			args{
				data: "WARC-Date: 2017-03-06T04:03:53Z\n" +
					"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\n" +
					"WARC-Filename: temp-20170306040353.warc.gz\n" +
					"WARC-Type: warcinfo\n" +
					"Content-Type: application/warc-fields\n" +
					"Content-Length: 249\n\n",
				opts: newOptions(WithSyntaxErrorPolicy(ErrFail)),
			},
			nil,
			&Validation{},
			true,
		},
		{
			"policy_fail/missing colon",
			args{
				data: "WARC-Date: 2017-03-06T04:03:53Z\r\n" +
					"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
					"WARC-Filename: temp-20170306040353.warc.gz\r\n" +
					"WARC-Type: warcinfo\r\n" +
					"Content-Type application/warc-fields\r\n" +
					"Content-Length: 249\r\n\r\n",
				opts: newOptions(WithSyntaxErrorPolicy(ErrFail)),
			},
			nil,
			&Validation{},
			true,
		},
		{
			"policy_fail/missing last line ending",
			args{
				data: "WARC-Date: 2017-03-06T04:03:53Z\r\n" +
					"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
					"WARC-Filename: temp-20170306040353.warc.gz\r\n" +
					"WARC-Type: warcinfo\r\n" +
					"Content-Type: application/warc-fields\r\n" +
					"Content-Length: 249",
				opts: newOptions(WithSyntaxErrorPolicy(ErrFail)),
			},
			nil,
			&Validation{},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bufio.NewReader(strings.NewReader(tt.args.data))
			p := &warcfieldsParser{Options: tt.args.opts}
			validation := &Validation{}
			got, err := p.Parse(r, validation, &position{})

			assert := assert.New(t)
			if tt.wantErr {
				assert.Error(err)
			} else {
				assert.NoError(err)
			}
			assert.Equal(tt.want, got)
			assert.Equal(tt.wantValidation, validation)
		})
	}
}
