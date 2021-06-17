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
		opts *options
	}
	tests := []struct {
		name           string
		args           args
		want           *warcFields
		wantValidation *Validation
		wantErr        bool
	}{
		{
			"valid",
			args{
				data: "WARC-Date: 2017-03-06T04:03:53Z\r\n" +
					"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
					"WARC-Filename: temp-20170306040353.warc.gz\r\n" +
					"WARC-Type: warcinfo\r\n" +
					"Content-Type: application/warc-fields\r\n" +
					"Content-Length: 249\r\n\r\n",
				opts: NewOptions(WithSyntaxErrorPolicy(ErrIgnore)),
			},
			&warcFields{
				&NameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
				&NameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
				&NameValue{Name: WarcFilename, Value: "temp-20170306040353.warc.gz"},
				&NameValue{Name: WarcType, Value: "warcinfo"},
				&NameValue{Name: ContentType, Value: "application/warc-fields"},
				&NameValue{Name: ContentLength, Value: "249"},
			},
			&Validation{},
			false,
		},
		{
			"missing carriage return",
			args{
				data: "WARC-Date: 2017-03-06T04:03:53Z\n" +
					"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\n" +
					"WARC-Filename: temp-20170306040353.warc.gz\n" +
					"WARC-Type: warcinfo\n" +
					"Content-Type: application/warc-fields\n" +
					"Content-Length: 249\n\n",
				opts: NewOptions(WithSyntaxErrorPolicy(ErrIgnore)),
			},
			&warcFields{
				&NameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
				&NameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
				&NameValue{Name: WarcFilename, Value: "temp-20170306040353.warc.gz"},
				&NameValue{Name: WarcType, Value: "warcinfo"},
				&NameValue{Name: ContentType, Value: "application/warc-fields"},
				&NameValue{Name: ContentLength, Value: "249"},
			},
			&Validation{},
			false,
		},
		{
			"missing colon",
			args{
				data: "WARC-Date: 2017-03-06T04:03:53Z\r\n" +
					"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
					"WARC-Filename: temp-20170306040353.warc.gz\r\n" +
					"WARC-Type: warcinfo\r\n" +
					"Content-Type application/warc-fields\r\n" +
					"Content-Length: 249\r\n\r\n",
				opts: NewOptions(WithSyntaxErrorPolicy(ErrIgnore)),
			},
			&warcFields{
				&NameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
				&NameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
				&NameValue{Name: WarcFilename, Value: "temp-20170306040353.warc.gz"},
				&NameValue{Name: WarcType, Value: "warcinfo"},
				&NameValue{Name: ContentLength, Value: "249"},
			},
			&Validation{},
			false,
		},
		{
			"missing last line ending",
			args{
				data: "WARC-Date: 2017-03-06T04:03:53Z\r\n" +
					"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
					"WARC-Filename: temp-20170306040353.warc.gz\r\n" +
					"WARC-Type: warcinfo\r\n" +
					"Content-Type: application/warc-fields\r\n" +
					"Content-Length: 249",
				opts: NewOptions(WithSyntaxErrorPolicy(ErrIgnore)),
			},
			&warcFields{
				&NameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
				&NameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
				&NameValue{Name: WarcFilename, Value: "temp-20170306040353.warc.gz"},
				&NameValue{Name: WarcType, Value: "warcinfo"},
				&NameValue{Name: ContentType, Value: "application/warc-fields"},
				&NameValue{Name: ContentLength, Value: "249"},
			},
			&Validation{},
			false,
		},
		{
			"valid",
			args{
				data: "WARC-Date: 2017-03-06T04:03:53Z\r\n" +
					"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
					"WARC-Filename: temp-20170306040353.warc.gz\r\n" +
					"WARC-Type: warcinfo\r\n" +
					"Content-Type: application/warc-fields\r\n" +
					"Content-Length: 249\r\n\r\n",
				opts: NewOptions(WithSyntaxErrorPolicy(ErrWarn)),
			},
			&warcFields{
				&NameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
				&NameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
				&NameValue{Name: WarcFilename, Value: "temp-20170306040353.warc.gz"},
				&NameValue{Name: WarcType, Value: "warcinfo"},
				&NameValue{Name: ContentType, Value: "application/warc-fields"},
				&NameValue{Name: ContentLength, Value: "249"},
			},
			&Validation{},
			false,
		},
		{
			"missing carriage return",
			args{
				data: "WARC-Date: 2017-03-06T04:03:53Z\n" +
					"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\n" +
					"WARC-Filename: temp-20170306040353.warc.gz\n" +
					"WARC-Type: warcinfo\n" +
					"Content-Type: application/warc-fields\n" +
					"Content-Length: 249\n\n",
				opts: NewOptions(WithSyntaxErrorPolicy(ErrWarn)),
			},
			&warcFields{
				&NameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
				&NameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
				&NameValue{Name: WarcFilename, Value: "temp-20170306040353.warc.gz"},
				&NameValue{Name: WarcType, Value: "warcinfo"},
				&NameValue{Name: ContentType, Value: "application/warc-fields"},
				&NameValue{Name: ContentLength, Value: "249"},
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
			"missing colon",
			args{
				data: "WARC-Date: 2017-03-06T04:03:53Z\r\n" +
					"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
					"WARC-Filename: temp-20170306040353.warc.gz\r\n" +
					"WARC-Type: warcinfo\r\n" +
					"Content-Type application/warc-fields\r\n" +
					"Content-Length: 249\r\n\r\n",
				opts: NewOptions(WithSyntaxErrorPolicy(ErrWarn)),
			},
			&warcFields{
				&NameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
				&NameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
				&NameValue{Name: WarcFilename, Value: "temp-20170306040353.warc.gz"},
				&NameValue{Name: WarcType, Value: "warcinfo"},
				&NameValue{Name: ContentLength, Value: "249"},
			},
			&Validation{
				&SyntaxError{msg: "could not parse header line. Missing ':' in Content-Type application/warc-fields", line: 5},
			},
			false,
		},
		{
			"missing last line ending",
			args{
				data: "WARC-Date: 2017-03-06T04:03:53Z\r\n" +
					"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
					"WARC-Filename: temp-20170306040353.warc.gz\r\n" +
					"WARC-Type: warcinfo\r\n" +
					"Content-Type: application/warc-fields\r\n" +
					"Content-Length: 249",
				opts: NewOptions(WithSyntaxErrorPolicy(ErrWarn)),
			},
			&warcFields{
				&NameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
				&NameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
				&NameValue{Name: WarcFilename, Value: "temp-20170306040353.warc.gz"},
				&NameValue{Name: WarcType, Value: "warcinfo"},
				&NameValue{Name: ContentType, Value: "application/warc-fields"},
				&NameValue{Name: ContentLength, Value: "249"},
			},
			&Validation{
				&SyntaxError{msg: "missing newline", line: 6},
			},
			false,
		},
		{
			"valid",
			args{
				data: "WARC-Date: 2017-03-06T04:03:53Z\r\n" +
					"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
					"WARC-Filename: temp-20170306040353.warc.gz\r\n" +
					"WARC-Type: warcinfo\r\n" +
					"Content-Type: application/warc-fields\r\n" +
					"Content-Length: 249\r\n\r\n",
				opts: NewOptions(WithSyntaxErrorPolicy(ErrFail)),
			},
			&warcFields{
				&NameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
				&NameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
				&NameValue{Name: WarcFilename, Value: "temp-20170306040353.warc.gz"},
				&NameValue{Name: WarcType, Value: "warcinfo"},
				&NameValue{Name: ContentType, Value: "application/warc-fields"},
				&NameValue{Name: ContentLength, Value: "249"},
			},
			&Validation{},
			false,
		},
		{
			"missing carriage return",
			args{
				data: "WARC-Date: 2017-03-06T04:03:53Z\n" +
					"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\n" +
					"WARC-Filename: temp-20170306040353.warc.gz\n" +
					"WARC-Type: warcinfo\n" +
					"Content-Type: application/warc-fields\n" +
					"Content-Length: 249\n\n",
				opts: NewOptions(WithSyntaxErrorPolicy(ErrFail)),
			},
			nil,
			&Validation{},
			true,
		},
		{
			"missing colon",
			args{
				data: "WARC-Date: 2017-03-06T04:03:53Z\r\n" +
					"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
					"WARC-Filename: temp-20170306040353.warc.gz\r\n" +
					"WARC-Type: warcinfo\r\n" +
					"Content-Type application/warc-fields\r\n" +
					"Content-Length: 249\r\n\r\n",
				opts: NewOptions(WithSyntaxErrorPolicy(ErrFail)),
			},
			nil,
			&Validation{},
			true,
		},
		{
			"missing last line ending",
			args{
				data: "WARC-Date: 2017-03-06T04:03:53Z\r\n" +
					"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\n" +
					"WARC-Filename: temp-20170306040353.warc.gz\r\n" +
					"WARC-Type: warcinfo\r\n" +
					"Content-Type: application/warc-fields\r\n" +
					"Content-Length: 249",
				opts: NewOptions(WithSyntaxErrorPolicy(ErrFail)),
			},
			nil,
			&Validation{},
			true,
		},
	}
	for _, tt := range tests {
		var name string
		switch tt.args.opts.errSyntax {
		case ErrIgnore:
			name = "policy_ignore/" + tt.name
		case ErrWarn:
			name = "policy_warn/" + tt.name
		case ErrFail:
			name = "policy_fail/" + tt.name
		}
		t.Run(name, func(t *testing.T) {
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
