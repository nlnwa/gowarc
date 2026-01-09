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
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseWarcFields(t *testing.T) {
	type args struct {
		data string
		opts *warcRecordOptions
	}
	tests := []struct {
		name           string
		args           args
		want           *WarcFields
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
			&WarcFields{
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
			&WarcFields{
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
			&WarcFields{
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
			&WarcFields{
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
			&WarcFields{
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
			&WarcFields{
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
			"policy_warn/marker_lf_only",
			args{
				data: "WARC-Date: 2017-03-06T04:03:53Z\r\n" +
					"Content-Length: 1\r\n" +
					"\n", // marker is LF only
				opts: newOptions(WithSyntaxErrorPolicy(ErrWarn)),
			},
			&WarcFields{
				&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
				&nameValue{Name: ContentLength, Value: "1"},
			},
			&Validation{}, // marker LF-only should not warn
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
			&WarcFields{
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
			&WarcFields{
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
			&WarcFields{
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
		{
			"policy_warn/empty_input",
			args{data: "", opts: newOptions(WithSyntaxErrorPolicy(ErrWarn))},
			&WarcFields{},
			&Validation{},
			false,
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
			assert.Equal(tt.wantValidation, validation, "%s", validation.String())
		})
	}
}

func TestParseWarcFields_DoesNotTreatEOHAsContinuationEvenIfNextByteIsSpace(t *testing.T) {
	// Regression test:
	// If the EOH marker (blank line) is read as a normal line (e.g. lookahead issues),
	// and the next byte in the stream is SP/HT, the parser must not treat that next
	// line as a continuation of the blank line and consume it.

	// One header, then EOH marker, then "payload" that starts with a space.
	in := []byte("WARC-Type: response\r\n\r\n continued-payload\r\n")

	r := bufio.NewReader(bytes.NewReader(in))
	validation := &Validation{}
	pos := &position{}

	p := &warcfieldsParser{
		Options: &warcRecordOptions{errSyntax: ErrWarn},
	}

	_, err := p.Parse(r, validation, pos)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	// The next line must still be available in the reader.
	rest, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll after Parse returned error: %v", err)
	}

	want := []byte(" continued-payload\r\n")
	if !bytes.Equal(rest, want) {
		t.Fatalf("remaining bytes mismatch:\nwant: %q\ngot : %q", want, rest)
	}
}

func TestParseWarcFields_DoesNotConsumePastEOHWhenNextLineStartsWithSpace(t *testing.T) {
	in := "WARC-Type: response\r\n\r\n continued-payload\r\n"
	r := bufio.NewReader(strings.NewReader(in))

	p := &warcfieldsParser{Options: newOptions(WithSyntaxErrorPolicy(ErrWarn))}
	validation := &Validation{}
	_, err := p.Parse(r, validation, &position{})
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	rest, _ := io.ReadAll(r)
	if string(rest) != " continued-payload\r\n" {
		t.Fatalf("expected payload to remain, got %q", string(rest))
	}
}

type failOnceAfterReader struct {
	r       io.Reader
	limit   int
	n       int
	errOnce error
	failed  bool
}

func (f *failOnceAfterReader) Read(p []byte) (int, error) {
	if f.n >= f.limit && !f.failed {
		f.failed = true
		return 0, f.errOnce
	}
	if f.n >= f.limit && f.failed {
		return 0, io.EOF
	}
	if len(p) > f.limit-f.n {
		p = p[:f.limit-f.n]
	}
	n, err := f.r.Read(p)
	f.n += n
	return n, err
}

func TestParseWarcFields_NonFatalPeekErrorIsIgnored(t *testing.T) {
	data := "WARC-Type: response\r\n\r\n"

	fr := &failOnceAfterReader{
		r:       strings.NewReader(data),
		limit:   len("WARC-Type: response\r\n"),
		errOnce: errors.New("boom"), // non-fatal
	}

	r := bufio.NewReader(fr)
	p := &warcfieldsParser{Options: newOptions(WithSyntaxErrorPolicy(ErrWarn))}
	validation := &Validation{}

	got, err := p.Parse(r, validation, &position{})
	if err != nil {
		t.Fatalf("Parse returned unexpected error: %v", err)
	}
	if len(*validation) != 0 {
		t.Fatalf("unexpected validation errors: %s", validation.String())
	}
	if got == nil || len(*got) != 1 {
		t.Fatalf("unexpected parsed fields: %#v", got)
	}
}
