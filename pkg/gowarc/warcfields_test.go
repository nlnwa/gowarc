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

package gowarc

import (
	"bufio"
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func Test_parseLine(t *testing.T) {
	opts := &WarcReaderOpts{Strict: false}
	wfp := newWarcfieldParser(opts)
	tests := []struct {
		name      string
		line      []byte
		wantName  string
		wantValue string
		wantErr   error
	}{
		{"unknown", []byte("foo:  bar"), "foo", "bar", nil},
		{"known", []byte("WARC-Type:  response"), "WARC-Type", "response", nil},
		{"case", []byte("warc-Type:  response"), "warc-Type", "response", nil},
		{"nonfield", []byte("WARC/1.0  "), "", "", errors.New("could not parse header line. Missing ':' in WARC/1.0")},
		{"version", []byte("WARC-Concurrent-To:foo\n"), "WARC-Concurrent-To", "foo", nil},
		{"utf8", []byte("WARC-Target-URI:Hello, 世界\n"), "WARC-Target-URI", "Hello, 世界", nil},
		{"encoded", []byte("WARC-Target-URI: =?utf-8?q?Hello,_=E4=B8=96=E7=95=8C?=\n"), "WARC-Target-URI", "Hello, 世界", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotName, gotValue, gotErr := wfp.parseLine(tt.line)
			if gotName != tt.wantName {
				t.Errorf("parseLine() gotName = %v, want %v", gotName, tt.wantName)
			}
			if gotValue != tt.wantValue {
				t.Errorf("parseLine() gotValue = %v, want %v", gotValue, tt.wantValue)
			}
			if !reflect.DeepEqual(gotErr, tt.wantErr) {
				t.Errorf("parseLine() gotError = %v, want %v", gotErr, tt.wantErr)
			}
		})
	}
}

func Test_parseWarcHeader(t *testing.T) {
	opts := &WarcReaderOpts{Strict: true}
	wfp := newWarcfieldParser(opts)
	tests := []struct {
		name    string
		args    *bufio.Reader
		want    WarcFields
		wantErr error
	}{
		{
			"ok",
			newReader("WARC-Type:  response\r\n\r\n"),
			createExpectedWarcFields("response"),
			nil,
		},
		{
			"missingCR",
			newReader("WARC-Type:  response\n\r\n"),
			NewWarcFields(),
			errors.New("missing carriage return on line 'WARC-Type:  response'"),
		},
		{
			"multiline",
			newReader("WARC-Type: \r\n resp\r\n onse\r\n\r\n"),
			createExpectedWarcFields("resp onse"),
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotErr := wfp.parse(tt.args)
			if !reflect.DeepEqual(gotErr, tt.wantErr) {
				t.Errorf("parse() error = %v, wantErr %v", gotErr, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parse() = %v, want %v %v", got, tt.want, tt.name)
			}
		})
	}
}

func newReader(buf string) *bufio.Reader {
	return bufio.NewReader(bytes.NewBuffer([]byte(buf)))
}

func createExpectedWarcFields(warcType string) WarcFields {
	h := NewWarcFields()
	h.Add("WARC-Type", warcType)
	return h
}
