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
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestHeaderFieldError_Error(t *testing.T) {
	tests := []struct {
		name      string
		fieldName string
		msg       string
		want      string
	}{
		{
			name:      "with field name",
			fieldName: "WARC-Date",
			msg:       "invalid date format",
			want:      "gowarc: invalid date format at header WARC-Date",
		},
		{
			name:      "without field name",
			fieldName: "",
			msg:       "missing required field",
			want:      "gowarc: missing required field",
		},
		{
			name:      "empty message",
			fieldName: "Content-Type",
			msg:       "",
			want:      "gowarc:  at header Content-Type",
		},
		{
			name:      "both empty",
			fieldName: "",
			msg:       "",
			want:      "gowarc: ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newHeaderFieldError(tt.fieldName, tt.msg)
			got := e.Error()
			if got != tt.want {
				t.Errorf("HeaderFieldError.Error() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestHeaderFieldErrorf(t *testing.T) {
	tests := []struct {
		name      string
		fieldName string
		format    string
		args      []any
		want      string
	}{
		{
			name:      "formatted message",
			fieldName: "WARC-Record-ID",
			format:    "invalid UUID format: %s",
			args:      []any{"abc-123"},
			want:      "gowarc: invalid UUID format: abc-123 at header WARC-Record-ID",
		},
		{
			name:      "multiple format args",
			fieldName: "Content-Length",
			format:    "expected %d, got %d",
			args:      []any{100, 200},
			want:      "gowarc: expected 100, got 200 at header Content-Length",
		},
		{
			name:      "no format args",
			fieldName: "WARC-Type",
			format:    "invalid type",
			args:      []any{},
			want:      "gowarc: invalid type at header WARC-Type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newHeaderFieldErrorf(tt.fieldName, tt.format, tt.args...)
			got := e.Error()
			if got != tt.want {
				t.Errorf("newHeaderFieldErrorf().Error() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSyntaxError_Error(t *testing.T) {
	tests := []struct {
		name    string
		msg     string
		line    int
		wrapped error
		want    string
	}{
		{
			name: "simple syntax error",
			msg:  "missing colon",
			line: 5,
			want: "gowarc: missing colon at line 5",
		},
		{
			name: "no line number",
			msg:  "invalid format",
			line: 0,
			want: "gowarc: invalid format",
		},
		{
			name:    "with wrapped error",
			msg:     "header parsing failed",
			line:    10,
			wrapped: errors.New("unexpected EOF"),
			want:    "gowarc: header parsing failed at line 10: unexpected EOF",
		},
		{
			name:    "with wrapped SyntaxError",
			msg:     "outer error",
			line:    3,
			wrapped: &SyntaxError{Msg: "inner error", Line: 2},
			want:    "gowarc: outer error at line 3: inner error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &SyntaxError{
				Msg:     tt.msg,
				Line:    tt.line,
				Wrapped: tt.wrapped,
			}
			got := e.Error()
			if got != tt.want {
				t.Errorf("SyntaxError.Error() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSyntaxError_Unwrap(t *testing.T) {
	tests := []struct {
		name    string
		wrapped error
	}{
		{
			name:    "with wrapped error",
			wrapped: errors.New("test error"),
		},
		{
			name:    "without wrapped error",
			wrapped: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &SyntaxError{
				Msg:     "test",
				Wrapped: tt.wrapped,
			}
			got := e.Unwrap()
			if got != tt.wrapped {
				t.Errorf("SyntaxError.Unwrap() = %v, want %v", got, tt.wrapped)
			}
		})
	}
}

func TestNewWrappedSyntaxError(t *testing.T) {
	tests := []struct {
		name         string
		msg          string
		line         int
		wrapped      error
		expectedLine int
		useLine      bool
	}{
		{
			name:         "with line number",
			msg:          "test error",
			line:         5,
			wrapped:      errors.New("inner"),
			expectedLine: 5,
			useLine:      true,
		},
		{
			name:         "no line with wrapped SyntaxError",
			msg:          "test error",
			wrapped:      &SyntaxError{Msg: "inner", Line: 10},
			expectedLine: 10,
			useLine:      false,
		},
		{
			name:         "zero line with non-SyntaxError",
			msg:          "test error",
			wrapped:      errors.New("inner"),
			expectedLine: 0,
			useLine:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var e *SyntaxError
			if tt.useLine {
				e = newWrappedSyntaxErrorAtLine(tt.msg, tt.line, tt.wrapped)
			} else {
				e = newWrappedSyntaxError(tt.msg, tt.wrapped)
			}
			if e.Line != tt.expectedLine {
				t.Errorf("newWrappedSyntaxError() line = %d, want %d", e.Line, tt.expectedLine)
			}
			if e.Msg != tt.msg {
				t.Errorf("newWrappedSyntaxError() msg = %q, want %q", e.Msg, tt.msg)
			}
			if e.Wrapped != tt.wrapped {
				t.Errorf("newWrappedSyntaxError() wrapped = %v, want %v", e.Wrapped, tt.wrapped)
			}
		})
	}
}

func TestNewSyntaxError(t *testing.T) {
	tests := []struct {
		name         string
		msg          string
		lineNumber   int
		expectedMsg  string
		expectedLine int
	}{
		{
			name:         "basic syntax error",
			msg:          "invalid syntax",
			lineNumber:   1,
			expectedMsg:  "invalid syntax",
			expectedLine: 1,
		},
		{
			name:         "zero line number",
			msg:          "test",
			lineNumber:   0,
			expectedMsg:  "test",
			expectedLine: 0,
		},
		{
			name:         "large line number",
			msg:          "error at end of file",
			lineNumber:   999999,
			expectedMsg:  "error at end of file",
			expectedLine: 999999,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newSyntaxErrorAtLine(tt.msg, tt.lineNumber)

			if e.Msg != tt.expectedMsg {
				t.Errorf("newSyntaxError().msg = %q, want %q", e.Msg, tt.expectedMsg)
			}
			if e.Line != tt.expectedLine {
				t.Errorf("newSyntaxError().line = %d, want %d", e.Line, tt.expectedLine)
			}
		})
	}
}

func TestSyntaxError_ErrorMessage_Format(t *testing.T) {
	// Test that the error message is properly constructed
	e := &SyntaxError{
		Msg:  "test error",
		Line: 42,
	}

	got := e.Error()

	if !strings.Contains(got, "gowarc:") {
		t.Error("error message should contain 'gowarc:' prefix")
	}
	if !strings.Contains(got, "test error") {
		t.Error("error message should contain the original message")
	}
	if !strings.Contains(got, "42") {
		t.Error("error message should contain line number")
	}
	if !strings.Contains(got, "at line") {
		t.Error("error message should contain 'at line' text")
	}
}

func TestDigestError_Error(t *testing.T) {
	e := &DigestError{
		Algorithm: "sha1",
		Expected:  "AAAA",
		Computed:  "BBBB",
	}
	got := e.Error()
	if got != "wrong digest: expected sha1:AAAA, computed: sha1:BBBB" {
		t.Errorf("DigestError.Error() = %q", got)
	}
}

func TestDigestError_ErrorsAs(t *testing.T) {
	// Simulate the wrapping pattern used in ValidateDigest: fmt.Errorf("block: %w", digestErr)
	inner := &DigestError{Algorithm: "sha256", Expected: "abc", Computed: "def"}
	wrapped := fmt.Errorf("block: %w", inner)

	var de *DigestError
	if !errors.As(wrapped, &de) {
		t.Fatal("errors.As should match *DigestError through wrapping")
	}
	if de.Algorithm != "sha256" || de.Expected != "abc" || de.Computed != "def" {
		t.Errorf("unexpected fields: %+v", de)
	}
}

func TestContentLengthError_Error(t *testing.T) {
	e := &ContentLengthError{Expected: 100, Actual: 200}
	got := e.Error()
	if got != "content length mismatch: header 100, actual 200" {
		t.Errorf("ContentLengthError.Error() = %q", got)
	}
}

func TestContentLengthError_ErrorsAs(t *testing.T) {
	var err error = &ContentLengthError{Expected: 18, Actual: 21}
	var cle *ContentLengthError
	if !errors.As(err, &cle) {
		t.Fatal("errors.As should match *ContentLengthError")
	}
	if cle.Expected != 18 || cle.Actual != 21 {
		t.Errorf("unexpected fields: %+v", cle)
	}
}

func TestHeaderFieldError_ErrorsAs(t *testing.T) {
	var err error = newHeaderFieldError("WARC-Date", "invalid format")
	var hfe *HeaderFieldError
	if !errors.As(err, &hfe) {
		t.Fatal("errors.As should match *HeaderFieldError")
	}
	if hfe.FieldName != "WARC-Date" || hfe.Msg != "invalid format" {
		t.Errorf("unexpected fields: %+v", hfe)
	}
}

func TestSyntaxError_ErrorsAs(t *testing.T) {
	inner := newSyntaxErrorAtLine("missing CR", 5)
	outer := newWrappedSyntaxError("parse failed", inner)
	var se *SyntaxError
	if !errors.As(outer, &se) {
		t.Fatal("errors.As should match *SyntaxError")
	}
	if se.Msg != "parse failed" || se.Line != 5 {
		t.Errorf("unexpected fields: %+v", se)
	}
	// Unwrap should give us the inner error
	var inner2 *SyntaxError
	if !errors.As(se.Wrapped, &inner2) {
		t.Fatal("inner should also be *SyntaxError")
	}
	if inner2.Msg != "missing CR" {
		t.Errorf("inner msg = %q", inner2.Msg)
	}
}
