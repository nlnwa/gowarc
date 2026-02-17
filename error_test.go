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
			wrapped: &SyntaxError{msg: "inner error", line: 2},
			want:    "gowarc: outer error at line 3: inner error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &SyntaxError{
				msg:     tt.msg,
				line:    tt.line,
				wrapped: tt.wrapped,
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
				msg:     "test",
				wrapped: tt.wrapped,
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
		pos          *position
		wrapped      error
		expectedLine int
	}{
		{
			name:         "with position",
			msg:          "test error",
			pos:          &position{lineNumber: 5},
			wrapped:      errors.New("inner"),
			expectedLine: 5,
		},
		{
			name:         "nil position with wrapped SyntaxError",
			msg:          "test error",
			pos:          nil,
			wrapped:      &SyntaxError{msg: "inner", line: 10},
			expectedLine: 10,
		},
		{
			name:         "nil position with non-SyntaxError",
			msg:          "test error",
			pos:          &position{lineNumber: 0},
			wrapped:      errors.New("inner"),
			expectedLine: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newWrappedSyntaxError(tt.msg, tt.pos, tt.wrapped)
			if e.line != tt.expectedLine {
				t.Errorf("newWrappedSyntaxError() line = %d, want %d", e.line, tt.expectedLine)
			}
			if e.msg != tt.msg {
				t.Errorf("newWrappedSyntaxError() msg = %q, want %q", e.msg, tt.msg)
			}
			if e.wrapped != tt.wrapped {
				t.Errorf("newWrappedSyntaxError() wrapped = %v, want %v", e.wrapped, tt.wrapped)
			}
		})
	}
}

func TestMultiErr_Error(t *testing.T) {
	tests := []struct {
		name   string
		errors []error
		want   string
	}{
		{
			name:   "empty",
			errors: []error{},
			want:   "",
		},
		{
			name:   "single error",
			errors: []error{errors.New("error 1")},
			want:   "error 1",
		},
		{
			name:   "two errors",
			errors: []error{errors.New("error 1"), errors.New("error 2")},
			want:   "[error 1, error 2]",
		},
		{
			name: "three errors",
			errors: []error{
				errors.New("first"),
				errors.New("second"),
				errors.New("third"),
			},
			want: "[first, second, third]",
		},
		{
			name: "errors with special characters",
			errors: []error{
				errors.New("error: with colon"),
				errors.New("error, with comma"),
			},
			want: "[error: with colon, error, with comma]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := multiErr(tt.errors)
			got := e.Error()
			if got != tt.want {
				t.Errorf("multiErr.Error() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPosition(t *testing.T) {
	tests := []struct {
		name         string
		initialLine  int
		increments   int
		expectedLine int
	}{
		{
			name:         "start at zero",
			initialLine:  0,
			increments:   1,
			expectedLine: 1,
		},
		{
			name:         "start at five",
			initialLine:  5,
			increments:   3,
			expectedLine: 8,
		},
		{
			name:         "no increments",
			initialLine:  10,
			increments:   0,
			expectedLine: 10,
		},
		{
			name:         "multiple increments",
			initialLine:  1,
			increments:   100,
			expectedLine: 101,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &position{lineNumber: tt.initialLine}
			for i := 0; i < tt.increments; i++ {
				p.incrLineNumber()
			}
			if p.lineNumber != tt.expectedLine {
				t.Errorf("position.lineNumber = %d, want %d", p.lineNumber, tt.expectedLine)
			}
		})
	}
}

func TestPosition_IncrLineNumber_ReturnsSelf(t *testing.T) {
	p := &position{lineNumber: 5}
	result := p.incrLineNumber()

	if result != p {
		t.Error("incrLineNumber() should return the same position instance")
	}

	if result.lineNumber != 6 {
		t.Errorf("incrLineNumber() lineNumber = %d, want 6", result.lineNumber)
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
			pos := &position{lineNumber: tt.lineNumber}
			e := newSyntaxError(tt.msg, pos)

			if e.msg != tt.expectedMsg {
				t.Errorf("newSyntaxError().msg = %q, want %q", e.msg, tt.expectedMsg)
			}
			if e.line != tt.expectedLine {
				t.Errorf("newSyntaxError().line = %d, want %d", e.line, tt.expectedLine)
			}
		})
	}
}

func TestSyntaxError_ErrorMessage_Format(t *testing.T) {
	// Test that the error message is properly constructed
	e := &SyntaxError{
		msg:  "test error",
		line: 42,
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
