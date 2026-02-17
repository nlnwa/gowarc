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

func TestValidation_Error(t *testing.T) {
	tests := []struct {
		name   string
		errors []error
		want   string
	}{
		{
			name:   "empty validation",
			errors: []error{},
			want:   "",
		},
		{
			name:   "single error",
			errors: []error{errors.New("test error")},
			want:   "gowarc: Validation errors:\n  1: test error",
		},
		{
			name: "multiple errors",
			errors: []error{
				errors.New("first error"),
				errors.New("second error"),
				errors.New("third error"),
			},
			want: "gowarc: Validation errors:\n  1: first error\n  2: second error\n  3: third error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := Validation(tt.errors)
			got := v.Error()
			if got != tt.want {
				t.Errorf("Validation.Error() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestValidation_String(t *testing.T) {
	tests := []struct {
		name   string
		errors []error
		want   string
	}{
		{
			name:   "empty validation",
			errors: []error{},
			want:   "",
		},
		{
			name:   "single error",
			errors: []error{errors.New("validation failed")},
			want:   "gowarc: Validation errors:\n  1: validation failed",
		},
		{
			name: "two errors",
			errors: []error{
				errors.New("error one"),
				errors.New("error two"),
			},
			want: "gowarc: Validation errors:\n  1: error one\n  2: error two",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := Validation(tt.errors)
			got := v.String()
			if got != tt.want {
				t.Errorf("Validation.String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestValidation_Valid(t *testing.T) {
	tests := []struct {
		name   string
		errors []error
		want   bool
	}{
		{
			name:   "empty validation is valid",
			errors: []error{},
			want:   true,
		},
		{
			name:   "single error is not valid",
			errors: []error{errors.New("error")},
			want:   false,
		},
		{
			name: "multiple errors is not valid",
			errors: []error{
				errors.New("error 1"),
				errors.New("error 2"),
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := Validation(tt.errors)
			got := v.Valid()
			if got != tt.want {
				t.Errorf("Validation.Valid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidation_AddError(t *testing.T) {
	tests := []struct {
		name           string
		initial        []error
		toAdd          []error
		expectedCount  int
		expectedValid  bool
		expectedString string
	}{
		{
			name:           "add to empty",
			initial:        []error{},
			toAdd:          []error{errors.New("first")},
			expectedCount:  1,
			expectedValid:  false,
			expectedString: "gowarc: Validation errors:\n  1: first",
		},
		{
			name:           "add multiple to empty",
			initial:        []error{},
			toAdd:          []error{errors.New("first"), errors.New("second")},
			expectedCount:  2,
			expectedValid:  false,
			expectedString: "gowarc: Validation errors:\n  1: first\n  2: second",
		},
		{
			name:           "add to existing",
			initial:        []error{errors.New("existing")},
			toAdd:          []error{errors.New("new")},
			expectedCount:  2,
			expectedValid:  false,
			expectedString: "gowarc: Validation errors:\n  1: existing\n  2: new",
		},
		{
			name:          "add nothing",
			initial:       []error{errors.New("existing")},
			toAdd:         []error{},
			expectedCount: 1,
			expectedValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := Validation(tt.initial)
			for _, err := range tt.toAdd {
				v.addError(err)
			}

			if len(v) != tt.expectedCount {
				t.Errorf("Validation length = %d, want %d", len(v), tt.expectedCount)
			}

			if v.Valid() != tt.expectedValid {
				t.Errorf("Validation.Valid() = %v, want %v", v.Valid(), tt.expectedValid)
			}

			if tt.expectedString != "" {
				got := v.String()
				if got != tt.expectedString {
					t.Errorf("Validation.String() = %q, want %q", got, tt.expectedString)
				}
			}
		})
	}
}

func TestValidation_ErrorNumbering(t *testing.T) {
	// Test that errors are numbered correctly starting from 1
	v := Validation{}
	v.addError(errors.New("first"))
	v.addError(errors.New("second"))
	v.addError(errors.New("third"))

	errorStr := v.Error()

	// Check that numbering starts from 1
	if !strings.Contains(errorStr, "1: first") {
		t.Error("Error numbering should start with 1")
	}
	if !strings.Contains(errorStr, "2: second") {
		t.Error("Second error should be numbered 2")
	}
	if !strings.Contains(errorStr, "3: third") {
		t.Error("Third error should be numbered 3")
	}

	// Check that there's no 0:
	if strings.Contains(errorStr, "0:") {
		t.Error("Error numbering should not contain 0")
	}
}

func TestValidation_FormattingConsistency(t *testing.T) {
	// Ensure Error() and String() return the same value
	tests := []struct {
		name   string
		errors []error
	}{
		{
			name:   "empty",
			errors: []error{},
		},
		{
			name:   "single",
			errors: []error{errors.New("test")},
		},
		{
			name: "multiple",
			errors: []error{
				errors.New("error 1"),
				errors.New("error 2"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := Validation(tt.errors)
			errorStr := v.Error()
			stringStr := v.String()

			if errorStr != stringStr {
				t.Errorf("Error() and String() should return the same value\nError():  %q\nString(): %q", errorStr, stringStr)
			}
		})
	}
}

func TestValidation_ErrorWithSpecialCharacters(t *testing.T) {
	tests := []struct {
		name       string
		errorMsg   string
		shouldFail bool
	}{
		{
			name:       "newline in error",
			errorMsg:   "error with\nnewline",
			shouldFail: false,
		},
		{
			name:       "tab in error",
			errorMsg:   "error with\ttab",
			shouldFail: false,
		},
		{
			name:       "unicode in error",
			errorMsg:   "error with unicode: 日本語",
			shouldFail: false,
		},
		{
			name:       "empty error message",
			errorMsg:   "",
			shouldFail: false,
		},
		{
			name:       "very long error",
			errorMsg:   strings.Repeat("x", 1000),
			shouldFail: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := Validation{}
			v.addError(errors.New(tt.errorMsg))

			got := v.Error()

			if !strings.Contains(got, tt.errorMsg) {
				t.Errorf("Validation.Error() should contain original error message")
			}

			if !strings.Contains(got, "1:") {
				t.Error("Validation.Error() should contain error number")
			}
		})
	}
}

func TestValidation_AsErrorInterface(t *testing.T) {
	// Test that Validation can be used as an error
	var err error

	v := Validation{}
	v.addError(errors.New("test error"))

	err = &v

	if err == nil {
		t.Error("Validation should be usable as error interface")
	}

	errorMsg := err.Error()
	if !strings.Contains(errorMsg, "test error") {
		t.Errorf("Error interface should return proper message, got: %q", errorMsg)
	}
}

func TestValidation_MultipleTypes(t *testing.T) {
	// Test with different types of errors mixed together
	v := Validation{}
	v.addError(errors.New("simple error"))
	v.addError(&HeaderFieldError{fieldName: "test", msg: "field error"})
	v.addError(&SyntaxError{msg: "syntax error", line: 5})

	if len(v) != 3 {
		t.Errorf("Validation length = %d, want 3", len(v))
	}

	errorStr := v.Error()
	if !strings.Contains(errorStr, "simple error") {
		t.Error("Error string should contain all errors")
	}
}

func TestValidation_LargeNumberOfErrors(t *testing.T) {
	// Test with a large number of errors
	v := Validation{}
	count := 1000

	for i := 0; i < count; i++ {
		v.addError(errors.New("error"))
	}

	if len(v) != count {
		t.Errorf("Validation length = %d, want %d", len(v), count)
	}

	if v.Valid() {
		t.Error("Validation with errors should not be valid")
	}

	// Should not panic with large error list
	errorStr := v.Error()
	if errorStr == "" {
		t.Error("Error string should not be empty")
	}
}

func TestValidation_addError_Nil(t *testing.T) {
	v := Validation{}
	v.addError(nil)
	if len(v) != 0 {
		t.Errorf("addError(nil) should not add an error, got %d errors", len(v))
	}
	if !v.Valid() {
		t.Error("Validation with nil error should be valid")
	}
}
