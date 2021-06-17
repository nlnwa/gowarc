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

import "fmt"

// HeaderFieldError is used for violations of WARC header specification
type HeaderFieldError struct {
	field string
	msg   string
	line  int
}

func (e *HeaderFieldError) Error() string {
	return "gowarc: " + e.msg
}

// SyntaxError is used for syntactical errors like wrong line endings
type SyntaxError struct {
	msg     string
	line    int
	wrapped error
}

func NewSyntaxError(msg string, pos *position) *SyntaxError {
	return &SyntaxError{msg: msg, line: pos.lineNumber}
}

func NewWrappedSyntaxError(msg string, pos *position, wrapped error) *SyntaxError {
	return &SyntaxError{msg: msg, line: pos.lineNumber, wrapped: wrapped}
}

func (e *SyntaxError) Error() string {
	if e.line > 0 {
		return fmt.Sprintf("gowarc: %v at line %d", e.msg, e.line)
	} else {
		return fmt.Sprintf("gowarc: %v", e.msg)
	}
}

func (e *SyntaxError) Unwrap() error {
	return e.wrapped
}
