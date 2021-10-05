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
	"strings"
)

// HeaderFieldError is used for violations of WARC header specification
type HeaderFieldError struct {
	fieldName string
	msg       string
}

func newHeaderFieldError(fieldName string, msg string) *HeaderFieldError {
	return &HeaderFieldError{fieldName: fieldName, msg: msg}
}

func newHeaderFieldErrorf(fieldName string, msg string, param ...interface{}) *HeaderFieldError {
	return &HeaderFieldError{fieldName: fieldName, msg: fmt.Sprintf(msg, param...)}
}

func (e *HeaderFieldError) Error() string {
	if e.fieldName != "" {
		return fmt.Sprintf("gowarc: %s at header %s", e.msg, e.fieldName)
	} else {
		return fmt.Sprintf("gowarc: %s", e.msg)
	}
}

// SyntaxError is used for syntactical errors like wrong line endings
type SyntaxError struct {
	msg     string
	line    int
	wrapped error
}

func newSyntaxError(msg string, pos *position) *SyntaxError {
	return &SyntaxError{msg: msg, line: pos.lineNumber}
}

func newWrappedSyntaxError(msg string, pos *position, wrapped error) *SyntaxError {
	return &SyntaxError{msg: msg, line: pos.lineNumber, wrapped: wrapped}
}

func (e *SyntaxError) Error() string {
	if e.line > 0 {
		return fmt.Sprintf("gowarc: %s at line %d", e.msg, e.line)
	} else {
		return fmt.Sprintf("gowarc: %s", e.msg)
	}
}

func (e *SyntaxError) Unwrap() error {
	return e.wrapped
}

type multiErr []error

func (e multiErr) Error() string {
	switch len(e) {

	case 0:
		return ""

	case 1:
		return e[0].Error()
	}

	const (
		start = "["
		sep   = ", "
		end   = "]"
	)

	n := len(start) + len(end) + (len(sep) * (len(e) - 1))
	for i := 0; i < len(e); i++ {
		n += len(e[i].Error())
	}

	var b strings.Builder
	b.Grow(n)
	b.WriteString(start)
	b.WriteString(e[0].Error())
	for _, s := range e[1:] {
		b.WriteString(sep)
		b.WriteString(s.Error())
	}
	b.WriteString(end)
	return b.String()
}
