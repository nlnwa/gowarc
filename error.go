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
)

// Sentinel errors for common conditions.
// These can be matched with [errors.Is].
var (
	// ErrNotRevisitRecord is returned when a revisit-only operation is attempted on a non-revisit record.
	ErrNotRevisitRecord = errors.New("gowarc: not a revisit record")

	// ErrIsRevisitRecord is returned when attempting to create a revisit reference from a revisit record.
	ErrIsRevisitRecord = errors.New("gowarc: cannot reference a revisit record")

	// ErrUnknownRevisitProfile is returned when a revisit record references an unrecognized profile URI.
	ErrUnknownRevisitProfile = errors.New("gowarc: unknown revisit profile")

	// ErrMissingPayloadDigest is returned when the identical-payload-digest profile is used but no payload digest is available.
	ErrMissingPayloadDigest = errors.New("gowarc: payload digest required for identical-payload-digest profile")

	// ErrMergeRequiresOneRecord is returned when Merge is called with zero or more than one referenced record.
	ErrMergeRequiresOneRecord = errors.New("gowarc: revisit merge requires exactly one referenced record")

	// ErrMergeNotSupported is returned when merging is attempted on a record type that does not support it.
	ErrMergeNotSupported = errors.New("gowarc: merging is only possible for revisit records or segmented records")

	// ErrMergeSegmentedNotImplemented is returned when merging of segmented records is attempted.
	ErrMergeSegmentedNotImplemented = errors.New("gowarc: merging of segmented records is not implemented")

	// ErrMergeWrongBlockType is returned when a revisit record's block type is incompatible with merging
	// (typically because the record was parsed with SkipParseBlock).
	ErrMergeWrongBlockType = errors.New("gowarc: revisit block type incompatible with merge; record must be parsed with SkipParseBlock=false")

	// ErrMergeUnsupportedBlock is returned when merging a revisit with a non-HTTP block type.
	ErrMergeUnsupportedBlock = errors.New("gowarc: merge only supports http request and response blocks")

	// ErrUnsupportedDigestAlgorithm is returned when an unrecognized digest algorithm is encountered.
	ErrUnsupportedDigestAlgorithm = errors.New("gowarc: unsupported digest algorithm")
)

// HeaderFieldError is used for violations of WARC header specification
type HeaderFieldError struct {
	fieldName string
	msg       string
}

func newHeaderFieldError(fieldName string, msg string) *HeaderFieldError {
	return &HeaderFieldError{fieldName: fieldName, msg: msg}
}

func newHeaderFieldErrorf(fieldName string, msg string, param ...any) *HeaderFieldError {
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

func newSyntaxError(msg string) *SyntaxError {
	return &SyntaxError{msg: msg}
}

func newSyntaxErrorAtLine(msg string, line int) *SyntaxError {
	return &SyntaxError{msg: msg, line: line}
}

func newWrappedSyntaxError(msg string, wrapped error) *SyntaxError {
	e := &SyntaxError{msg: msg, wrapped: wrapped}
	if se, ok := wrapped.(*SyntaxError); ok && se.line > 0 {
		e.line = se.line
	}
	return e
}

func newWrappedSyntaxErrorAtLine(msg string, line int, wrapped error) *SyntaxError {
	return &SyntaxError{msg: msg, line: line, wrapped: wrapped}
}

func (e *SyntaxError) Error() string {
	s := "gowarc: " + e.msg
	if e.line > 0 {
		s += fmt.Sprintf(" at line %d", e.line)
	}
	if e.wrapped != nil {
		if v, ok := e.wrapped.(*SyntaxError); ok {
			s += ": " + v.msg
		} else {
			s += ": " + e.wrapped.Error()
		}
	}
	return s
}

func (e *SyntaxError) Unwrap() error {
	return e.wrapped
}
