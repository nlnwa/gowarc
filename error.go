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

	// ErrNoRecord is returned by [Unmarshaler.Unmarshal] and [WarcFileReader.Next]
	// when the reader scans past one or more bytes without finding a WARC record
	// before reaching end-of-file. This distinguishes "stream contained only
	// unrecognizable data" from a clean EOF on an empty or fully-consumed stream.
	ErrNoRecord = errors.New("gowarc: no WARC record found")
)

// HeaderFieldError is used for violations of WARC header specification.
// Use [errors.As] to extract the field name and message programmatically.
type HeaderFieldError struct {
	// FieldName is the WARC header field that caused the error (e.g. "WARC-Date").
	// May be empty for structural errors like missing required fields.
	FieldName string
	// Msg describes the violation.
	Msg string
}

func newHeaderFieldError(fieldName string, msg string) *HeaderFieldError {
	return &HeaderFieldError{FieldName: fieldName, Msg: msg}
}

func newHeaderFieldErrorf(fieldName string, msg string, param ...any) *HeaderFieldError {
	return &HeaderFieldError{FieldName: fieldName, Msg: fmt.Sprintf(msg, param...)}
}

func (e *HeaderFieldError) Error() string {
	if e.FieldName != "" {
		return fmt.Sprintf("gowarc: %s at header %s", e.Msg, e.FieldName)
	} else {
		return fmt.Sprintf("gowarc: %s", e.Msg)
	}
}

// SyntaxError is used for syntactical errors like wrong line endings.
// Use [errors.As] to extract position information and wrapped cause programmatically.
type SyntaxError struct {
	// Msg describes the syntax violation.
	Msg string
	// Line is the 1-based line number where the error occurred, or 0 if unknown.
	Line int
	// Wrapped is the underlying cause, if any. Use [errors.As] or [errors.Is]
	// to inspect it, or access it directly.
	Wrapped error
}

func newSyntaxError(msg string) *SyntaxError {
	return &SyntaxError{Msg: msg}
}

func newSyntaxErrorAtLine(msg string, line int) *SyntaxError {
	return &SyntaxError{Msg: msg, Line: line}
}

func newWrappedSyntaxError(msg string, wrapped error) *SyntaxError {
	e := &SyntaxError{Msg: msg, Wrapped: wrapped}
	if se, ok := wrapped.(*SyntaxError); ok && se.Line > 0 {
		e.Line = se.Line
	}
	return e
}

func newWrappedSyntaxErrorAtLine(msg string, line int, wrapped error) *SyntaxError {
	return &SyntaxError{Msg: msg, Line: line, Wrapped: wrapped}
}

func (e *SyntaxError) Error() string {
	s := "gowarc: " + e.Msg
	if e.Line > 0 {
		s += fmt.Sprintf(" at line %d", e.Line)
	}
	if e.Wrapped != nil {
		if v, ok := e.Wrapped.(*SyntaxError); ok {
			s += ": " + v.Msg
		} else {
			s += ": " + e.Wrapped.Error()
		}
	}
	return s
}

func (e *SyntaxError) Unwrap() error {
	return e.Wrapped
}

// DigestError is returned when a computed digest does not match the expected value
// from a WARC-Block-Digest or WARC-Payload-Digest header.
// Use [errors.As] to extract the algorithm, expected, and computed values programmatically.
type DigestError struct {
	// Algorithm is the digest algorithm name (e.g. "sha1", "sha256").
	Algorithm string
	// Expected is the digest value from the WARC header.
	Expected string
	// Computed is the digest value calculated from the record content.
	Computed string
}

func (e *DigestError) Error() string {
	return fmt.Sprintf("wrong digest: expected %s:%s, computed: %s:%s", e.Algorithm, e.Expected, e.Algorithm, e.Computed)
}

// ContentLengthError is returned when the actual content size does not match
// the Content-Length header value.
// Use [errors.As] to extract the expected and actual lengths programmatically.
type ContentLengthError struct {
	// Expected is the Content-Length value declared in the WARC header.
	Expected int64
	// Actual is the measured size of the content.
	Actual int64
}

func (e *ContentLengthError) Error() string {
	return fmt.Sprintf("content length mismatch: header %d, actual %d", e.Expected, e.Actual)
}
