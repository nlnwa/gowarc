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
	"github.com/google/uuid"
	"github.com/nlnwa/gowarc/v2/internal/diskbuffer"
	"github.com/nlnwa/whatwg-url/url"
)

type warcRecordOptions struct {
	warcVersion              *WarcVersion
	errSyntax                ErrorPolicy
	errSpec                  ErrorPolicy
	errUnknownRecordType     ErrorPolicy
	errBlock                 ErrorPolicy
	skipParseBlock           bool
	addMissingRecordId       bool
	recordIdFunc             func() (string, error)
	addMissingContentLength  bool
	addMissingDigest         bool
	fixContentLength         bool
	fixDigest                bool
	fixSyntaxErrors          bool
	fixWarcFieldsBlockErrors bool
	defaultDigestAlgorithm   string
	defaultDigestEncoding    DigestEncoding
	bufferOptions            []diskbuffer.Option
	urlParserOptions         []url.ParserOption
}

// ErrorPolicy describes how to handle WARC record errors.
type ErrorPolicy int8

const (
	ErrIgnore ErrorPolicy = 0 // Ignore the given error.
	ErrWarn   ErrorPolicy = 1 // Ignore given error, but submit a warning.
	ErrFail   ErrorPolicy = 2 // Fail on given error.
)

// defaultIdGenerator is the default function used to generate record ids.
var defaultIdGenerator = func() (string, error) {
	return uuid.New().URN(), nil
}

// WarcRecordOption configures validation, marshaling and unmarshaling of WARC records.
type WarcRecordOption func(*warcRecordOptions)

func (f WarcRecordOption) apply(o *warcRecordOptions) { f(o) }

func defaultWarcRecordOptions() warcRecordOptions {
	uuid.EnableRandPool()
	return warcRecordOptions{
		warcVersion:              V1_1,
		errSyntax:                ErrWarn,
		errSpec:                  ErrWarn,
		errUnknownRecordType:     ErrWarn,
		errBlock:                 ErrIgnore,
		skipParseBlock:           false,
		addMissingRecordId:       false,
		recordIdFunc:             defaultIdGenerator,
		addMissingContentLength:  false,
		addMissingDigest:         false,
		defaultDigestAlgorithm:   "sha1",
		defaultDigestEncoding:    Base32,
		fixContentLength:         false,
		fixDigest:                false,
		fixSyntaxErrors:          false,
		fixWarcFieldsBlockErrors: false,
	}
}

// New creates a new configuration with the supplied warcRecordOptions.
func newOptions(opts ...WarcRecordOption) *warcRecordOptions {
	o := defaultWarcRecordOptions()
	for _, opt := range opts {
		opt.apply(&o)
	}
	return &o
}

// WithVersion sets the WARC version to use for new records.
//
// defaults to WARC/1.1
func WithVersion(version *WarcVersion) WarcRecordOption {
	return func(o *warcRecordOptions) {
		o.warcVersion = version
	}
}

// WithSyntaxErrorPolicy sets the policy for handling syntax errors in WARC records.
//
// defaults to ErrWarn
func WithSyntaxErrorPolicy(policy ErrorPolicy) WarcRecordOption {
	return func(o *warcRecordOptions) {
		o.errSyntax = policy
	}
}

// WithSpecViolationPolicy sets the policy for handling violations of the WARC specification in WARC records.
//
// defaults to ErrWarn
func WithSpecViolationPolicy(policy ErrorPolicy) WarcRecordOption {
	return func(o *warcRecordOptions) {
		o.errSpec = policy
	}
}

// WithUnknownRecordTypePolicy sets the policy for handling unknown record types.
//
// defaults to ErrWarn
func WithUnknownRecordTypePolicy(policy ErrorPolicy) WarcRecordOption {
	return func(o *warcRecordOptions) {
		o.errUnknownRecordType = policy
	}
}

// WithBlockErrorPolicy sets the policy for handling errors in block parsing.
//
// For most records this is the content fetched from the original source and errors here should be ignored.
//
// defaults to ErrIgnore
func WithBlockErrorPolicy(policy ErrorPolicy) WarcRecordOption {
	return func(o *warcRecordOptions) {
		o.errBlock = policy
	}
}

// WithAddMissingRecordId sets if missing WARC-Record-ID header should be generated.
//
// When creating records with [NewRecordBuilder], missing WARC-Record-ID is always generated.
// This option primarily affects parsing/unmarshalling behavior.
//
// defaults to false
func WithAddMissingRecordId(addMissingRecordId bool) WarcRecordOption {
	return func(o *warcRecordOptions) {
		o.addMissingRecordId = addMissingRecordId
	}
}

// WithRecordIdFunc sets a function for generating WARC-Record-ID if AddMissingRecordId is true.
//
// Expected output is a valid URI without the surrounding '<' and '>' as described in the WARC spec
// (https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#warc-record-id-mandatory)
//
// defaults to generating uuid
func WithRecordIdFunc(recordIdFunc func() (string, error)) WarcRecordOption {
	return func(o *warcRecordOptions) {
		o.recordIdFunc = recordIdFunc
	}
}

// WithAddMissingContentLength sets if missing Content-Length header should be calculated.
//
// When creating records with [NewRecordBuilder], missing Content-Length is always set.
// This option primarily affects parsing/unmarshalling behavior.
//
// defaults to false
func WithAddMissingContentLength(addMissingContentLength bool) WarcRecordOption {
	return func(o *warcRecordOptions) {
		o.addMissingContentLength = addMissingContentLength
	}
}

// WithAddMissingDigest sets if missing Block digest and eventually Payload digest header fields should be calculated.
//
// Only digest fields are controlled by this option. Record ID and Content-Length are always set for records
// created with [NewRecordBuilder] when missing.
//
// defaults to false
func WithAddMissingDigest(addMissingDigest bool) WarcRecordOption {
	return func(o *warcRecordOptions) {
		o.addMissingDigest = addMissingDigest
	}
}

// WithDefaultDigestAlgorithm sets which algorithm to use for digest generation.
//
// Valid values: 'md5', 'sha1', 'sha256' and 'sha512'.
//
// defaults to sha1
func WithDefaultDigestAlgorithm(defaultDigestAlgorithm string) WarcRecordOption {
	return func(o *warcRecordOptions) {
		o.defaultDigestAlgorithm = defaultDigestAlgorithm
	}
}

// WithDefaultDigestEncoding sets which encoding to use for digest generation.
//
// Valid values: Base16, Base32 and Base64.
//
// defaults to Base32
func WithDefaultDigestEncoding(defaultDigestEncoding DigestEncoding) WarcRecordOption {
	return func(o *warcRecordOptions) {
		o.defaultDigestEncoding = defaultDigestEncoding
	}
}

// WithFixContentLength sets if a ContentLength header with value which do not match the actual content length should be set to the real value.
//
// # This will not have any impact if SpecViolationPolicy is ErrIgnore
//
// defaults to false
func WithFixContentLength(fixContentLength bool) WarcRecordOption {
	return func(o *warcRecordOptions) {
		o.fixContentLength = fixContentLength
	}
}

// WithFixDigest sets if a BlockDigest header or a PayloadDigest header with a value which do not match the actual content should be recalculated.
//
// # This will not have any impact if SpecViolationPolicy is ErrIgnore
//
// defaults to false
func WithFixDigest(fixDigest bool) WarcRecordOption {
	return func(o *warcRecordOptions) {
		o.fixDigest = fixDigest
	}
}

// WithFixSyntaxErrors sets if an attempt to fix syntax errors should be done when those are detected.
//
// # This will not have any impact if SyntaxErrorPolicy is ErrIgnore
//
// defaults to false
func WithFixSyntaxErrors(fixSyntaxErrors bool) WarcRecordOption {
	return func(o *warcRecordOptions) {
		o.fixSyntaxErrors = fixSyntaxErrors
	}
}

// WithFixWarcFieldsBlockErrors sets if an attempt to fix syntax errors in warcfields block should be done when those are detected.
//
// A warcfields block is typically generated by a web crawler. An error in this context suggests a potential bug in the crawler's WARC writer.
//
// defaults to false
func WithFixWarcFieldsBlockErrors(fixWarcFieldsBlockErrors bool) WarcRecordOption {
	return func(o *warcRecordOptions) {
		o.fixWarcFieldsBlockErrors = fixWarcFieldsBlockErrors
	}
}

// WithSkipParseBlock sets parser to skip detecting known block types.
//
// This implies that no payload digest can be computed.
func WithSkipParseBlock() WarcRecordOption {
	return func(o *warcRecordOptions) {
		o.skipParseBlock = true
	}
}

// WithNoValidation sets the parser to do as little validation as possible.
//
// This option is for parsing as fast as possible and being as lenient as possible.
// Settings implied by this option are:
//
//	SyntaxErrorPolicy = ErrIgnore
//	SpecViolationPolicy = ErrIgnore
//	UnknownRecordPolicy = ErrIgnore
//	SkipParseBlock = true
func WithNoValidation() WarcRecordOption {
	return func(o *warcRecordOptions) {
		o.errSyntax = ErrIgnore
		o.errSpec = ErrIgnore
		o.errUnknownRecordType = ErrIgnore
		o.skipParseBlock = true
	}
}

// WithStrictValidation sets the parser to fail on first error or violation of WARC specification.
//
// Settings implied by this option are:
//
//	SyntaxErrorPolicy = ErrFail
//	SpecViolationPolicy = ErrFail
//	UnknownRecordPolicy = ErrFail
//	SkipParseBlock = false
func WithStrictValidation() WarcRecordOption {
	return func(o *warcRecordOptions) {
		o.errSyntax = ErrFail
		o.errSpec = ErrFail
		o.errUnknownRecordType = ErrFail
		o.skipParseBlock = false
	}
}

// WithBufferTmpDir sets the directory to use for temporary files.
//
// If not set or dir is the empty string then the default directory for temporary files is used (see os.TempDir).
func WithBufferTmpDir(dir string) WarcRecordOption {
	return func(o *warcRecordOptions) {
		o.bufferOptions = append(o.bufferOptions, diskbuffer.WithTmpDir(dir))
	}
}

// WithBufferMaxMemBytes sets the maximum amount of memory a buffer is allowed to use before overflowing to disk.
//
// defaults to 1 MiB
func WithBufferMaxMemBytes(size int64) WarcRecordOption {
	return func(o *warcRecordOptions) {
		//  TODO : change interface from int64 to int and remove the cast here
		o.bufferOptions = append(o.bufferOptions, diskbuffer.WithMaxMemBytes(int(size)))
	}
}

func WithUrlParserOptions(opts ...url.ParserOption) WarcRecordOption {
	return func(o *warcRecordOptions) {
		o.urlParserOptions = append(o.urlParserOptions, opts...)
	}
}
