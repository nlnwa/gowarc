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

type options struct {
	compress            bool
	warcVersion         *version
	errSyntax           errorPolicy
	errSpec             errorPolicy
	errUnknowRecordType errorPolicy
	addMissingFields    bool
	fixContentLength    bool
	fixDigest           bool
}

// The errorPolicy constants describe how to handle WARC record errors.
type errorPolicy int8

const (
	ErrIgnore errorPolicy = 0 // Ignore the given error.
	ErrWarn   errorPolicy = 1 // Ignore given error, but submit a warning.
	ErrFail   errorPolicy = 2 // Fail on given error.
)

// Option configures validation, serialization and deserialization of WARC record.
type Option interface {
	apply(*options)
}

// EmptyOption does not alter the parser configuration. It can be embedded in
// another structure to build custom options.
type EmptyOption struct{}

func (EmptyOption) apply(*options) {}

// funcOption wraps a function that modifies options into an
// implementation of the Option interface.
type funcOption struct {
	f func(*options)
}

func (fo *funcOption) apply(po *options) {
	fo.f(po)
}

func newFuncOption(f func(*options)) *funcOption {
	return &funcOption{
		f: f,
	}
}

func defaultOptions() options {
	return options{
		compress:            true,
		warcVersion:         V1_1,
		errSyntax:           ErrWarn,
		errSpec:             ErrWarn,
		errUnknowRecordType: ErrWarn,
		addMissingFields:    true,
		fixContentLength:    true,
		fixDigest:           true,
	}
}

// New creates a new configuration with the supplied options.
func NewOptions(opts ...Option) *options {
	o := defaultOptions()
	for _, opt := range opts {
		opt.apply(&o)
	}
	return &o
}

func (o options) NewOptions(opts ...Option) *options {
	for _, opt := range opts {
		opt.apply(&o)
	}
	return &o
}

// WithCompression sets if writer should write compressed WARC files.
// defaults to true
func WithCompression(compress bool) Option {
	return newFuncOption(func(o *options) {
		o.compress = compress
	})
}

// WithVersion sets the WARC version to use for new records
// defaults to WARC/1.1
func WithVersion(version *version) Option {
	return newFuncOption(func(o *options) {
		o.warcVersion = version
	})
}

// WithSyntaxErrorPolicy sets the policy for handling syntax errors in WARC records
// defaults to ErrWarn
func WithSyntaxErrorPolicy(policy errorPolicy) Option {
	return newFuncOption(func(o *options) {
		o.errSyntax = policy
	})
}

// WithSpecViolationPolicy sets the policy for handling violations of the WARC specification in WARC records
// defaults to ErrWarn
func WithSpecViolationPolicy(policy errorPolicy) Option {
	return newFuncOption(func(o *options) {
		o.errSpec = policy
	})
}

// WithUnknownRecordTypePolicy sets the policy for handling unknown record types
// defaults to ErrWarn
func WithUnknownRecordTypePolicy(policy errorPolicy) Option {
	return newFuncOption(func(o *options) {
		o.errUnknowRecordType = policy
	})
}

// WithAddMissingFields sets if missing WARC-header fields should be added.
// Only fields which can be generated automaticly are added. That includes WarcRecordID, ContentLength, BlockDigest and PayloadDigest.
// defaults to true
func WithAddMissingFields(addMissingFields bool) Option {
	return newFuncOption(func(o *options) {
		o.addMissingFields = addMissingFields
	})
}

// WithFixContentLength sets if a ContentLength header with value which do not match the actual content length should be set to the real value.
// This will not have any impact if SpecViolationPolicy is ErrIgnore
// defaults to true
func WithFixContentLength(fixContentLength bool) Option {
	return newFuncOption(func(o *options) {
		o.fixContentLength = fixContentLength
	})
}

// WithFixDigest sets if a BlockDigest header or a PayloadDigest header with a value which do not match the actual content should be recalculated.
// This will not have any impact if SpecViolationPolicy is ErrIgnore
// defaults to true
func WithFixDigest(fixDigest bool) Option {
	return newFuncOption(func(o *options) {
		o.fixDigest = fixDigest
	})
}
