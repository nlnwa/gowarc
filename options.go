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
	strict      bool
	compress    bool
	warcVersion *version
	errEOL      errorPolicy // How to handle missing carriage return in record
}

// The errorPolicy constants describe how to handle WARC record errors.
type errorPolicy int8

const (
	errIgnore errorPolicy = 0 // Ignore the given error.
	errWarn   errorPolicy = 1 // Ignore given error, but submit a warning.
	errFail   errorPolicy = 2 // Fail on given error.
	errFix    errorPolicy = 4 // Try to fix the given error.
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
		strict:      false,
		compress:    true,
		warcVersion: V1_1,
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

// WithStrict decides if record is reuired to strictly follwing the WARC spec
// defaults to false
func WithStrict(strict bool) Option {
	return newFuncOption(func(o *options) {
		o.strict = strict
	})
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

// WithVersion sets the WARC version to use for new records
// defaults to WARC/1.1
func WithEOLPolicy(policy errorPolicy) Option {
	return newFuncOption(func(o *options) {
		o.errEOL = policy
	})
}
