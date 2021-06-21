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

type warcRecordOptions struct {
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

// WarcRecordOption configures validation, marshaling and unmarshaling of WARC records.
type WarcRecordOption interface {
	apply(*warcRecordOptions)
}

// funcWarcRecordOption wraps a function that modifies warcRecordOptions into an
// implementation of the WarcRecordOption interface.
type funcWarcRecordOption struct {
	f func(*warcRecordOptions)
}

func (fo *funcWarcRecordOption) apply(po *warcRecordOptions) {
	fo.f(po)
}

func newFuncWarcRecordOption(f func(*warcRecordOptions)) *funcWarcRecordOption {
	return &funcWarcRecordOption{
		f: f,
	}
}

func defaultWarcRecordOptions() warcRecordOptions {
	return warcRecordOptions{
		warcVersion:         V1_1,
		errSyntax:           ErrWarn,
		errSpec:             ErrWarn,
		errUnknowRecordType: ErrWarn,
		addMissingFields:    true,
		fixContentLength:    true,
		fixDigest:           true,
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

// WithVersion sets the WARC version to use for new records
// defaults to WARC/1.1
func WithVersion(version *version) WarcRecordOption {
	return newFuncWarcRecordOption(func(o *warcRecordOptions) {
		o.warcVersion = version
	})
}

// WithSyntaxErrorPolicy sets the policy for handling syntax errors in WARC records
// defaults to ErrWarn
func WithSyntaxErrorPolicy(policy errorPolicy) WarcRecordOption {
	return newFuncWarcRecordOption(func(o *warcRecordOptions) {
		o.errSyntax = policy
	})
}

// WithSpecViolationPolicy sets the policy for handling violations of the WARC specification in WARC records
// defaults to ErrWarn
func WithSpecViolationPolicy(policy errorPolicy) WarcRecordOption {
	return newFuncWarcRecordOption(func(o *warcRecordOptions) {
		o.errSpec = policy
	})
}

// WithUnknownRecordTypePolicy sets the policy for handling unknown record types
// defaults to ErrWarn
func WithUnknownRecordTypePolicy(policy errorPolicy) WarcRecordOption {
	return newFuncWarcRecordOption(func(o *warcRecordOptions) {
		o.errUnknowRecordType = policy
	})
}

// WithAddMissingFields sets if missing WARC-header fields should be added.
// Only fields which can be generated automaticly are added. That includes WarcRecordID, ContentLength, BlockDigest and PayloadDigest.
// defaults to true
func WithAddMissingFields(addMissingFields bool) WarcRecordOption {
	return newFuncWarcRecordOption(func(o *warcRecordOptions) {
		o.addMissingFields = addMissingFields
	})
}

// WithFixContentLength sets if a ContentLength header with value which do not match the actual content length should be set to the real value.
// This will not have any impact if SpecViolationPolicy is ErrIgnore
// defaults to true
func WithFixContentLength(fixContentLength bool) WarcRecordOption {
	return newFuncWarcRecordOption(func(o *warcRecordOptions) {
		o.fixContentLength = fixContentLength
	})
}

// WithFixDigest sets if a BlockDigest header or a PayloadDigest header with a value which do not match the actual content should be recalculated.
// This will not have any impact if SpecViolationPolicy is ErrIgnore
// defaults to true
func WithFixDigest(fixDigest bool) WarcRecordOption {
	return newFuncWarcRecordOption(func(o *warcRecordOptions) {
		o.fixDigest = fixDigest
	})
}
