/*
 * Copyright 2020 National Library of Norway.
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

package diskbuffer

// browserControllerOptions configure the BrowserController. browserControllerOptions are set by the BrowserControllerOption
// values passed to New.
type options struct {
	maxMemBytes       int64
	maxTotalBytes     int64
	memBufferSizeHint int64
	tmpDir            string
	readOnly          bool
}

// BrowserControllerOption configures BrowserController.
type Option interface {
	apply(*options)
}

// funcBrowserControllerOption wraps a function that modifies browserControllerOptions into an
// implementation of the BrowserControllerOption interface.
type funcOption struct {
	f func(*options)
}

func (fco *funcOption) apply(po *options) {
	fco.f(po)
}

func newFuncOption(f func(*options)) *funcOption {
	return &funcOption{
		f: f,
	}
}

func defaultOptions() options {
	return options{
		maxMemBytes:       1024 * 1024, // 1 MB
		maxTotalBytes:     0,           // No limit
		tmpDir:            "",          // Use OS default
		memBufferSizeHint: 1024 * 16,
	}
}

func WithMaxMemBytes(size int64) Option {
	return newFuncOption(func(o *options) {
		o.maxMemBytes = size
	})
}

func WithMemBufferSizeHint(size int64) Option {
	return newFuncOption(func(o *options) {
		o.memBufferSizeHint = size
	})
}

func WithMaxTotalBytes(size int64) Option {
	return newFuncOption(func(o *options) {
		o.maxTotalBytes = size
	})
}

func WithTmpDir(dir string) Option {
	return newFuncOption(func(o *options) {
		o.tmpDir = dir
	})
}
