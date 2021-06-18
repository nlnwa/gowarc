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
	"bufio"
	"compress/gzip"
	"github.com/nlnwa/gowarc/pkg/countingreader"
	"io"
	"os"
)

type WarcFileNameGenerator interface {
	NewWarcfileName() string
}

type WarcFileWriter struct {
	opts            *warcFileOptions
	currentFile     *os.File
	currentFileSize int64
}

// New creates a new configuration with the supplied options.
func NewWarcFileWriter(opts ...WarcFileOption) *WarcFileWriter {
	o := defaultwarcFileOptions()
	for _, opt := range opts {
		opt.apply(&o)
	}
	return &WarcFileWriter{opts: &o}
}

func (w *WarcFileWriter) Write(record WarcRecord) (int64, error) {
	if w.currentFile == nil {
		fileName := w.opts.nameGenerator.NewWarcfileName()
		file, err := os.OpenFile(fileName, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0666)
		if err != nil {
			return 0, err
		}
		w.currentFile = file
	}

	var writer io.Writer = w.currentFile
	if w.opts.compress {
		gz := gzip.NewWriter(writer)
		defer gz.Close()
		writer = gz
	}

	nextRec, size, err := w.opts.marshaler.Marshal(writer, record, 0)
	if err != nil {
		return size, err
	}
	if nextRec != nil {
		s, e := w.Write(nextRec)
		s += size
		return s, e
	}
	return size, nil
}

func (w *WarcFileWriter) Close() error {
	if w.currentFile != nil {
		return w.currentFile.Close()
	}
	return nil
}

type WarcFileReader struct {
	file           *os.File
	initialOffset  int64
	offset         int64
	warcReader     Unmarshaler
	countingReader *countingreader.Reader
	bufferedReader *bufio.Reader
	currentRecord  WarcRecord
}

func NewWarcFilename(filename string, offset int64, opts *options) (*WarcFileReader, error) {
	file, err := os.Open(filename) // For read access.
	if err != nil {
		return nil, err
	}

	return NewWarcFile(file, offset, opts)
}

func NewWarcFile(file *os.File, offset int64, opts *options) (*WarcFileReader, error) {
	wf := &WarcFileReader{
		file:       file,
		offset:     offset,
		warcReader: NewUnmarshaler(opts),
	}
	_, err := file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	wf.countingReader = countingreader.New(file)
	wf.initialOffset = offset
	wf.bufferedReader = bufio.NewReaderSize(wf.countingReader, 4*1024)
	return wf, nil
}

func (wf *WarcFileReader) Next() (WarcRecord, int64, error) {
	if wf.currentRecord != nil {
		wf.currentRecord.Close()
	}
	wf.offset = wf.initialOffset + wf.countingReader.N() - int64(wf.bufferedReader.Buffered())
	fs, _ := wf.file.Stat()
	if fs.Size() <= wf.offset {
		wf.offset = 0
	}

	var err error
	var recordOffset int64
	wf.currentRecord, recordOffset, err = wf.warcReader.Unmarshal(wf.bufferedReader)
	return wf.currentRecord, wf.offset + recordOffset, err
}

func (wf *WarcFileReader) Close() error {
	return wf.file.Close()
}

// Options for Warc file reader and writer
type warcFileOptions struct {
	maxFileSize     int64
	compress        bool
	useSegmentation bool
	nameGenerator   WarcFileNameGenerator
	marshaler       Marshaler
}

// WarcFileOption configures how to write WARC files.
type WarcFileOption interface {
	apply(*warcFileOptions)
}

// funcOption wraps a function that modifies options into an
// implementation of the Option interface.
type funcWarcFileOption struct {
	f func(*warcFileOptions)
}

func (fo *funcWarcFileOption) apply(po *warcFileOptions) {
	fo.f(po)
}

func newFuncWarcFileOption(f func(*warcFileOptions)) *funcWarcFileOption {
	return &funcWarcFileOption{
		f: f,
	}
}

func defaultwarcFileOptions() warcFileOptions {
	return warcFileOptions{
		maxFileSize:     1024 ^ 3,
		compress:        true,
		useSegmentation: false,
		//nameGenerator:   WarcFileNameGenerator,
		marshaler: &defaultMarshaler{},
	}
}

// WithMaxFileSize sets the max size of the Warc file before creating a new one.
// defaults to 1 GiB
func WithMaxFileSize(size int64) WarcFileOption {
	return newFuncWarcFileOption(func(o *warcFileOptions) {
		o.maxFileSize = size
	})
}

// WithCompression sets if writer should write compressed WARC files.
// defaults to true
func WithCompression(compress bool) WarcFileOption {
	return newFuncWarcFileOption(func(o *warcFileOptions) {
		o.compress = compress
	})
}

// WithSegmentation sets if writer should use segmentation for large WARC records.
// defaults to false
func WithSegmentation() WarcFileOption {
	return newFuncWarcFileOption(func(o *warcFileOptions) {
		o.useSegmentation = true
	})
}

// WithFileNameGenerator sets the WarcFileNameGenerator to use for generating new Warc file names.
// defaults to defaultGenerator
func WithFileNameGenerator(generator WarcFileNameGenerator) WarcFileOption {
	return newFuncWarcFileOption(func(o *warcFileOptions) {
		o.nameGenerator = generator
	})
}

// WithMarshaler sets the Warc record marshaler to use.
// defaults to defaultMarshaler
func WithMarshaler(marshaler Marshaler) WarcFileOption {
	return newFuncWarcFileOption(func(o *warcFileOptions) {
		o.marshaler = marshaler
	})
}
