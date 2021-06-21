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
	countingreader2 "github.com/nlnwa/gowarc/internal/countingreader"
	"io"
	"os"
)

type WarcFileNameGenerator interface {
	NewWarcfileName() string
}

type WarcFileWriter struct {
	opts            *warcFileWriterOptions
	currentFile     *os.File
	currentFileSize int64
}

// NewWarcFileWriter creates a new WarcFileWriter with the supplied options.
func NewWarcFileWriter(opts ...WarcFileWriterOption) *WarcFileWriter {
	o := defaultwarcFileWriterOptions()
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
	countingReader *countingreader2.Reader
	bufferedReader *bufio.Reader
	currentRecord  WarcRecord
}

func NewWarcFileReader(filename string, offset int64, opts ...WarcRecordOption) (*WarcFileReader, error) {
	file, err := os.Open(filename) // For read access.
	if err != nil {
		return nil, err
	}

	wf := &WarcFileReader{
		file:       file,
		offset:     offset,
		warcReader: NewUnmarshaler(opts...),
	}
	_, err = file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	wf.countingReader = countingreader2.New(file)
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

// Options for Warc file writer
type warcFileWriterOptions struct {
	maxFileSize     int64
	compress        bool
	useSegmentation bool
	nameGenerator   WarcFileNameGenerator
	marshaler       Marshaler
}

// WarcFileWriterOption configures how to write WARC files.
type WarcFileWriterOption interface {
	apply(*warcFileWriterOptions)
}

// funcWarcFileWriterOption wraps a function that modifies warcFileWriterOptions into an
// implementation of the WarcFileWriterOption interface.
type funcWarcFileWriterOption struct {
	f func(*warcFileWriterOptions)
}

func (fo *funcWarcFileWriterOption) apply(po *warcFileWriterOptions) {
	fo.f(po)
}

func newFuncWarcFileOption(f func(*warcFileWriterOptions)) *funcWarcFileWriterOption {
	return &funcWarcFileWriterOption{
		f: f,
	}
}

func defaultwarcFileWriterOptions() warcFileWriterOptions {
	return warcFileWriterOptions{
		maxFileSize:     1024 ^ 3,
		compress:        true,
		useSegmentation: false,
		//nameGenerator:   WarcFileNameGenerator,
		marshaler: &defaultMarshaler{},
	}
}

// WithMaxFileSize sets the max size of the Warc file before creating a new one.
// defaults to 1 GiB
func WithMaxFileSize(size int64) WarcFileWriterOption {
	return newFuncWarcFileOption(func(o *warcFileWriterOptions) {
		o.maxFileSize = size
	})
}

// WithCompression sets if writer should write compressed WARC files.
// defaults to true
func WithCompression(compress bool) WarcFileWriterOption {
	return newFuncWarcFileOption(func(o *warcFileWriterOptions) {
		o.compress = compress
	})
}

// WithSegmentation sets if writer should use segmentation for large WARC records.
// defaults to false
func WithSegmentation() WarcFileWriterOption {
	return newFuncWarcFileOption(func(o *warcFileWriterOptions) {
		o.useSegmentation = true
	})
}

// WithFileNameGenerator sets the WarcFileNameGenerator to use for generating new Warc file names.
// defaults to defaultGenerator
func WithFileNameGenerator(generator WarcFileNameGenerator) WarcFileWriterOption {
	return newFuncWarcFileOption(func(o *warcFileWriterOptions) {
		o.nameGenerator = generator
	})
}

// WithMarshaler sets the Warc record marshaler to use.
// defaults to defaultMarshaler
func WithMarshaler(marshaler Marshaler) WarcFileWriterOption {
	return newFuncWarcFileOption(func(o *warcFileWriterOptions) {
		o.marshaler = marshaler
	})
}
