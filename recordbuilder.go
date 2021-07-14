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
	"github.com/google/uuid"
	"github.com/nlnwa/gowarc/internal/diskbuffer"
	"io"
	"strconv"
	"time"
)

type WarcRecordBuilder interface {
	io.Writer
	io.StringWriter
	io.ReaderFrom
	io.Closer
	AddWarcHeader(name string, value string)
	AddWarcHeaderInt(name string, value int)
	AddWarcHeaderInt64(name string, value int64)
	AddWarcHeaderTime(name string, value time.Time)
	Build() (WarcRecord, *Validation, error)
	Size() int64
}

type recordBuilder struct {
	opts       *warcRecordOptions
	version    *version
	headers    *WarcFields
	recordType RecordType
	content    diskbuffer.Buffer
}

func (rb *recordBuilder) Write(p []byte) (n int, err error) {
	return rb.content.Write(p)
}

func (rb *recordBuilder) WriteString(s string) (n int, err error) {
	return rb.content.WriteString(s)
}

func (rb *recordBuilder) ReadFrom(r io.Reader) (n int64, err error) {
	return rb.content.ReadFrom(r)
}

func (rb *recordBuilder) AddWarcHeader(name string, value string) {
	rb.headers.Add(name, value)
}

func (rb *recordBuilder) AddWarcHeaderInt(name string, value int) {
	rb.headers.Add(name, strconv.Itoa(value))
}

func (rb *recordBuilder) AddWarcHeaderInt64(name string, value int64) {
	rb.headers.Add(name, strconv.FormatInt(value, 10))
}

func (rb *recordBuilder) AddWarcHeaderTime(name string, value time.Time) {
	rb.headers.Add(name, value.UTC().Format(time.RFC3339))
}

// Close releases resources used by the WarcRecordBuilder
// This method should only be used in the case when for some reason the record is not going to be build.
// Calling Build after Close is an error
func (rb *recordBuilder) Close() error {
	return rb.content.Close()
}

// Size returns the size of the record.
// It is legal to add more content after which the value returned from size will reflect the new size.
func (rb *recordBuilder) Size() int64 {
	return rb.content.Size()
}

func (rb *recordBuilder) Build() (WarcRecord, *Validation, error) {
	if rb.opts.addMissingRecordId && !rb.headers.Has(WarcRecordID) {
		rb.headers.Set(WarcRecordID, "<"+uuid.New().URN()+">")
	}

	wr := &warcRecord{
		opts:       rb.opts,
		version:    rb.version,
		recordType: rb.recordType,
		headers:    rb.headers,
		closer: func() error {
			return rb.content.Close()
		},
	}

	validation, err := rb.validate(wr)
	if err != nil {
		return wr, validation, err
	}
	err = wr.parseBlock(rb.content, validation)
	return wr, validation, err
}

func (rb *recordBuilder) validate(wr *warcRecord) (*Validation, error) {
	validation := &Validation{}
	_, err := validateHeader(rb.headers, wr.version, validation, wr.opts)
	if err != nil {
		return validation, err
	}

	if rb.opts.errSpec > ErrIgnore {
		size := strconv.FormatInt(rb.content.Size(), 10)
		if wr.WarcHeader().Has(ContentLength) {
			if size != wr.headers.Get(ContentLength) {
				switch rb.opts.errSpec {
				case ErrWarn:
					validation.addError(fmt.Errorf("content length mismatch. header: %v, actual: %v", wr.headers.Get(ContentLength), size))
					if rb.opts.fixContentLength {
						wr.WarcHeader().Set(ContentLength, size)
					}
				case ErrFail:
					return validation, fmt.Errorf("content length mismatch. header: %v, actual: %v", wr.headers.Get(ContentLength), size)
				}
			}
		} else if rb.opts.addMissingContentLength {
			wr.headers.Set(ContentLength, size)
		}
	}

	d, err := newDigest(wr.WarcHeader().Get(WarcBlockDigest))
	if err != nil {
		return validation, err
	}
	if _, err := io.Copy(d, rb.content); err != nil {
		return validation, err
	}
	if err := d.validate(); err != nil {
		switch rb.opts.errSpec {
		case ErrIgnore:
		case ErrWarn:
			validation.addError(err)
			if rb.opts.fixDigest {
				wr.WarcHeader().Set(WarcBlockDigest, d.format())
			}
		case ErrFail:
			return validation, fmt.Errorf("wrong block digest " + err.Error())
		}
	}
	_, err = rb.content.Seek(0, io.SeekStart)
	return validation, err
}

func NewRecordBuilder(recordType RecordType, opts ...WarcRecordOption) WarcRecordBuilder {
	o := newOptions(opts...)

	rb := &recordBuilder{
		opts:       o,
		version:    o.warcVersion,
		recordType: recordType,
		headers:    &WarcFields{},
		content:    diskbuffer.New(),
	}
	if recordType != 0 {
		rb.headers.Set(WarcType, recordType.String())
	}
	return rb
}
