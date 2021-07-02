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
)

type WarcRecordBuilder interface {
	io.Writer
	io.StringWriter
	io.ReaderFrom
	AddWarcHeader(name string, value string)
	Finalize() (WarcRecord, *Validation, error)
}

type recordBuilder struct {
	opts       *warcRecordOptions
	version    *version
	headers    *WarcFields
	recordType recordType
	content    diskbuffer.Buffer
}

func (rb recordBuilder) Write(p []byte) (n int, err error) {
	return rb.content.Write(p)
}

func (rb recordBuilder) WriteString(s string) (n int, err error) {
	return rb.content.WriteString(s)
}

func (rb recordBuilder) ReadFrom(r io.Reader) (n int64, err error) {
	return rb.content.ReadFrom(r)
}

func (rb recordBuilder) AddWarcHeader(name string, value string) {
	rb.headers.Add(name, value)
}

func (rb recordBuilder) Finalize() (WarcRecord, *Validation, error) {
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

func NewRecordBuilder(recordType recordType, opts ...WarcRecordOption) *recordBuilder {
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
