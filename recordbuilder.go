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
	"io"
	"time"

	"github.com/nlnwa/gowarc/v2/internal/diskbuffer"
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
	SetRecordType(recordType RecordType)
}

type recordBuilder struct {
	opts       *warcRecordOptions
	version    *WarcVersion
	headers    *WarcFields
	recordType RecordType
	content    diskbuffer.Buffer
}

// Write implements the io.Writer interface
// Data written is added to the record's content block
func (rb *recordBuilder) Write(p []byte) (n int, err error) {
	return rb.content.Write(p)
}

// WriteString implements the io.StringWriter interface
// Data written is added to the record's content block
func (rb *recordBuilder) WriteString(s string) (n int, err error) {
	return rb.content.WriteString(s)
}

// ReadFrom implements the io.ReaderFrom interface
// Data written is added to the record's content block
func (rb *recordBuilder) ReadFrom(r io.Reader) (n int64, err error) {
	return rb.content.ReadFrom(r)
}

// AddWarcHeader adds a new WARC header field with the given name and a string value to the record
func (rb *recordBuilder) AddWarcHeader(name string, value string) {
	rb.headers.Add(name, value)
}

// AddWarcHeaderInt adds a new WARC header field with the given name and an int value to the record
func (rb *recordBuilder) AddWarcHeaderInt(name string, value int) {
	rb.headers.AddInt(name, value)
}

// AddWarcHeaderInt64 adds a new WARC header field with the given name and an int64 value to the record
func (rb *recordBuilder) AddWarcHeaderInt64(name string, value int64) {
	rb.headers.AddInt64(name, value)
}

// AddWarcHeaderTime adds a new WARC header field with the given name and a time.Time value to the record
func (rb *recordBuilder) AddWarcHeaderTime(name string, value time.Time) {
	rb.headers.AddTime(name, value)
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

// SetRecordType overrides the record type set in NewRecordBuilder
func (rb *recordBuilder) SetRecordType(recordType RecordType) {
	rb.recordType = recordType
	if recordType != 0 {
		rb.headers.Set(WarcType, recordType.String())
	}
}

func (rb *recordBuilder) Build() (WarcRecord, *Validation, error) {
	if rb.opts.addMissingRecordId && !rb.headers.Has(WarcRecordID) {
		if id, err := rb.opts.recordIdFunc(); err != nil {
			return nil, nil, err
		} else {
			rb.headers.SetId(WarcRecordID, id)
		}
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
	if err != nil {
		return wr, validation, err
	}

	err = wr.ValidateDigest(validation)

	return wr, validation, err
}

func (rb *recordBuilder) validate(wr *warcRecord) (*Validation, error) {
	size := rb.content.Size()
	if rb.opts.addMissingContentLength && !wr.WarcHeader().Has(ContentLength) {
		wr.headers.SetInt64(ContentLength, size)
	}

	validation := &Validation{}
	_, err := validateHeader(rb.headers, wr.version, validation, wr.opts)
	if err != nil {
		return validation, err
	}

	return validation, err
}

// NewRecordBuilder initializes a WarcRecordBuilder used for creating a new record.
//
// WarcRecordBuilder implements io.Writer for adding the content block. recordType might be 0, but then SetRecordType or
// AddWarcHeader(WarcType, "myRecordType") must be called before Build is called.
//
// When finished with adding headers and writing content, call Build on the WarcRecordBuilder to create a WarcRecord.
func NewRecordBuilder(recordType RecordType, opts ...WarcRecordOption) WarcRecordBuilder {
	o := newOptions(opts...)

	rb := &recordBuilder{
		opts:       o,
		version:    o.warcVersion,
		recordType: recordType,
		headers:    &WarcFields{},
		content:    diskbuffer.New(o.bufferOptions...),
	}
	if recordType != 0 {
		rb.headers.Set(WarcType, recordType.String())
	}
	return rb
}
