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
	"errors"
	"fmt"
	"github.com/nlnwa/gowarc/pkg/diskbuffer"
	"io"
	"strconv"
	"strings"
)

type WarcRecordBuilder interface {
	io.Writer
	io.StringWriter
	io.ReaderFrom
	AddWarcHeader(name string, value string)
	Finalize() (WarcRecord, error)
}

type recordBuilder struct {
	opts       *options
	version    *version
	headers    *warcFields
	recordType *recordType
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

func (rb recordBuilder) Finalize() (WarcRecord, error) {
	wr := &warcRecord{
		opts:       rb.opts,
		version:    rb.version,
		recordType: rb.recordType,
		headers:    rb.headers,
		//block: &genericBlock{
		//	rawBytes: rb.content,
		//},
		closer: func() error {
			return rb.content.Close()
		},
	}

	err := rb.validate(wr)
	return wr, err
}

func (rb *recordBuilder) validate(wr *warcRecord) error {
	_, err := rb.headers.ValidateHeader(wr.opts, wr.version)
	if err != nil {
		return err
	}

	size := strconv.FormatInt(rb.content.Size(), 10)
	if wr.WarcHeader().Has(ContentLength) {
		if size != wr.headers.Get(ContentLength) {
			return errors.New("content length mismatch")
		}
	} else {
		if err := wr.WarcHeader().Set(ContentLength, size); err != nil {
			return err
		}
	}

	d, err := newDigest(wr.WarcHeader().Get(WarcBlockDigest))
	if err != nil {
		return err
	}
	io.Copy(d, rb.content)
	if err := d.validate(); err != nil {
		fmt.Printf("%v\n", err)
		//return fmt.Errorf("wrong block digest")
	}
	rb.content.Seek(0, io.SeekStart)

	if strings.HasPrefix(wr.headers.Get(ContentType), "application/http") {
		httpBlock, err := NewHttpBlock(rb.content)
		if err != nil {
			return err
		}
		wr.block = httpBlock
	} else {
		wr.block = &genericBlock{
			rawBytes: rb.content,
		}
	}
	//wr.block.finalize()
	return nil
}

func NewRecordBuilder(opts *options, recordType *recordType) *recordBuilder {
	rb := &recordBuilder{
		opts:       opts,
		version:    opts.warcVersion,
		recordType: recordType,
		headers:    &warcFields{},
		content:    diskbuffer.New(),
	}
	rb.headers.Set(WarcType, recordType.txt)
	return rb
}

func NewResponseRecord(opts *options) WarcRecordBuilder {
	rb := NewRecordBuilder(opts, RESPONSE)

	rb.headers.Set(ContentType, "application/http;msgtype=response")
	rb.headers.Set(WarcBlockDigest, "sha1:UZY6ND6CCHXETFVJD2MSS7ZENMWF7KQ2")
	rb.headers.Set(WarcPayloadDigest, "sha1:CCHXETFVJD2MUZY6ND6SS7ZENMWF7KQ2")

	return rb
}
