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
	"io"
	"os"
	"strings"
	"sync"
)

const (
	SPHTCRLF = " \t\r\n"
	CR       = '\r'
	LF       = '\n'
	SP       = ' '
	HT       = '\t'
	CRLF     = "\r\n"
	CRLFCRLF = "\r\n\r\n"
)

type WarcRecord interface {
	Version() *version
	Type() *recordType
	WarcHeader() *warcFields
	Block() Block
	String() string
	//Finalize() error
	Close()
}

type version struct {
	id    uint8
	txt   string
	major uint8
	minor uint8
}

func (v *version) String() string {
	return "WARC/" + v.txt
}

func (v *version) Major() uint8 {
	return v.major
}

func (v *version) Minor() uint8 {
	return v.minor
}

var (
	V1_0 = &version{id: 1, txt: "1.0", major: 1, minor: 0}
	V1_1 = &version{id: 2, txt: "1.1", major: 1, minor: 1}
)

type recordType struct {
	id  uint8
	txt string
}

func (rt *recordType) String() string {
	return rt.txt
}

var (
	WARCINFO     = &recordType{id: 1, txt: "warcinfo"}
	RESPONSE     = &recordType{id: 2, txt: "response"}
	RESOURCE     = &recordType{id: 4, txt: "resource"}
	REQUEST      = &recordType{id: 8, txt: "request"}
	METADATA     = &recordType{id: 16, txt: "metadata"}
	REVISIT      = &recordType{id: 32, txt: "revisit"}
	CONVERSION   = &recordType{id: 64, txt: "conversion"}
	CONTINUATION = &recordType{id: 128, txt: "continuation"}
)

var recordTypeStringToType = map[string]*recordType{
	WARCINFO.txt:     WARCINFO,
	RESPONSE.txt:     RESPONSE,
	RESOURCE.txt:     RESOURCE,
	REQUEST.txt:      REQUEST,
	METADATA.txt:     METADATA,
	REVISIT.txt:      REVISIT,
	CONVERSION.txt:   CONVERSION,
	CONTINUATION.txt: CONTINUATION,
}

type warcRecord struct {
	opts         *options
	version      *version
	headers      *warcFields
	recordType   *recordType
	block        Block
	finalizeOnce sync.Once
	closer       func() error
}

func newRecord(opts *options, version *version) *warcRecord {
	wr := &warcRecord{
		opts:       opts,
		version:    version,
		headers:    &warcFields{},
		recordType: nil,
		block:      nil,
	}
	return wr
}

func (wr *warcRecord) Version() *version { return wr.version }

func (wr *warcRecord) Type() *recordType { return wr.recordType }

func (wr *warcRecord) WarcHeader() *warcFields { return wr.headers }

func (wr *warcRecord) Block() Block {
	return wr.block
}

func (wr *warcRecord) String() string {
	return fmt.Sprintf("WARC record: version: %s, type: %s", wr.version, wr.Type())
}

func (wr *warcRecord) Close() {
	if v, ok := wr.block.(PayloadBlock); ok {
		fmt.Fprintf(os.Stderr, "Payload digest: %s, ", v.PayloadDigest())
	}
	fmt.Fprintf(os.Stderr, "Block digest: %s\n", wr.block.BlockDigest())
	if wr.closer != nil {
		wr.closer()
	}
}

func (wr *warcRecord) parseBlock(reader io.Reader) (err error) {
	if wr.recordType.id&(REVISIT.id) != 0 {
		wr.block, err = NewRevisitBlock(wr.block)
		return
	}
	contentType := strings.ToLower(wr.headers.Get(ContentType))
	if wr.recordType.id&(RESPONSE.id|RESOURCE.id|REQUEST.id|CONVERSION.id|CONTINUATION.id) != 0 {
		if strings.HasPrefix(contentType, "application/http") {
			httpBlock, err := NewHttpBlock(reader)
			if err != nil {
				return err
			}
			wr.block = httpBlock
			return nil
		}
	}
	if strings.HasPrefix(contentType, "application/warc-fields") {
		warcFieldsBlock, err := NewWarcFieldsBlock(reader, wr.opts)
		if err != nil {
			return err
		}
		wr.block = warcFieldsBlock
		return nil
	}

	wr.block = &genericBlock{rawBytes: reader}
	return
}
