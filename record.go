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
	sphtcrlf = " \t\r\n"  // Space, Tab, Carriage return, Newline
	cr       = '\r'       // Carriage return
	lf       = '\n'       // Newline
	sp       = ' '        // Space
	ht       = '\t'       // Tab
	crlf     = "\r\n"     // Carriage return, Newline
	crlfcrlf = "\r\n\r\n" // Carriage return, Newline, Carriage return, Newline
)

type WarcRecord interface {
	Version() *version
	Type() recordType
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
	// WARC versions
	V1_0 = &version{id: 1, txt: "1.0", major: 1, minor: 0} // WARC 1.0
	V1_1 = &version{id: 2, txt: "1.1", major: 1, minor: 1} // WARC 1.1
)

type recordType uint16

func (rt recordType) String() string {
	switch rt {
	case 1:
		return "warcinfo"
	case 2:
		return "response"
	case 4:
		return "resource"
	case 8:
		return "request"
	case 16:
		return "metadata"
	case 32:
		return "revisit"
	case 64:
		return "conversion"
	case 128:
		return "continuation"
	default:
		return "unknown"
	}
}

func stringToRecordType(rt string) recordType {
	switch rt {
	case "warcinfo":
		return 1
	case "response":
		return 2
	case "resource":
		return 4
	case "request":
		return 8
	case "metadata":
		return 16
	case "revisit":
		return 32
	case "conversion":
		return 64
	case "continuation":
		return 128
	default:
		return 0
	}
}

const (
	// WARC record types
	Warcinfo     = 1
	Response     = 2
	Resource     = 4
	Request      = 8
	Metadata     = 16
	Revisit      = 32
	Conversion   = 64
	Continuation = 128
)

type warcRecord struct {
	opts         *warcRecordOptions
	version      *version
	headers      *warcFields
	recordType   recordType
	block        Block
	finalizeOnce sync.Once
	closer       func() error
}

func newRecord(opts *warcRecordOptions, version *version) *warcRecord {
	wr := &warcRecord{
		opts:       opts,
		version:    version,
		headers:    &warcFields{},
		recordType: 0,
		block:      nil,
	}
	return wr
}

func (wr *warcRecord) Version() *version { return wr.version }

func (wr *warcRecord) Type() recordType { return wr.recordType }

func (wr *warcRecord) WarcHeader() *warcFields { return wr.headers }

func (wr *warcRecord) Block() Block {
	return wr.block
}

func (wr *warcRecord) String() string {
	return fmt.Sprintf("WARC record: version: %s, type: %s, id: %s", wr.version, wr.Type(), wr.WarcHeader().Get(WarcRecordID))
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

func (wr *warcRecord) parseBlock(reader io.Reader, validation *Validation) (err error) {
	//if wr.recordType.id&(Revisit.id) != 0 {
	//	wr.block, err = NewRevisitBlock(wr.block)
	//	return
	//}
	contentType := strings.ToLower(wr.headers.Get(ContentType))
	if wr.recordType&(Response|Resource|Request|Conversion|Continuation) != 0 {
		if strings.HasPrefix(contentType, "application/http") {
			httpBlock, err := newHttpBlock(reader)
			if err != nil {
				return err
			}
			wr.block = httpBlock
			return nil
		}
	}
	if strings.HasPrefix(contentType, "application/warc-fields") {
		warcFieldsBlock, err := newWarcFieldsBlock(reader, validation, wr.opts)
		if err != nil {
			return err
		}
		wr.block = warcFieldsBlock
		return nil
	}

	wr.block = &genericBlock{rawBytes: reader}
	return
}
