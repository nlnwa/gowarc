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
	"strconv"
	"strings"
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
	Type() RecordType
	WarcHeader() *WarcFields
	Block() Block
	String() string
	io.Closer
	ToRevisitRecord(ref *RevisitRef) (WarcRecord, error)
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

type RecordType uint16

func (rt RecordType) String() string {
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

func stringToRecordType(rt string) RecordType {
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

type RevisitRef struct {
	Profile        string
	TargetRecordId string
	TargetUri      string
	TargetDate     string
}

const (
	// WARC record types
	Warcinfo     RecordType = 1
	Response     RecordType = 2
	Resource     RecordType = 4
	Request      RecordType = 8
	Metadata     RecordType = 16
	Revisit      RecordType = 32
	Conversion   RecordType = 64
	Continuation RecordType = 128
)

const (
	// Well known content types
	ApplicationWarcFields = "application/warc-fields"
	ApplicationHttp       = "application/http"
)

const (
	// Well known revisit profiles
	ProfileIdenticalPayloadDigest = "http://netpreserve.org/warc/1.1/revisit/identical-payload-digest"
	ProfileServerNotModified      = "http://netpreserve.org/warc/1.1/revisit/server-not-modified"
)

type warcRecord struct {
	opts       *warcRecordOptions
	version    *version
	headers    *WarcFields
	recordType RecordType
	block      Block
	closer     func() error
}

func (wr *warcRecord) Version() *version { return wr.version }

func (wr *warcRecord) Type() RecordType { return wr.recordType }

func (wr *warcRecord) WarcHeader() *WarcFields { return wr.headers }

func (wr *warcRecord) Block() Block {
	return wr.block
}

func (wr *warcRecord) String() string {
	return fmt.Sprintf("WARC record: version: %s, type: %s, id: %s", wr.version, wr.Type(), wr.WarcHeader().Get(WarcRecordID))
}

func (wr *warcRecord) Close() error {
	if wr.closer != nil {
		return wr.closer()
	}
	return nil
}

func (wr *warcRecord) ToRevisitRecord(ref *RevisitRef) (WarcRecord, error) {
	h := wr.headers.clone()

	switch ref.Profile {
	case ProfileIdenticalPayloadDigest:
		if !wr.headers.Has(WarcPayloadDigest) {
			return nil, fmt.Errorf("payload digest is required for Identical Payload Digest Profile")
		}
	case ProfileServerNotModified:
	default:
		return nil, fmt.Errorf("Unknown revisit profile")
	}

	h.Set(WarcType, Revisit.String())
	h.Set(WarcProfile, ref.Profile)
	if ref.TargetRecordId != "" {
		h.Set(WarcRefersTo, ref.TargetRecordId)
	}
	if ref.TargetUri != "" {
		h.Set(WarcRefersToTargetURI, ref.TargetUri)
	}
	if ref.TargetDate != "" {
		h.Set(WarcRefersToDate, ref.TargetDate)
	}
	h.Set(WarcTruncated, "length")

	block, err := newRevisitBlock(wr.opts, wr.block)
	if err != nil {
		return nil, err
	}
	h.Set(WarcBlockDigest, block.BlockDigest())
	h.Set(WarcPayloadDigest, block.PayloadDigest())
	h.Set(ContentLength, strconv.Itoa(len(block.headerBytes)))

	revisit := &warcRecord{
		opts:       wr.opts,
		version:    wr.version,
		recordType: Revisit,
		headers:    h,
		block:      block,
	}
	return revisit, nil
}

func (wr *warcRecord) parseBlock(reader io.Reader, validation *Validation) (err error) {
	d, _ := newDigest("sha1")

	if !wr.opts.skipParseBlock {
		contentType := strings.ToLower(wr.headers.Get(ContentType))
		if wr.recordType&(Response|Resource|Request|Conversion|Continuation) != 0 {
			if strings.HasPrefix(contentType, ApplicationHttp) {
				pd, _ := newDigest("sha1")
				httpBlock, err := newHttpBlock(wr.opts, reader, d, pd)
				if err != nil {
					return err
				}
				wr.block = httpBlock
				return nil
			}
		}
		if wr.recordType == Revisit {
			revisitBlock, err := parseRevisitBlock(wr.opts, reader, wr.headers.Get(WarcBlockDigest), wr.headers.Get(WarcPayloadDigest))
			if err != nil {
				return err
			}
			wr.block = revisitBlock
			return nil
		}
		if strings.HasPrefix(contentType, ApplicationWarcFields) {
			warcFieldsBlock, err := newWarcFieldsBlock(reader, d, validation, wr.opts)
			if err != nil {
				return err
			}
			wr.block = warcFieldsBlock
			return nil
		}
	}

	wr.block = newGenericBlock(wr.opts, reader, d)
	return
}
