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

package gowarc

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type recordTypeMask uint16

// Record type constants
const (
	UNRECOGNIZED_RECORD_TYPE recordTypeMask = 0
	WARCINFO                                = 1 << iota
	RESPONSE
	RESOURCE
	REQUEST
	METADATA
	REVISIT
	CONVERSION
	CONTINUATION
)

const (
	S_WARCINFO     = "warcinfo"
	S_RESPONSE     = "response"
	S_RESOURCE     = "resource"
	S_REQUEST      = "request"
	S_METADATA     = "metadata"
	S_REVISIT      = "revisit"
	S_CONVERSION   = "conversion"
	S_CONTINUATION = "continuation"
)

var RecordTypeStringToMask = map[string]recordTypeMask{
	S_WARCINFO:     WARCINFO,
	S_RESPONSE:     RESPONSE,
	S_RESOURCE:     RESOURCE,
	S_REQUEST:      REQUEST,
	S_METADATA:     METADATA,
	S_REVISIT:      REVISIT,
	S_CONVERSION:   CONVERSION,
	S_CONTINUATION: CONTINUATION,
}

//var RecordTypeMaskToString = map[recordTypeMask]string{
//	WARCINFO:     S_WARCINFO,
//	RESPONSE:     S_RESPONSE,
//	RESOURCE:     S_RESOURCE,
//	REQUEST:      S_REQUEST,
//	METADATA:     S_METADATA,
//	REVISIT:      S_REVISIT,
//	CONVERSION:   S_CONVERSION,
//	CONTINUATION: S_CONTINUATION,
//}

type versionMask uint8

const (
	V_UNSUPPORTED versionMask = 0
	V1_0                      = 1 << iota
	V1_1
)

const (
	S_UNSUPPORTED = "unsupported"
	S_V1_0        = "1.0"
	S_V1_1        = "1.1"
)

var VersionStringToMask = map[string]versionMask{
	S_V1_0: V1_0,
	S_V1_1: V1_1,
}

var VersionMaskToString = map[versionMask]string{
	V_UNSUPPORTED: S_UNSUPPORTED,
	V1_0:          S_V1_0,
	V1_1:          S_V1_1,
}

type version struct {
	id  versionMask
	txt string
}

type WarcRecord struct {
	headers          WarcFields
	extensionHeaders WarcFields
	version          *version
	RecordType       recordTypeMask

	contentLength         int64
	contentType           string
	blockDigest           string
	concurrentTo          []string
	date                  time.Time
	filename              string
	iPAddress             string
	identifiedPayloadType string
	payloadDigest         string
	profile               string
	recordID              string
	refersTo              string
	refersToDate          time.Time
	refersToTargetUri     string
	segmentNumber         int
	segmentOriginId       string
	segmentTotalLength    int64
	targetUri             string
	truncated             string
	typeString            string
	warcinfoID            string

	block Block
}

func (wr *WarcRecord) Version() *version                     { return wr.version }
func (wr *WarcRecord) ContentLength() int64                  { return wr.contentLength }
func (wr *WarcRecord) ContentType() string                   { return wr.contentType }
func (wr *WarcRecord) BlockDigest() string                   { return wr.blockDigest }
func (wr *WarcRecord) ConcurrentTo() []string                { return wr.concurrentTo }
func (wr *WarcRecord) Date() time.Time                       { return wr.date }
func (wr *WarcRecord) Filename() string                      { return wr.filename }
func (wr *WarcRecord) IPAddress() string                     { return wr.iPAddress }
func (wr *WarcRecord) IdentifiedPayloadType() string         { return wr.identifiedPayloadType }
func (wr *WarcRecord) PayloadDigest() string                 { return wr.payloadDigest }
func (wr *WarcRecord) Profile() string                       { return wr.profile }
func (wr *WarcRecord) RecordID() string                      { return wr.recordID }
func (wr *WarcRecord) RefersTo() string                      { return wr.refersTo }
func (wr *WarcRecord) RefersToDate() time.Time               { return wr.refersToDate }
func (wr *WarcRecord) RefersToTargetUri() string             { return wr.refersToTargetUri }
func (wr *WarcRecord) SegmentNumber() int                    { return wr.segmentNumber }
func (wr *WarcRecord) SegmentOriginId() string               { return wr.segmentOriginId }
func (wr *WarcRecord) SegmentTotalLength() int64             { return wr.segmentTotalLength }
func (wr *WarcRecord) TargetUri() string                     { return wr.targetUri }
func (wr *WarcRecord) Truncated() string                     { return wr.truncated }
func (wr *WarcRecord) Type() string                          { return wr.typeString }
func (wr *WarcRecord) WarcinfoID() string                    { return wr.warcinfoID }
func (wr *WarcRecord) Block() Block                          { return wr.block }
func (wr *WarcRecord) ExtensionField(name string) []string   { return wr.extensionHeaders.GetAll(name) }
func (wr *WarcRecord) HasExtensionField(name string) bool    { return wr.extensionHeaders.Has(name) }
func (wr *WarcRecord) ExtensionFieldnames() []string         { return wr.extensionHeaders.Names() }
func (wr *WarcRecord) AddExtensionField(nameVal namedValues) { wr.extensionHeaders.AddAll(nameVal) }
func (wr *WarcRecord) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Type: %v\n", wr.typeString)
	fmt.Fprintf(&sb, "Version: %v\n", wr.version.txt)
	exNames := wr.ExtensionFieldnames()
	if len(exNames) > 0 {
		for _, k := range exNames {
			fmt.Fprintf(&sb, "Extensions: %v = %v\n", k, wr.ExtensionField(k))
		}
	}
	return sb.String()
}

func (wr *WarcRecord) GF() WarcFields { return wr.headers }

func (wr *WarcRecord) Close() {
	rb, err := wr.Block().RawBytes()
	if err != nil {
		return
	}

	remaining := wr.contentLength
	for remaining > 0 {
		n, err := rb.Discard(int(remaining))
		if err != nil {
			break
		}
		remaining = remaining - int64(n)
	}
}

type HttpPayload struct {
}

type Block interface {
	RawBytes() (*bufio.Reader, error)
}

type genericBlock struct {
	rawBytes *bufio.Reader
}

func (p *genericBlock) RawBytes() (*bufio.Reader, error) {
	return p.rawBytes, nil
}

type PayloadBlock interface {
	Block
	PayloadBytes() (io.ReadCloser, error)
}

type HttpRequestBlock interface {
	PayloadBlock
	Request() (*http.Request, error)
}

type HttpResponseBlock interface {
	PayloadBlock
	Response() (*http.Response, error)
}

type WarcFieldsBlock struct {
	Block
	WarcFields WarcFields
}