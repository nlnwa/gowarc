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

package warcrecord

import (
	"bufio"
	"fmt"
	"github.com/nlnwa/gowarc/warcfields"
	"io"
	"strconv"
	"strings"
)

type WarcRecord interface {
	Version() string
	Type() *recordType
	WarcHeader() warcfields.WarcFields
	Block() Block
	String() string
	Close()
}

type Block interface {
	RawBytes() (*bufio.Reader, error)
}

type PayloadBlock interface {
	Block
	PayloadBytes() (io.ReadCloser, error)
}

type version struct {
	id  uint8
	txt string
}

func (v *version) String() string {
	return "WARC/" + v.txt
}

var (
	V1_0 = &version{id: 1, txt: "1.0"}
	V1_1 = &version{id: 2, txt: "1.1"}
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

func New(version *version, recordType *recordType) (WarcRecord, error) {
	r := &warcRecord{
		version:    version,
		recordType: recordType,
		block:      nil,
		strict:     false,
	}
	r.headers = &warcHeader{
		WarcFields: warcfields.New(),
		wr:         r,
	}
	return r, nil
}

type warcRecord struct {
	version    *version
	headers    warcfields.WarcFields
	recordType *recordType

	block  Block
	strict bool
}

func (wr *warcRecord) Version() string { return wr.version.txt }

func (wr *warcRecord) Type() *recordType { return wr.recordType }

func (wr *warcRecord) WarcHeader() warcfields.WarcFields { return wr.headers }

type warcHeader struct {
	warcfields.WarcFields
	wr *warcRecord
}

func (wh *warcHeader) Get(name string) string {
	name, _ = NormalizeName(name)
	return wh.WarcFields.Get(name)
}

func (wh *warcHeader) GetAll(name string) []string {
	name, _ = NormalizeName(name)
	return wh.WarcFields.GetAll(name)
}

func (wh *warcHeader) Has(name string) bool {
	name, _ = NormalizeName(name)
	return wh.WarcFields.Has(name)
}

func (wh *warcHeader) Add(name string, value string) error {
	var def fieldDef
	var err error
	name, def = NormalizeName(name)
	value, err = def.validationFunc(name, value, wh.wr, def, wh.wr.strict)
	if err != nil {
		return err
	}
	return wh.WarcFields.Add(name, value)
}

func (wh *warcHeader) Set(name string, value string) error {
	var def fieldDef
	var err error
	name, def = NormalizeName(name)
	value, err = def.validationFunc(name, value, wh.wr, def, wh.wr.strict)
	if err != nil {
		return err
	}
	return wh.WarcFields.Set(name, value)
}

func (wh *warcHeader) Delete(name string) {
	name, _ = NormalizeName(name)
	wh.WarcFields.Delete(name)
}

func (wh *warcHeader) Sort() {
	wh.WarcFields.Sort()
}

func (wh *warcHeader) Write(w io.Writer) (int64, error) {
	return wh.WarcFields.Write(w)
}

func (wr *warcRecord) Block() Block {
	return wr.block
}

func (wr *warcRecord) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Type: %v\n", wr.Type())
	fmt.Fprintf(&sb, "Version: %v\n", wr.version.txt)
	return sb.String()
}

func (wr *warcRecord) Close() {
	rb, err := wr.Block().RawBytes()
	if err != nil {
		return
	}

	remaining, _ := strconv.Atoi(wr.headers.Get(ContentLength))
	for remaining > 0 {
		n, err := rb.Discard(int(remaining))
		if err != nil {
			break
		}
		remaining = remaining - n
	}
}

type HttpPayload struct {
}

type genericBlock struct {
	rawBytes *bufio.Reader
}

func (p *genericBlock) RawBytes() (*bufio.Reader, error) {
	return p.rawBytes, nil
}
