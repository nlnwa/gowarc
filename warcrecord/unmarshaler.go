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
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"github.com/nlnwa/gowarc/pkg/countingreader"
	"github.com/nlnwa/gowarc/warcfields"
	"github.com/nlnwa/gowarc/warcoptions"
	log "github.com/sirupsen/logrus"
	"io"
	"strconv"
	"strings"
)

const (
	SPHTCRLF = " \t\r\n"
	CR       = '\r'
	LF       = '\n'
	SP       = ' '
	HT       = '\t'
)

type Unmarshaler interface {
	Unmarshal(b *bufio.Reader) (WarcRecord, int64, error)
}

type unmarshaler struct {
	opts             *warcoptions.WarcOptions
	warcFieldsParser *warcfields.Parser
	LastOffset       int64
}

func NewUnmarshaler(opts *warcoptions.WarcOptions) *unmarshaler {
	um := &unmarshaler{
		opts:             opts,
		warcFieldsParser: warcfields.NewParser(opts),
	}
	um.warcFieldsParser.NewFunc = func(values []warcfields.NameValue, ver interface{}) (warcfields.WarcFields, error) {
		rt, err := um.resolveRecordType(values)
		if err != nil {
			return nil, err
		}
		wr, err := New(ver.(*version), rt)
		if err != nil {
			return nil, err
		}
		for _, f := range values {
			err = wr.WarcHeader().Add(f.Name, f.Value)
			if err != nil {
				return nil, err
			}
		}
		return wr.WarcHeader(), nil
	}
	return um
}

func (wr *unmarshaler) Unmarshal(b *bufio.Reader) (WarcRecord, int64, error) {
	var r *bufio.Reader
	var offset int64

	magic, err := b.Peek(5)
	if err != nil {
		return nil, offset, err
	}
	// Search for start of new record
	for !(magic[0] == 0x1f && magic[1] == 0x8b) && !bytes.Equal(magic, []byte("WARC/")) {
		b.Discard(1)
		offset++
		magic, err = b.Peek(5)
		if err != nil {
			return nil, offset, err
		}
	}

	if magic[0] == 0x1f && magic[1] == 0x8b {
		log.Debug("detected gzip record")
		var g *gzip.Reader
		g, err = gzip.NewReader(b)
		if err != nil {
			return nil, offset, err
		}
		defer g.Close()
		g.Multistream(false)
		r = bufio.NewReader(g)
	} else {
		r = b
	}

	l := make([]byte, 5)
	i, err := io.ReadFull(r, l)
	if err != nil {
		return nil, offset, err
	}
	if i != 5 || !bytes.Equal(l, []byte("WARC/")) {
		return nil, offset, errors.New("missing record version")
	}
	l, err = r.ReadBytes('\n')
	if err != nil {
		return nil, offset, err
	}
	if wr.opts.Strict && l[len(l)-2] != '\r' {
		return nil, offset, fmt.Errorf("missing carriage return on line '%s'", bytes.Trim(l, SPHTCRLF))
	}
	version, err := wr.resolveRecordVersion(string(bytes.Trim(l, SPHTCRLF)))
	if err != nil {
		return nil, offset, err
	}

	wf, err := wr.warcFieldsParser.Parse(r, version)
	if err != nil {
		return nil, offset, err
	}

	record := wf.(*warcHeader).wr
	//record = &WarcRecord{
	//	headers:          wf,
	//	extensionHeaders: NewWarcFields(),
	//	version:          version,
	//}

	//err = wr.parseWarcHeader(record)
	//if err != nil {
	//	return nil, offset, err
	//}

	length, _ := strconv.ParseInt(record.headers.Get(ContentLength), 10, 64)

	c2 := countingreader.NewLimited(r, length)
	record.block = &genericBlock{bufio.NewReader(c2)}

	err = wr.parseBlock(record)

	n, err := r.Discard(4)
	if n != 4 || err != nil {
		return record, offset, fmt.Errorf("failed skipping record trailer %v", err)
	}

	return record, offset, nil
}

func (wr *unmarshaler) parseBlock(record *warcRecord) (err error) {
	if record.recordType.id&(REVISIT.id) != 0 {
		record.block, err = NewRevisitBlock(record.block)
		return
	}
	contentType := strings.ToLower(record.headers.Get(ContentType))
	if record.recordType.id&(RESPONSE.id|RESOURCE.id|REQUEST.id|CONVERSION.id|CONTINUATION.id) != 0 {
		if strings.HasPrefix(contentType, "application/http") {
			httpBlock, err := NewHttpBlock(record.block)
			if err != nil {
				return err
			}
			record.block = httpBlock
			return nil
		}
	}
	if strings.HasPrefix(contentType, "application/warc-fields") {
		warcFieldsBlock, err := NewWarcFieldsBlock(record.block, wr.opts)
		if err != nil {
			return err
		}
		record.block = warcFieldsBlock
		return nil
	}
	return
}

func (um *unmarshaler) resolveRecordType(nv []warcfields.NameValue) (*recordType, error) {
	typeFieldNameLc := "warc-type"
	var typeField string
	for _, f := range nv {
		if strings.ToLower(f.Name) == typeFieldNameLc {
			typeField = f.Value
			break
		}
	}

	var rt *recordType
	if typeField == "" {
		rt = &recordType{id: 0, txt: "MISSING"}
		if um.opts.Strict {
			return rt, errors.New("missing required field WARC-Type")
		}
	}
	typeFieldValLc := strings.ToLower(typeField)
	var ok bool
	rt, ok = recordTypeStringToType[typeFieldValLc]
	if !ok {
		rt = &recordType{id: 0, txt: typeField}
		if um.opts.Strict {
			return rt, fmt.Errorf("unrecognized value in field WARC-Type '%s'", typeField)
		}
	}

	return rt, nil
}

func (um *unmarshaler) resolveRecordVersion(s string) (*version, error) {
	switch s {
	case V1_0.txt:
		return V1_0, nil
	case V1_1.txt:
		return V1_1, nil
	default:
		if um.opts.Strict {
			return nil, fmt.Errorf("unsupported WARC version: %v", s)
		} else {
			return &version{id: 0, txt: s}, nil
		}
	}
}
