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
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"strconv"
	"strings"
	"time"
)

const (
	SPHTCRLF = " \t\r\n"
	CR       = '\r'
	LF       = '\n'
	SP       = ' '
	HT       = '\t'
)

const (
	HdrUnknown uint8 = iota
	StdHdrIdContentLength
	StdHdrIdContentType
	StdHdrIdBlockDigest
	StdHdrIdConcurrentTo
	StdHdrIdDate
	StdHdrIdFilename
	StdHdrIdIPAddress
	StdHdrIdIdentifiedPayloadType
	StdHdrIdPayloadDigest
	StdHdrIdProfile
	StdHdrIdRecordID
	StdHdrIdRefersTo
	StdHdrIdRefersToDate
	StdHdrIdRefersToTargetUri
	StdHdrIdSegmentNumber
	StdHdrIdSegmentOriginId
	StdHdrIdSegmentTotalLength
	StdHdrIdTargetUri
	StdHdrIdTruncated
	StdHdrIdType
	StdHdrIdWarcinfoID
)

type fieldDef struct {
	id            uint8
	name          string
	converterFunc func(wr *WarcRecord, def fieldDef, nameVal namedValues, target interface{}, strict bool) (err error)
	repeatable    bool
	supportedRec  recordTypeMask
	supportedSpec versionMask
}

var fieldDefs = []fieldDef{
	{HdrUnknown, "", pUnknown, true, WARCINFO | RESPONSE | RESOURCE | REQUEST | METADATA | REVISIT | CONVERSION | CONTINUATION, V1_0 | V1_1},
	{StdHdrIdContentLength, "Content-Length", pLong, false, WARCINFO | RESPONSE | RESOURCE | REQUEST | METADATA | REVISIT | CONVERSION | CONTINUATION, V1_0 | V1_1},
	{StdHdrIdContentType, "Content-Type", pString, false, WARCINFO | RESPONSE | RESOURCE | REQUEST | METADATA | REVISIT | CONVERSION | CONTINUATION, V1_0 | V1_1},
	{StdHdrIdBlockDigest, "WARC-Block-Digest", pDigest, false, WARCINFO | RESPONSE | RESOURCE | REQUEST | METADATA | REVISIT | CONVERSION | CONTINUATION, V1_0 | V1_1},
	{StdHdrIdConcurrentTo, "WARC-Concurrent-To", pWarcId, true, RESPONSE | RESOURCE | REQUEST | METADATA | REVISIT, V1_0 | V1_1},
	{StdHdrIdDate, "WARC-Date", pTime, false, WARCINFO | RESPONSE | RESOURCE | REQUEST | METADATA | REVISIT | CONVERSION | CONTINUATION, V1_0 | V1_1},
	{StdHdrIdFilename, "WARC-Filename", pString, false, WARCINFO, V1_0 | V1_1},
	{StdHdrIdIPAddress, "WARC-IP-Address", pString, false, RESPONSE | RESOURCE | REQUEST | METADATA | REVISIT, V1_0 | V1_1},
	{StdHdrIdIdentifiedPayloadType, "WARC-Identified-Payload-Type", pString, false, WARCINFO | RESPONSE | RESOURCE | REQUEST | METADATA | REVISIT | CONVERSION | CONTINUATION, V1_0 | V1_1},
	{StdHdrIdPayloadDigest, "WARC-Payload-Digest", pDigest, false, WARCINFO | RESPONSE | RESOURCE | REQUEST | METADATA | REVISIT | CONVERSION | CONTINUATION, V1_0 | V1_1},
	{StdHdrIdProfile, "WARC-Profile", pString, false, REVISIT, V1_0 | V1_1},
	{StdHdrIdRecordID, "WARC-Record-ID", pWarcId, false, WARCINFO | RESPONSE | RESOURCE | REQUEST | METADATA | REVISIT | CONVERSION | CONTINUATION, V1_0 | V1_1},
	{StdHdrIdRefersTo, "WARC-Refers-To", pWarcId, false, METADATA | REVISIT | CONVERSION, V1_0 | V1_1},
	{StdHdrIdRefersToDate, "WARC-Refers-To-Date", pTime, false, REVISIT, V1_1},
	{StdHdrIdRefersToTargetUri, "WARC-Refers-To-Target-URI", pString, false, REVISIT, V1_1},
	{StdHdrIdSegmentNumber, "WARC-Segment-Number", pInt, false, WARCINFO | RESPONSE | RESOURCE | REQUEST | METADATA | REVISIT | CONVERSION | CONTINUATION, V1_0 | V1_1},
	{StdHdrIdSegmentOriginId, "WARC-Segment-Origin-ID", pWarcId, false, WARCINFO | RESPONSE | RESOURCE | REQUEST | METADATA | REVISIT | CONVERSION | CONTINUATION, V1_0 | V1_1},
	{StdHdrIdSegmentTotalLength, "WARC-Segment-Total-Length", pLong, false, WARCINFO | RESPONSE | RESOURCE | REQUEST | METADATA | REVISIT | CONVERSION | CONTINUATION, V1_0 | V1_1},
	{StdHdrIdTargetUri, "WARC-Target-URI", pString, false, WARCINFO | RESPONSE | RESOURCE | REQUEST | METADATA | REVISIT | CONVERSION | CONTINUATION, V1_0 | V1_1},
	{StdHdrIdTruncated, "WARC-Truncated", pTruncReason, false, WARCINFO | RESPONSE | RESOURCE | REQUEST | METADATA | REVISIT | CONVERSION | CONTINUATION, V1_0 | V1_1},
	{StdHdrIdType, "WARC-Type", pString, false, WARCINFO | RESPONSE | RESOURCE | REQUEST | METADATA | REVISIT | CONVERSION | CONTINUATION, V1_0 | V1_1},
	{StdHdrIdWarcinfoID, "WARC-Warcinfo-ID", pWarcId, false, RESPONSE | RESOURCE | REQUEST | METADATA | REVISIT | CONVERSION | CONTINUATION, V1_0 | V1_1},
}

// Map lower case header name to field definition
var lcHdrNameToDef = make(map[string]fieldDef)

func init() {
	for _, fd := range fieldDefs {
		lcHdrNameToDef[strings.ToLower(fd.name)] = fd
	}
}

type WarcReaderOpts struct {
	Strict bool
}

type WarcReader struct {
	opts             *WarcReaderOpts
	warcFieldsParser *warcFieldsParser
	LastOffset       int64
}

func NewWarcReader(opts *WarcReaderOpts) *WarcReader {
	return &WarcReader{
		opts:             opts,
		warcFieldsParser: newWarcfieldParser(opts),
	}
}

func (wr *WarcReader) GetRecord(b *bufio.Reader) (record *WarcRecord, offset int64, err error) {
	var r *bufio.Reader

	magic, err := b.Peek(5)
	if err != nil {
		return
	}
	// Search for start of new record
	for !(magic[0] == 0x1f && magic[1] == 0x8b) && !bytes.Equal(magic, []byte("WARC/")) {
		b.Discard(1)
		offset++
		magic, err = b.Peek(5)
		if err != nil {
			return
		}
	}

	if magic[0] == 0x1f && magic[1] == 0x8b {
		log.Debug("detected gzip record")
		var g *gzip.Reader
		g, err = gzip.NewReader(b)
		if err != nil {
			return
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
		return
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
	version := &version{txt: string(bytes.Trim(l, SPHTCRLF))}
	version.id = VersionStringToMask[version.txt]

	wf, err := wr.warcFieldsParser.parse(r)
	if err != nil {
		return nil, offset, err
	}

	record = &WarcRecord{
		headers:          wf,
		extensionHeaders: NewWarcFields(),
		version:          version,
	}

	if wr.opts.Strict && version.id == V_UNSUPPORTED {
		return record, offset, fmt.Errorf("unsupported WARC version: " + version.txt)
	}

	err = wr.parseWarcHeader(record)
	if err != nil {
		return nil, offset, err
	}

	length := record.contentLength

	c2 := NewLimitedReader(r, length)
	record.block = &genericBlock{bufio.NewReader(c2)}

	err = wr.parseBlock(record)

	n, err := r.Discard(4)
	if n != 4 || err != nil {
		return record, offset, fmt.Errorf("failed skipping record trailer %v", err)
	}

	return
}

func resolveRecordType(record *WarcRecord, strict bool) (recordType recordTypeMask, err error) {
	typeFieldNameLc := "warc-type"

	typeField, ok := record.headers[typeFieldNameLc]
	if !ok {
		if strict {
			err = errors.New("missing required field WARC-Type")
			return
		} else {
			recordType = UNRECOGNIZED_RECORD_TYPE
		}
	}
	if len(typeField.value) == 1 {
		typeFieldValLc := strings.ToLower(typeField.value[0])
		recordType, ok = RecordTypeStringToMask[typeFieldValLc]
		if !ok {
			if strict {
				err = fmt.Errorf("unrecognized value in field WARC-Type '%s'", typeField.value[0])
				return
			} else {
				recordType = UNRECOGNIZED_RECORD_TYPE
			}
		}
	}

	return
}

func (wr *WarcReader) parseBlock(record *WarcRecord) (err error) {
	if record.RecordType&(REVISIT) != 0 {
		record.block, err = NewRevisitBlock(record.block)
		return
	}
	if record.RecordType&(RESPONSE|RESOURCE|REQUEST|CONVERSION|CONTINUATION) != 0 {
		if strings.HasPrefix(strings.ToLower(record.contentType), "application/http") {
			httpBlock, err := NewHttpBlock(record.block)
			if err != nil {
				return err
			}
			record.block = httpBlock
			return nil
		}
	}
	if strings.HasPrefix(strings.ToLower(record.contentType), "application/warc-fields") {
		wb := &WarcFieldsBlock{
			Block: record.block,
		}
		var rb *bufio.Reader
		rb, err = record.block.RawBytes()
		if err != nil {
			return
		}
		wb.WarcFields, err = wr.warcFieldsParser.parse(bufio.NewReader(rb))
		record.block = wb
		return
	}
	return
}

func (wr *WarcReader) parseWarcHeader(record *WarcRecord) (err error) {
	record.RecordType, err = resolveRecordType(record, wr.opts.Strict)
	if err != nil {
		return
	}

	for k, v := range record.headers {
		var ux = ""
		headerFieldDef := lcHdrNameToDef[k]
		if headerFieldDef.converterFunc == nil {
			headerFieldDef = fieldDefs[0]
		}

		switch headerFieldDef.id {
		case HdrUnknown:
			err = headerFieldDef.converterFunc(record, headerFieldDef, v, &ux, wr.opts.Strict)
		case StdHdrIdContentLength:
			err = headerFieldDef.converterFunc(record, headerFieldDef, v, &record.contentLength, wr.opts.Strict)
		case StdHdrIdContentType:
			err = headerFieldDef.converterFunc(record, headerFieldDef, v, &record.contentType, wr.opts.Strict)
		case StdHdrIdBlockDigest:
			err = headerFieldDef.converterFunc(record, headerFieldDef, v, &record.blockDigest, wr.opts.Strict)
		case StdHdrIdConcurrentTo:
			err = headerFieldDef.converterFunc(record, headerFieldDef, v, &record.concurrentTo, wr.opts.Strict)
		case StdHdrIdDate:
			err = headerFieldDef.converterFunc(record, headerFieldDef, v, &record.date, wr.opts.Strict)
		case StdHdrIdFilename:
			err = headerFieldDef.converterFunc(record, headerFieldDef, v, &record.filename, wr.opts.Strict)
		case StdHdrIdIPAddress:
			err = headerFieldDef.converterFunc(record, headerFieldDef, v, &record.iPAddress, wr.opts.Strict)
		case StdHdrIdIdentifiedPayloadType:
			err = headerFieldDef.converterFunc(record, headerFieldDef, v, &record.identifiedPayloadType, wr.opts.Strict)
		case StdHdrIdPayloadDigest:
			err = headerFieldDef.converterFunc(record, headerFieldDef, v, &record.payloadDigest, wr.opts.Strict)
		case StdHdrIdProfile:
			err = headerFieldDef.converterFunc(record, headerFieldDef, v, &record.profile, wr.opts.Strict)
		case StdHdrIdRecordID:
			err = headerFieldDef.converterFunc(record, headerFieldDef, v, &record.recordID, wr.opts.Strict)
		case StdHdrIdRefersTo:
			err = headerFieldDef.converterFunc(record, headerFieldDef, v, &record.refersTo, wr.opts.Strict)
		case StdHdrIdRefersToDate:
			err = headerFieldDef.converterFunc(record, headerFieldDef, v, &record.refersToDate, wr.opts.Strict)
		case StdHdrIdRefersToTargetUri:
			err = headerFieldDef.converterFunc(record, headerFieldDef, v, &record.refersToTargetUri, wr.opts.Strict)
		case StdHdrIdSegmentNumber:
			err = headerFieldDef.converterFunc(record, headerFieldDef, v, &record.segmentNumber, wr.opts.Strict)
		case StdHdrIdSegmentOriginId:
			err = headerFieldDef.converterFunc(record, headerFieldDef, v, &record.segmentOriginId, wr.opts.Strict)
		case StdHdrIdSegmentTotalLength:
			err = headerFieldDef.converterFunc(record, headerFieldDef, v, &record.segmentTotalLength, wr.opts.Strict)
		case StdHdrIdTargetUri:
			err = headerFieldDef.converterFunc(record, headerFieldDef, v, &record.targetUri, wr.opts.Strict)
		case StdHdrIdType:
			err = headerFieldDef.converterFunc(record, headerFieldDef, v, &record.typeString, wr.opts.Strict)
		case StdHdrIdWarcinfoID:
			err = headerFieldDef.converterFunc(record, headerFieldDef, v, &record.warcinfoID, wr.opts.Strict)
		case StdHdrIdTruncated:
			err = headerFieldDef.converterFunc(record, headerFieldDef, v, &record.warcinfoID, wr.opts.Strict)
		default:
			panic("Unhandled standard field: " + headerFieldDef.name)
		}

		if err != nil {
			return
		}
	}

	return
}

var (
	pUnknown = func(wr *WarcRecord, def fieldDef, nameVal namedValues, target interface{}, strict bool) (err error) {
		wr.extensionHeaders.AddAll(nameVal)
		return
	}
	pString = func(wr *WarcRecord, def fieldDef, nameVal namedValues, target interface{}, strict bool) (err error) {
		if err = checkLegal(wr, def, nameVal, strict); err != nil {
			return
		}
		if def.repeatable {
			dst := target.(*[]string)
			*dst = append(*dst, nameVal.value...)
		} else {
			*target.(*string) = nameVal.value[0]
		}
		return
	}
	pTime = func(wr *WarcRecord, def fieldDef, nameVal namedValues, target interface{}, strict bool) (err error) {
		if err = checkLegal(wr, def, nameVal, strict); err != nil {
			return
		}
		if def.repeatable {
			dst := target.(*[]time.Time)
			for _, v := range nameVal.value {
				var p time.Time
				p, err = time.Parse(time.RFC3339, v)
				*dst = append(*dst, p)
			}
		} else {
			*target.(*time.Time), err = time.Parse(time.RFC3339, nameVal.value[0])
		}
		return
	}
	pWarcId = func(wr *WarcRecord, def fieldDef, nameVal namedValues, target interface{}, strict bool) (err error) {
		if err = checkLegal(wr, def, nameVal, strict); err != nil {
			return
		}
		// TODO: Check WarcID
		if def.repeatable {
			dst := target.(*[]string)
			for _, v := range nameVal.value {
				*dst = append(*dst, strings.Trim(v, "<>"))
			}
		} else {
			*target.(*string) = strings.Trim(nameVal.value[0], "<>")
		}
		return
	}
	pInt = func(wr *WarcRecord, def fieldDef, nameVal namedValues, target interface{}, strict bool) (err error) {
		if err = checkLegal(wr, def, nameVal, strict); err != nil {
			return
		}
		if def.repeatable {
			dst := target.(*[]int)
			for _, v := range nameVal.value {
				var p int
				p, err = strconv.Atoi(v)
				*dst = append(*dst, p)
			}
		} else {
			*target.(*int), err = strconv.Atoi(nameVal.value[0])
		}
		return
	}
	pLong = func(wr *WarcRecord, def fieldDef, nameVal namedValues, target interface{}, strict bool) (err error) {
		if err = checkLegal(wr, def, nameVal, strict); err != nil {
			return
		}
		if def.repeatable {
			dst := target.(*[]int64)
			for _, v := range nameVal.value {
				var p int64
				p, err = strconv.ParseInt(v, 0, 64)
				*dst = append(*dst, p)
			}
		} else {
			*target.(*int64), err = strconv.ParseInt(nameVal.value[0], 0, 64)
		}
		return
	}
	pDigest = func(wr *WarcRecord, def fieldDef, nameVal namedValues, target interface{}, strict bool) (err error) {
		if err = checkLegal(wr, def, nameVal, strict); err != nil {
			return
		}
		// TODO: Check Digest
		if def.repeatable {
			dst := target.(*[]string)
			*dst = append(*dst, nameVal.value...)
		} else {
			*target.(*string) = nameVal.value[0]
		}
		return
	}
	pTruncReason = func(wr *WarcRecord, def fieldDef, nameVal namedValues, target interface{}, strict bool) (err error) {
		if err = checkLegal(wr, def, nameVal, strict); err != nil {
			return
		}
		return
	}
)

func checkLegal(wr *WarcRecord, def fieldDef, nameVal namedValues, strict bool) (err error) {
	if strict && wr.version.id&def.supportedSpec == 0 {
		wr.extensionHeaders.AddAll(nameVal)
		return
	}
	if strict && wr.RecordType&def.supportedRec == 0 {
		err = fmt.Errorf("illegal field '%v' in record type '%v'", nameVal.name, wr.typeString)
		return
	}
	if strict && !def.repeatable && len(nameVal.value) > 1 {
		err = fmt.Errorf("field '%v' occurs more than once in record type '%v'", nameVal.name, wr.typeString)
	}
	return
}
