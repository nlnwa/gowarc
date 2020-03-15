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
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// WARC header field names
const (
	ContentLength             = "Content-Length"
	ContentType               = "Content-Type"
	WarcBlockDigest           = "WARC-Block-Digest"
	WarcConcurrentTo          = "WARC-Concurrent-To"
	WarcDate                  = "WARC-Date"
	WarcFilename              = "WARC-Filename"
	WarcIPAddress             = "WARC-IP-Address"
	WarcIdentifiedPayloadType = "WARC-Identified-Payload-Type"
	WarcPayloadDigest         = "WARC-Payload-Digest"
	WarcProfile               = "WARC-Profile"
	WarcRecordID              = "WARC-Record-ID"
	WarcRefersTo              = "WARC-Refers-To"
	WarcRefersToDate          = "WARC-Refers-To-Date"
	WarcRefersToTargetURI     = "WARC-Refers-To-Target-URI"
	WarcSegmentNumber         = "WARC-Segment-Number"
	WarcSegmentOriginID       = "WARC-Segment-Origin-ID"
	WarcSegmentTotalLength    = "WARC-Segment-Total-Length"
	WarcTargetURI             = "WARC-Target-URI"
	WarcTruncated             = "WARC-Truncated"
	WarcType                  = "WARC-Type"
	WarcWarcinfoID            = "WARC-Warcinfo-ID"
)

type fieldDef struct {
	name           string
	validationFunc func(name, value string, wr *warcRecord, def fieldDef, strict bool) (validatedValue string, err error)
	repeatable     bool
	supportedRec   uint8
	supportedSpec  uint8
}

var fieldDefs = []fieldDef{
	{"", pUnknown, true,
		WARCINFO.id | RESPONSE.id | RESOURCE.id | REQUEST.id | METADATA.id | REVISIT.id | CONVERSION.id | CONTINUATION.id,
		V1_0.id | V1_1.id},
	{ContentLength, pLong, false,
		WARCINFO.id | RESPONSE.id | RESOURCE.id | REQUEST.id | METADATA.id | REVISIT.id | CONVERSION.id | CONTINUATION.id,
		V1_0.id | V1_1.id},
	{ContentType, pString, false,
		WARCINFO.id | RESPONSE.id | RESOURCE.id | REQUEST.id | METADATA.id | REVISIT.id | CONVERSION.id | CONTINUATION.id,
		V1_0.id | V1_1.id},
	{WarcBlockDigest, pDigest, false,
		WARCINFO.id | RESPONSE.id | RESOURCE.id | REQUEST.id | METADATA.id | REVISIT.id | CONVERSION.id | CONTINUATION.id,
		V1_0.id | V1_1.id},
	{WarcConcurrentTo, pWarcId, true,
		RESPONSE.id | RESOURCE.id | REQUEST.id | METADATA.id | REVISIT.id,
		V1_0.id | V1_1.id},
	{WarcDate, pTime, false,
		WARCINFO.id | RESPONSE.id | RESOURCE.id | REQUEST.id | METADATA.id | REVISIT.id | CONVERSION.id | CONTINUATION.id,
		V1_0.id | V1_1.id},
	{WarcFilename, pString, false,
		WARCINFO.id,
		V1_0.id | V1_1.id},
	{WarcIPAddress, pString, false,
		RESPONSE.id | RESOURCE.id | REQUEST.id | METADATA.id | REVISIT.id,
		V1_0.id | V1_1.id},
	{WarcIdentifiedPayloadType, pString, false,
		WARCINFO.id | RESPONSE.id | RESOURCE.id | REQUEST.id | METADATA.id | REVISIT.id | CONVERSION.id | CONTINUATION.id,
		V1_0.id | V1_1.id},
	{WarcPayloadDigest, pDigest, false,
		WARCINFO.id | RESPONSE.id | RESOURCE.id | REQUEST.id | METADATA.id | REVISIT.id | CONVERSION.id | CONTINUATION.id,
		V1_0.id | V1_1.id},
	{WarcProfile, pString, false,
		REVISIT.id,
		V1_0.id | V1_1.id},
	{WarcRecordID, pWarcId, false,
		WARCINFO.id | RESPONSE.id | RESOURCE.id | REQUEST.id | METADATA.id | REVISIT.id | CONVERSION.id | CONTINUATION.id,
		V1_0.id | V1_1.id},
	{WarcRefersTo, pWarcId, false,
		METADATA.id | REVISIT.id | CONVERSION.id,
		V1_0.id | V1_1.id},
	{WarcRefersToDate, pTime, false,
		REVISIT.id,
		V1_1.id},
	{WarcRefersToTargetURI, pString, false,
		REVISIT.id,
		V1_1.id},
	{WarcSegmentNumber, pInt, false,
		WARCINFO.id | RESPONSE.id | RESOURCE.id | REQUEST.id | METADATA.id | REVISIT.id | CONVERSION.id | CONTINUATION.id,
		V1_0.id | V1_1.id},
	{WarcSegmentOriginID, pWarcId, false,
		WARCINFO.id | RESPONSE.id | RESOURCE.id | REQUEST.id | METADATA.id | REVISIT.id | CONVERSION.id | CONTINUATION.id,
		V1_0.id | V1_1.id},
	{WarcSegmentTotalLength, pLong, false,
		WARCINFO.id | RESPONSE.id | RESOURCE.id | REQUEST.id | METADATA.id | REVISIT.id | CONVERSION.id | CONTINUATION.id,
		V1_0.id | V1_1.id},
	{WarcTargetURI, pString, false,
		WARCINFO.id | RESPONSE.id | RESOURCE.id | REQUEST.id | METADATA.id | REVISIT.id | CONVERSION.id | CONTINUATION.id,
		V1_0.id | V1_1.id},
	{WarcTruncated, pTruncReason, false,
		WARCINFO.id | RESPONSE.id | RESOURCE.id | REQUEST.id | METADATA.id | REVISIT.id | CONVERSION.id | CONTINUATION.id,
		V1_0.id | V1_1.id},
	{WarcType, pWarcType, false,
		WARCINFO.id | RESPONSE.id | RESOURCE.id | REQUEST.id | METADATA.id | REVISIT.id | CONVERSION.id | CONTINUATION.id,
		V1_0.id | V1_1.id},
	{WarcWarcinfoID, pWarcId, false,
		RESPONSE.id | RESOURCE.id | REQUEST.id | METADATA.id | REVISIT.id | CONVERSION.id | CONTINUATION.id,
		V1_0.id | V1_1.id},
}

// Map lower case header name to field definition
var lcHdrNameToDef = make(map[string]fieldDef)

func init() {
	for _, fd := range fieldDefs {
		lcHdrNameToDef[strings.ToLower(fd.name)] = fd
	}
}

func NormalizeName(name string) (string, fieldDef) {
	lcName := strings.ToLower(name)
	if f, ok := lcHdrNameToDef[lcName]; ok {
		return f.name, f
	}
	return http.CanonicalHeaderKey(name), lcHdrNameToDef[""]
}

var (
	pUnknown = func(name, value string, wr *warcRecord, def fieldDef, strict bool) (string, error) {
		return value, nil
	}
	pString = func(name, value string, wr *warcRecord, def fieldDef, strict bool) (string, error) {
		if err := checkLegal(name, value, wr, def, strict); err != nil {
			return "", err
		}
		return value, nil
	}
	pTime = func(name, value string, wr *warcRecord, def fieldDef, strict bool) (string, error) {
		if err := checkLegal(name, value, wr, def, strict); err != nil {
			return "", err
		}
		if _, err := time.Parse(time.RFC3339, value); err != nil {
			return "", err
		}
		return value, nil
	}
	pWarcType = func(name, value string, wr *warcRecord, def fieldDef, strict bool) (string, error) {
		if value != wr.recordType.txt {
			return "", fmt.Errorf("not allowed to change record type")
		}
		if err := checkLegal(name, value, wr, def, strict); err != nil {
			return "", err
		}
		return value, nil
	}
	pWarcId = func(name, value string, wr *warcRecord, def fieldDef, strict bool) (string, error) {
		if err := checkLegal(name, value, wr, def, strict); err != nil {
			return "", err
		}
		return value, nil
		//v := strings.Trim(value, "<>")
		//if value != v {
		//	return "", fmt.Errorf("WARC id should not be encapsulated by brackets")
		//}
		//return v, nil
	}
	pInt = func(name, value string, wr *warcRecord, def fieldDef, strict bool) (string, error) {
		if err := checkLegal(name, value, wr, def, strict); err != nil {
			return "", err
		}
		if _, err := strconv.Atoi(value); err != nil {
			return "", err
		}
		return value, nil
	}
	pLong = func(name, value string, wr *warcRecord, def fieldDef, strict bool) (string, error) {
		if err := checkLegal(name, value, wr, def, strict); err != nil {
			return "", err
		}
		if _, err := strconv.ParseInt(value, 0, 64); err != nil {
			return "", err
		}
		return value, nil
	}
	pDigest = func(name, value string, wr *warcRecord, def fieldDef, strict bool) (string, error) {
		if err := checkLegal(name, value, wr, def, strict); err != nil {
			return "", err
		}
		// TODO: Check Digest
		return value, nil
	}
	pTruncReason = func(name, value string, wr *warcRecord, def fieldDef, strict bool) (string, error) {
		if err := checkLegal(name, value, wr, def, strict); err != nil {
			return "", err
		}
		return value, nil
	}
)

func checkLegal(name, value string, wr *warcRecord, def fieldDef, strict bool) (err error) {
	if strict && wr.version.id&def.supportedSpec == 0 {
		return
	}
	if strict && wr.recordType.id&def.supportedRec == 0 {
		err = fmt.Errorf("illegal field '%v' in record type '%v'", name, wr.recordType.txt)
		return
	}
	if strict && !def.repeatable && wr.headers.Has(name) {
		err = fmt.Errorf("field '%v' occurs more than once in record type '%v'", name, wr.recordType.txt)
	}
	return
}
