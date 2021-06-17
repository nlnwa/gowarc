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
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	// WARC header field name constants
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

// ValidateHeader validates a warcFields object as a WARC-record header
func ValidateHeader(wf *warcFields, version *version, opts *options) (*Validation, *recordType, error) {
	v := &Validation{}

	rt, err := resolveRecordType(wf, v, opts)
	if err != nil {
		return v, rt, err
	}

	for _, nv := range *wf {
		name, def := NormalizeName(nv.Name)
		value, err := def.validationFunc(opts, name, nv.Value, version, rt, def)
		nv.Name = name
		nv.Value = value
		if err != nil {
			return v, rt, err
		}
		if opts.errSpec > ErrIgnore && !def.repeatable && len(wf.GetAll(name)) > 1 {
			switch opts.errSpec {
			case ErrWarn:
				v.AddError(fmt.Errorf("field '%v' occurs more than once in record type '%v'", name, rt.txt))
			case ErrFail:
				return v, rt, fmt.Errorf("field '%v' occurs more than once in record type '%v'", name, rt.txt)
			}
		}
	}

	// Check for required fields
	for _, f := range requiredFields {
		if !wf.Has(f) {
			return v, rt, fmt.Errorf("missing required field: %s", f)
		}
	}
	contentLength, _ := strconv.ParseInt(wf.Get(ContentLength), 10, 64)
	if rt != CONTINUATION && contentLength > 0 && !wf.Has(ContentType) {
		return v, rt, fmt.Errorf("missing required field: %s", ContentType)
	}

	// Check for illegal fields
	if (WARCINFO.id|CONVERSION.id|CONTINUATION.id)&rt.id != 0 && wf.Has(WarcConcurrentTo) {
		return v, rt, fmt.Errorf("field %s not allowed for record type %s :: %b %b %b", WarcConcurrentTo, rt, (WARCINFO.id | CONVERSION.id | CONTINUATION.id), rt.id, (WARCINFO.id|CONVERSION.id|CONTINUATION.id)&rt.id)
	}
	return v, rt, nil
}

func resolveRecordType(wf *warcFields, validation *Validation, opts *options) (*recordType, error) {
	typeFieldNameLc := "warc-type"
	var typeField string
	for _, f := range *wf {
		if strings.ToLower(f.Name) == typeFieldNameLc {
			typeField = f.Value
			break
		}
	}

	var rt *recordType
	if typeField == "" {
		rt = &recordType{id: 0, txt: "MISSING"}
		switch opts.errSpec {
		case ErrIgnore:
		case ErrWarn:
			validation.AddError(errors.New("missing required field WARC-Type"))
		case ErrFail:
			return rt, errors.New("missing required field WARC-Type")
		}
	}
	typeFieldValLc := strings.ToLower(typeField)
	var ok bool
	rt, ok = recordTypeStringToType[typeFieldValLc]
	if !ok {
		rt = &recordType{id: 0, txt: typeField}
		switch opts.errSpec {
		case ErrIgnore:
		case ErrWarn:
			validation.AddError(fmt.Errorf("unrecognized value in field WARC-Type '%s'", typeField))
		case ErrFail:
			return rt, fmt.Errorf("unrecognized value in field WARC-Type '%s'", typeField)
		}
	}

	return rt, nil
}

var requiredFields = []string{WarcRecordID, ContentLength, WarcDate, WarcType}

type fieldDef struct {
	name           string
	validationFunc func(opts *options, name, value string, version *version, recordType *recordType, def fieldDef) (validatedValue string, err error)
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
	pUnknown = func(opts *options, name, value string, version *version, recordType *recordType, def fieldDef) (string, error) {
		return value, nil
	}
	pString = func(opts *options, name, value string, version *version, recordType *recordType, def fieldDef) (string, error) {
		if err := checkLegal(opts, name, value, version, recordType, def); err != nil {
			return "", err
		}
		return value, nil
	}
	pTime = func(opts *options, name, value string, version *version, recordType *recordType, def fieldDef) (string, error) {
		if err := checkLegal(opts, name, value, version, recordType, def); err != nil {
			return "", err
		}
		if _, err := time.Parse(time.RFC3339, value); err != nil {
			return "", err
		}
		return value, nil
	}
	pWarcType = func(opts *options, name, value string, version *version, recordType *recordType, def fieldDef) (string, error) {
		//if value != wr.recordType.txt {
		//	return "", fmt.Errorf("not allowed to change record type")
		//}
		if err := checkLegal(opts, name, value, version, recordType, def); err != nil {
			return "", err
		}
		return value, nil
	}
	pWarcId = func(opts *options, name, value string, version *version, recordType *recordType, def fieldDef) (string, error) {
		if err := checkLegal(opts, name, value, version, recordType, def); err != nil {
			return "", err
		}
		return value, nil
		//v := strings.Trim(value, "<>")
		//if value != v {
		//	return "", fmt.Errorf("WARC id should not be encapsulated by brackets")
		//}
		//return v, nil
	}
	pInt = func(opts *options, name, value string, version *version, recordType *recordType, def fieldDef) (string, error) {
		if err := checkLegal(opts, name, value, version, recordType, def); err != nil {
			return "", err
		}
		if _, err := strconv.Atoi(value); err != nil {
			return "", err
		}
		return value, nil
	}
	pLong = func(opts *options, name, value string, version *version, recordType *recordType, def fieldDef) (string, error) {
		if err := checkLegal(opts, name, value, version, recordType, def); err != nil {
			return "", err
		}
		if _, err := strconv.ParseInt(value, 0, 64); err != nil {
			return "", err
		}
		return value, nil
	}
	pDigest = func(opts *options, name, value string, version *version, recordType *recordType, def fieldDef) (string, error) {
		if err := checkLegal(opts, name, value, version, recordType, def); err != nil {
			return "", err
		}
		// TODO: Check Digest
		return value, nil
	}
	pTruncReason = func(opts *options, name, value string, version *version, recordType *recordType, def fieldDef) (string, error) {
		if err := checkLegal(opts, name, value, version, recordType, def); err != nil {
			return "", err
		}
		return value, nil
	}
)

func checkLegal(opts *options, name, value string, version *version, recordType *recordType, def fieldDef) (err error) {
	if opts.errSpec > ErrIgnore && version.id&def.supportedSpec == 0 {
		return
	}
	if opts.errSpec > ErrIgnore && recordType.id&def.supportedRec == 0 {
		err = fmt.Errorf("illegal field '%v' in record type '%v'", name, recordType.txt)
		return
	}
	return
}
