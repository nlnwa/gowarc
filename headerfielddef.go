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
	"github.com/nlnwa/whatwg-url/url"
	"net"
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

// validateHeader validates a WarcFields object as a WARC-record header
func validateHeader(wf *WarcFields, version *WarcVersion, validation *Validation, opts *warcRecordOptions) (RecordType, error) {
	rt, err := resolveRecordType(wf, validation, opts)
	if err != nil {
		return rt, err
	}

	if opts.errSpec > ErrIgnore {
		for _, nv := range *wf {
			name, def := normalizeName(nv.Name)
			value, err := def.validationFunc(opts, name, nv.Value, version, rt, def)
			nv.Name = name
			nv.Value = value
			if err != nil {
				switch opts.errSpec {
				case ErrWarn:
					validation.addError(newHeaderFieldError(name, err.Error()))
				case ErrFail:
					return rt, newHeaderFieldError(name, err.Error())
				}
			}

			if !def.repeatable && len(wf.GetAll(name)) > 1 {
				switch opts.errSpec {
				case ErrWarn:
					validation.addError(newHeaderFieldError(name, "field occurs more than once"))
				case ErrFail:
					return rt, newHeaderFieldError(name, "field occurs more than once")
				}
			}
		}

		// Check for required fields
		for _, f := range requiredFields {
			if !wf.Has(f) {
				switch opts.errSpec {
				case ErrWarn:
					validation.addError(newHeaderFieldErrorf("", "missing required field: %s", f))
				case ErrFail:
					return rt, newHeaderFieldErrorf("", "missing required field: %s", f)
				}
			}
		}
		contentLength, _ := strconv.ParseInt(wf.Get(ContentLength), 10, 64)
		if rt != Continuation && contentLength > 0 && !wf.Has(ContentType) {
			switch opts.errSpec {
			case ErrWarn:
				validation.addError(newHeaderFieldErrorf("", "missing required field: %s", ContentType))
			case ErrFail:
				return rt, newHeaderFieldErrorf("", "missing required field: %s", ContentType)
			}
		}

		// Check for illegal fields
		if (Warcinfo|Conversion|Continuation)&rt != 0 && wf.Has(WarcConcurrentTo) {
			switch opts.errSpec {
			case ErrWarn:
				validation.addError(newHeaderFieldErrorf("", "not allowed for record type: %s", ContentType))
			case ErrFail:
				return rt, newHeaderFieldErrorf(WarcConcurrentTo, "not allowed for record type: %s", rt)
			}
		}
	}
	return rt, nil
}

func resolveRecordType(wf *WarcFields, validation *Validation, opts *warcRecordOptions) (RecordType, error) {
	typeFieldNameLc := "warc-type"
	var typeField string
	for _, f := range *wf {
		if strings.ToLower(f.Name) == typeFieldNameLc {
			typeField = f.Value
			break
		}
	}

	var rt RecordType
	if typeField == "" {
		rt = 0
		switch opts.errSpec {
		case ErrIgnore:
		case ErrWarn:
			validation.addError(errors.New("missing required field WARC-Type"))
		case ErrFail:
			return rt, errors.New("missing required field WARC-Type")
		}
	}
	typeFieldValLc := strings.ToLower(typeField)
	rt = stringToRecordType(typeFieldValLc)
	if rt == 0 {
		switch opts.errUnknownRecordType {
		case ErrIgnore:
		case ErrWarn:
			validation.addError(fmt.Errorf("unrecognized value '%s' in field WARC-Type", typeField))
		case ErrFail:
			return rt, fmt.Errorf("unrecognized value '%s' in field WARC-Type", typeField)
		}
	}

	return rt, nil
}

var requiredFields = []string{WarcRecordID, ContentLength, WarcDate, WarcType}

type fieldDef struct {
	name           string
	validationFunc func(opts *warcRecordOptions, name, value string, version *WarcVersion, recordType RecordType, def fieldDef) (validatedValue string, err error)
	repeatable     bool
	supportedRec   RecordType
	supportedSpec  uint8
}

var fieldDefs = []fieldDef{
	{"", pUnknown, true,
		Warcinfo | Response | Resource | Request | Metadata | Revisit | Conversion | Continuation,
		V1_0.id | V1_1.id},
	{ContentLength, pLong, false,
		Warcinfo | Response | Resource | Request | Metadata | Revisit | Conversion | Continuation,
		V1_0.id | V1_1.id},
	{ContentType, pString, false,
		Warcinfo | Response | Resource | Request | Metadata | Revisit | Conversion | Continuation,
		V1_0.id | V1_1.id},
	{WarcBlockDigest, pDigest, false,
		Warcinfo | Response | Resource | Request | Metadata | Revisit | Conversion | Continuation,
		V1_0.id | V1_1.id},
	{WarcConcurrentTo, pWarcId, true,
		Response | Resource | Request | Metadata | Revisit,
		V1_0.id | V1_1.id},
	{WarcDate, pTime, false,
		Warcinfo | Response | Resource | Request | Metadata | Revisit | Conversion | Continuation,
		V1_0.id | V1_1.id},
	{WarcFilename, pString, false,
		Warcinfo,
		V1_0.id | V1_1.id},
	{WarcIPAddress, pIp, false,
		Response | Resource | Request | Metadata | Revisit,
		V1_0.id | V1_1.id},
	{WarcIdentifiedPayloadType, pString, false,
		Warcinfo | Response | Resource | Request | Metadata | Revisit | Conversion | Continuation,
		V1_0.id | V1_1.id},
	{WarcPayloadDigest, pDigest, false,
		Warcinfo | Response | Resource | Request | Metadata | Revisit | Conversion | Continuation,
		V1_0.id | V1_1.id},
	{WarcProfile, pURI, false,
		Revisit,
		V1_0.id | V1_1.id},
	{WarcRecordID, pWarcId, false,
		Warcinfo | Response | Resource | Request | Metadata | Revisit | Conversion | Continuation,
		V1_0.id | V1_1.id},
	{WarcRefersTo, pWarcId, false,
		Metadata | Revisit | Conversion,
		V1_0.id | V1_1.id},
	{WarcRefersToDate, pTime, false,
		Revisit,
		V1_1.id},
	{WarcRefersToTargetURI, pURI, false,
		Revisit,
		V1_1.id},
	{WarcSegmentNumber, pInt, false,
		Warcinfo | Response | Resource | Request | Metadata | Revisit | Conversion | Continuation,
		V1_0.id | V1_1.id},
	{WarcSegmentOriginID, pWarcId, false,
		Continuation,
		V1_0.id | V1_1.id},
	{WarcSegmentTotalLength, pLong, false,
		Continuation,
		V1_0.id | V1_1.id},
	{WarcTargetURI, pURI, false,
		Warcinfo | Response | Resource | Request | Metadata | Revisit | Conversion | Continuation,
		V1_0.id | V1_1.id},
	{WarcTruncated, pTruncReason, false,
		Warcinfo | Response | Resource | Request | Metadata | Revisit | Conversion | Continuation,
		V1_0.id | V1_1.id},
	{WarcType, pWarcType, false,
		Warcinfo | Response | Resource | Request | Metadata | Revisit | Conversion | Continuation,
		V1_0.id | V1_1.id},
	{WarcWarcinfoID, pWarcId, false,
		Response | Resource | Request | Metadata | Revisit | Conversion | Continuation,
		V1_0.id | V1_1.id},
}

// Map lower case header name to field definition
var lcHdrNameToDef = make(map[string]fieldDef)

func init() {
	for _, fd := range fieldDefs {
		lcHdrNameToDef[strings.ToLower(fd.name)] = fd
	}
}

func normalizeName(name string) (string, fieldDef) {
	lcName := strings.ToLower(name)
	if f, ok := lcHdrNameToDef[lcName]; ok {
		return f.name, f
	}
	return http.CanonicalHeaderKey(name), lcHdrNameToDef[""]
}

var (
	pUnknown = func(opts *warcRecordOptions, name, value string, version *WarcVersion, recordType RecordType, def fieldDef) (string, error) {
		return value, nil
	}
	pString = func(opts *warcRecordOptions, name, value string, version *WarcVersion, recordType RecordType, def fieldDef) (string, error) {
		if _, err := checkLegal(opts, name, version, recordType, def); err != nil {
			return "", err
		}
		return value, nil
	}
	pURI = func(opts *warcRecordOptions, name, value string, version *WarcVersion, recordType RecordType, def fieldDef) (string, error) {
		if shouldValidate, err := checkLegal(opts, name, version, recordType, def); err != nil {
			return "", err
		} else if shouldValidate {
			if _, err := url.Parse(value); err != nil {
				return "", err
			}
		}
		return value, nil
	}
	pIp = func(opts *warcRecordOptions, name, value string, version *WarcVersion, recordType RecordType, def fieldDef) (string, error) {
		if shouldValidate, err := checkLegal(opts, name, version, recordType, def); err != nil {
			return "", err
		} else if shouldValidate {
			if ip := net.ParseIP(value); ip == nil {
				return "", fmt.Errorf("illegal ip address: %s", value)
			}
		}
		return value, nil
	}
	pTime = func(opts *warcRecordOptions, name, value string, version *WarcVersion, recordType RecordType, def fieldDef) (string, error) {
		if shouldValidate, err := checkLegal(opts, name, version, recordType, def); err != nil {
			return "", err
		} else if shouldValidate {
			if _, err := time.Parse(time.RFC3339, value); err != nil {
				return "", err
			}
		}
		return value, nil
	}
	pWarcType = func(opts *warcRecordOptions, name, value string, version *WarcVersion, recordType RecordType, def fieldDef) (string, error) {
		if _, err := checkLegal(opts, name, version, recordType, def); err != nil {
			return "", err
		}
		return value, nil
	}
	pWarcId = func(opts *warcRecordOptions, name, value string, version *WarcVersion, recordType RecordType, def fieldDef) (string, error) {
		if shouldValidate, err := checkLegal(opts, name, version, recordType, def); err != nil {
			return "", err
		} else if shouldValidate {
			v := strings.Trim(value, "<>")
			if len(value) != len(v)+2 {
				return "", fmt.Errorf("WARC id should be encapsulated by <>")
			}
			if _, err := url.Parse(v); err != nil {
				return "", err
			}
		}
		return value, nil
	}
	pInt = func(opts *warcRecordOptions, name, value string, version *WarcVersion, recordType RecordType, def fieldDef) (string, error) {
		if shouldValidate, err := checkLegal(opts, name, version, recordType, def); err != nil {
			return "", err
		} else if shouldValidate {
			if _, err := strconv.Atoi(value); err != nil {
				return "", err
			}
		}
		return value, nil
	}
	pLong = func(opts *warcRecordOptions, name, value string, version *WarcVersion, recordType RecordType, def fieldDef) (string, error) {
		if shouldValidate, err := checkLegal(opts, name, version, recordType, def); err != nil {
			return "", err
		} else if shouldValidate {
			if _, err := strconv.ParseInt(value, 0, 64); err != nil {
				return "", err
			}
		}
		return value, nil
	}
	pDigest = func(opts *warcRecordOptions, name, value string, version *WarcVersion, recordType RecordType, def fieldDef) (string, error) {
		if _, err := checkLegal(opts, name, version, recordType, def); err != nil {
			return "", err
		}
		return value, nil
	}
	pTruncReason = func(opts *warcRecordOptions, name, value string, version *WarcVersion, recordType RecordType, def fieldDef) (string, error) {
		if _, err := checkLegal(opts, name, version, recordType, def); err != nil {
			return "", err
		}
		return value, nil
	}
)

func checkLegal(opts *warcRecordOptions, name string, version *WarcVersion, recordType RecordType, def fieldDef) (shouldValidate bool, err error) {
	// All fields are allowed for unknown record types
	if recordType == 0 {
		return
	}

	// If field is not defined in spec version, skip validation
	if opts.errSpec > ErrIgnore && version.id&def.supportedSpec == 0 {
		return
	}

	if opts.errSpec > ErrIgnore && recordType&def.supportedRec == 0 {
		err = fmt.Errorf("illegal field '%v' in record type '%v'", name, recordType.String())
		return
	}
	shouldValidate = true
	return
}
