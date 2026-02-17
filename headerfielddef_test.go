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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateHeader(t *testing.T) {
	tests := []struct {
		name              string
		header            *WarcFields
		opts              *warcRecordOptions
		wantErr           error
		wantValidationErr error
	}{
		{
			"Valid warcinfo header",
			&WarcFields{
				&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
				&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
				&nameValue{Name: WarcFilename, Value: "temp-20170306040353.warc.gz"},
				&nameValue{Name: WarcType, Value: "warcinfo"},
				&nameValue{Name: ContentType, Value: "application/warc-fields"},
				&nameValue{Name: ContentLength, Value: "249"},
			},
			newOptions(),
			nil,
			nil,
		},

		{
			"Missing required field: Warc-Type",
			&WarcFields{
				&nameValue{Name: WarcDate, Value: "2017-13-06T04:03:53Z"},
				&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
				&nameValue{Name: WarcFilename, Value: "temp-20170306040353.warc.gz"},
				&nameValue{Name: ContentType, Value: "application/warc-fields"},
				&nameValue{Name: ContentLength, Value: "249"},
			},
			newOptions(WithSpecViolationPolicy(ErrFail)),
			fmt.Errorf("missing required field %s", WarcType),
			nil,
		},
		{
			"Month out of range",
			&WarcFields{
				&nameValue{Name: WarcDate, Value: "2017-13-06T04:03:53Z"},
				&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
				&nameValue{Name: WarcType, Value: "resource"},
				&nameValue{Name: ContentLength, Value: "249"},
			},
			newOptions(WithSpecViolationPolicy(ErrFail)),
			errors.New("gowarc: parsing time \"2017-13-06T04:03:53Z\": month out of range at header WARC-Date"),
			nil,
		},
		{
			"Missing required field: Content-Type",
			&WarcFields{
				&nameValue{Name: WarcDate, Value: "2017-12-06T04:03:53Z"},
				&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
				&nameValue{Name: WarcType, Value: "resource"},
				&nameValue{Name: ContentLength, Value: "249"},
			},
			newOptions(WithSpecViolationPolicy(ErrFail)),
			newHeaderFieldErrorf("", "missing required field: %s", ContentType),
			nil,
		},
		{
			"Illegal field 'Warc-Filename' in resource record",
			&WarcFields{
				&nameValue{Name: WarcDate, Value: "2017-12-06T04:03:53Z"},
				&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
				&nameValue{Name: WarcFilename, Value: "temp-20170306040353.warc.gz"},
				&nameValue{Name: WarcType, Value: "resource"},
				&nameValue{Name: ContentLength, Value: "249"},
				&nameValue{Name: ContentType, Value: "application/http; msgtype=response"},
			},
			newOptions(),
			nil,
			errors.New("gowarc: illegal field 'WARC-Filename' in record type 'resource' at header WARC-Filename"),
		},
		{
			"Browsertrix extension fields",
			&WarcFields{
				&nameValue{Name: WarcDate, Value: "2024-03-17T16:26:51.802Z"},
				&nameValue{Name: WarcRecordID, Value: "<urn:uuid:d3aae465-714f-4aa8-8f1b-23e75b09af42>"},
				&nameValue{Name: WarcType, Value: "response"},
				&nameValue{Name: ContentType, Value: "application/http; msgtype=response"},
				&nameValue{Name: ContentLength, Value: "249"},
				&nameValue{Name: WarcTargetURI, Value: "http://www.example.com/"},
				&nameValue{Name: WarcPayloadDigest, Value: "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
				&nameValue{Name: WarcPageID, Value: "53b8df8a-3b50-42bc-8c01-747e94fcd2bc"},
				&nameValue{Name: WarcResourceType, Value: "document"},
				&nameValue{Name: WarcJSONMetadata, Value: "{\"ipType\":\"Public\",\"cert\":{\"issuer\":\"GeoTrust RSA CA 2018\",\"ctc\":\"1\"}}"},
			},
			newOptions(WithSpecViolationPolicy(ErrFail)),
			nil,
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt, validation, err := validateHeader(tt.header, V1_1, tt.opts)
			if err != nil && tt.wantErr == nil {
				t.Errorf("validateHeader() unexpected error = %v", err)
				return
			}
			if err == nil && tt.wantErr != nil {
				t.Errorf("validateHeader() expected error = %v, got nil", tt.wantErr)
				return
			}
			if err != nil && tt.wantErr != nil && err.Error() != tt.wantErr.Error() {
				t.Errorf("validateHeader() error = %v, want %v", err.Error(), tt.wantErr.Error())
				return
			}
			if rt != stringToRecordType(tt.header.Get(WarcType)) {
				t.Errorf("validateHeader() rt = %v, want %v", rt, tt.header.Get(WarcType))
			}
			if tt.wantValidationErr == nil && len(validation) > 0 {
				t.Errorf("validateHeader() unexpected validation error = %v", validation)
				return
			}
			if tt.wantValidationErr != nil {
				if len(validation) != 1 {
					t.Errorf("validateHeader() want single validation error = %v, got %v", tt.wantValidationErr, validation)
					return
				}
				err := validation[0]
				if err.Error() != tt.wantValidationErr.Error() {
					t.Errorf("validateHeader() got validation error = %v, want error %v", err.Error(), tt.wantValidationErr.Error())
				}
			}
		})
	}
}

func TestNormalizeName(t *testing.T) {
	type test struct {
		name string
		want string
	}
	var tests []test

	// Add all known headers as test cases
	for fieldName, def := range lcHdrNameToDef {
		tests = append(tests, test{fieldName, def.name})
	}

	tests = append(tests,
		test{
			// Test that unknown headers are normalized to title case
			name: "WARC-SOME-UNKNOWN-HEADER",
			want: "Warc-Some-Unknown-Header",
		},
	)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := normalizeName(tt.name)
			if got != tt.want {
				t.Errorf("normalizeName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidateHeader_DuplicateField_ErrFail(t *testing.T) {
	header := &WarcFields{
		&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
		&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
		&nameValue{Name: WarcType, Value: "warcinfo"},
		&nameValue{Name: ContentLength, Value: "0"},
		&nameValue{Name: ContentType, Value: "application/warc-fields"},
		&nameValue{Name: WarcDate, Value: "2017-04-06T04:03:53Z"}, // duplicate!
	}
	opts := newOptions(WithSpecViolationPolicy(ErrFail))
	_, _, err := validateHeader(header, V1_1, opts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "field occurs more than once")
}

func TestValidateHeader_DuplicateField_ErrWarn(t *testing.T) {
	header := &WarcFields{
		&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
		&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
		&nameValue{Name: WarcType, Value: "warcinfo"},
		&nameValue{Name: ContentLength, Value: "0"},
		&nameValue{Name: ContentType, Value: "application/warc-fields"},
		&nameValue{Name: WarcDate, Value: "2017-04-06T04:03:53Z"},
	}
	opts := newOptions(WithSpecViolationPolicy(ErrWarn))
	_, validation, err := validateHeader(header, V1_1, opts)
	require.NoError(t, err)
	assert.NotEmpty(t, validation)
	assert.Contains(t, fmt.Sprint(validation), "field occurs more than once")
}

func TestValidateHeader_IllegalConcurrentTo_Warcinfo_ErrFail(t *testing.T) {
	header := &WarcFields{
		&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
		&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
		&nameValue{Name: WarcType, Value: "warcinfo"},
		&nameValue{Name: ContentLength, Value: "0"},
		&nameValue{Name: ContentType, Value: "application/warc-fields"},
		&nameValue{Name: WarcConcurrentTo, Value: "<urn:uuid:fff0cecc-0221-11e7-adb1-0242ac120008>"},
	}
	opts := newOptions(WithSpecViolationPolicy(ErrFail))
	_, _, err := validateHeader(header, V1_1, opts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "illegal field")
}

func TestValidateHeader_IllegalConcurrentTo_Warcinfo_ErrWarn(t *testing.T) {
	header := &WarcFields{
		&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
		&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
		&nameValue{Name: WarcType, Value: "warcinfo"},
		&nameValue{Name: ContentLength, Value: "0"},
		&nameValue{Name: ContentType, Value: "application/warc-fields"},
		&nameValue{Name: WarcConcurrentTo, Value: "<urn:uuid:fff0cecc-0221-11e7-adb1-0242ac120008>"},
	}
	opts := newOptions(WithSpecViolationPolicy(ErrWarn))
	_, validation, err := validateHeader(header, V1_1, opts)
	require.NoError(t, err)
	assert.NotEmpty(t, validation)
}

func TestValidateHeader_MissingRequiredField_ErrWarn(t *testing.T) {
	header := &WarcFields{
		&nameValue{Name: WarcType, Value: "warcinfo"},
		&nameValue{Name: ContentLength, Value: "0"},
	}
	opts := newOptions(WithSpecViolationPolicy(ErrWarn))
	_, validation, err := validateHeader(header, V1_1, opts)
	require.NoError(t, err)
	assert.NotEmpty(t, validation)
	assert.Contains(t, fmt.Sprint(validation), "missing required field")
}

func TestValidateHeader_MissingContentType_ErrWarn(t *testing.T) {
	header := &WarcFields{
		&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
		&nameValue{Name: WarcRecordID, Value: "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>"},
		&nameValue{Name: WarcType, Value: "resource"},
		&nameValue{Name: ContentLength, Value: "249"},
	}
	opts := newOptions(WithSpecViolationPolicy(ErrWarn))
	_, validation, err := validateHeader(header, V1_1, opts)
	require.NoError(t, err)
	assert.NotEmpty(t, validation)
	assert.Contains(t, fmt.Sprint(validation), "missing required field: Content-Type")
}

func TestResolveRecordType_UnknownType_ErrWarn(t *testing.T) {
	header := &WarcFields{
		&nameValue{Name: WarcType, Value: "unknowntype"},
	}
	opts := newOptions(WithUnknownRecordTypePolicy(ErrWarn))
	rt, validation, err := resolveRecordType(header, opts)
	require.NoError(t, err)
	assert.Equal(t, RecordType(0), rt)
	assert.NotEmpty(t, validation)
}

func TestResolveRecordType_UnknownType_ErrFail(t *testing.T) {
	header := &WarcFields{
		&nameValue{Name: WarcType, Value: "unknowntype"},
	}
	opts := newOptions(WithUnknownRecordTypePolicy(ErrFail))
	_, _, err := resolveRecordType(header, opts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unrecognized value")
}

func TestResolveRecordType_MissingType_ErrWarn(t *testing.T) {
	header := &WarcFields{
		&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
	}
	opts := newOptions(WithSpecViolationPolicy(ErrWarn))
	_, validation, err := resolveRecordType(header, opts)
	require.NoError(t, err)
	assert.NotEmpty(t, validation)
}

func TestResolveRecordType_MissingType_ErrIgnore(t *testing.T) {
	header := &WarcFields{
		&nameValue{Name: WarcDate, Value: "2017-03-06T04:03:53Z"},
	}
	opts := newOptions(WithSpecViolationPolicy(ErrIgnore), WithUnknownRecordTypePolicy(ErrIgnore))
	rt, validation, err := resolveRecordType(header, opts)
	require.NoError(t, err)
	assert.Equal(t, RecordType(0), rt)
	assert.Empty(t, validation)
}

func TestCheckLegal_UnknownRecordType(t *testing.T) {
	opts := newOptions(WithSpecViolationPolicy(ErrFail))
	shouldValidate, err := checkLegal(opts, WarcDate, V1_1, 0, lcHdrNameToDef["warc-date"])
	require.NoError(t, err)
	assert.False(t, shouldValidate) // unknown record types skip validation
}

func TestCheckLegal_FieldNotInSpec(t *testing.T) {
	opts := newOptions(WithSpecViolationPolicy(ErrFail))
	// WarcRefersToDate is only in V1_1 not V1_0
	def := lcHdrNameToDef["warc-refers-to-date"]
	shouldValidate, err := checkLegal(opts, WarcRefersToDate, V1_0, Revisit, def)
	require.NoError(t, err)
	assert.False(t, shouldValidate)
}
