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
	"io"
	"sort"
	"strconv"
	"strings"
)

type NameValue struct {
	Name  string
	Value string
}

type warcFields []*NameValue

// Get gets the first value associated with the given key. It is case insensitive.
// If the key doesn't exist or there are no values associated with the key, Get returns "".
// To access multiple values of a key, use GetAll.
func (wf *warcFields) Get(name string) string {
	for _, nv := range *wf {
		if nv.Name == name {
			return nv.Value
		}
	}
	return ""
}

func (wf *warcFields) GetAll(name string) []string {
	var result []string
	for _, nv := range *wf {
		if nv.Name == name {
			result = append(result, nv.Value)
		}
	}
	return result
}

func (wf *warcFields) Has(name string) bool {
	for _, nv := range *wf {
		if nv.Name == name {
			return true
		}
	}
	return false
}

//func (wf *WarcFields) Names() []string {
//	var result []string
//	//keys := make([]string, len(wf))
//	for k := range wf.values {
//		keys[i] = k
//		i++
//	}
//	return result
//}

func (wf *warcFields) Add(name string, value string) error {
	*wf = append(*wf, &NameValue{Name: name, Value: value})
	return nil
}

func (wf *warcFields) Set(name string, value string) error {
	isSet := false
	for idx, nv := range *wf {
		if nv.Name == name {
			if isSet {
				*wf = append((*wf)[:idx], (*wf)[idx+1:]...)
			} else {
				nv.Value = value
				isSet = true
			}
		}
	}
	if !isSet {
		*wf = append(*wf, &NameValue{Name: name, Value: value})
	}
	return nil
}

func (wf *warcFields) Delete(name string) {
	var result []*NameValue
	for _, nv := range *wf {
		if nv.Name != name {
			result = append(result, nv)
		}
	}
	*wf = result
}

func (wf *warcFields) Sort() {
	sort.SliceStable(*wf, func(i, j int) bool {
		return (*wf)[i].Name < (*wf)[j].Name
	})
}

func (wf *warcFields) Write(w io.Writer) (bytesWritten int64, err error) {
	var n int
	for _, field := range *wf {
		n, err = fmt.Fprintf(w, "%v: %v\r\n", field.Name, field.Value)
		bytesWritten += int64(n)
		if err != nil {
			return
		}
	}
	return
}

func (wf *warcFields) String() string {
	sb := &strings.Builder{}
	wf.Write(sb)
	return sb.String()
}

// ValidateHeader validates a warcFields object as a WARC-record header
func (wf *warcFields) ValidateHeader(opts *options, version *version) (*recordType, error) {
	rt, err := wf.resolveRecordType(opts)
	if err != nil {
		return rt, err
	}

	for _, nv := range *wf {
		name, def := NormalizeName(nv.Name)
		value, err := def.validationFunc(opts, name, nv.Value, version, rt, def)
		nv.Name = name
		nv.Value = value
		if err != nil {
			return rt, err
		}
		if opts.strict && !def.repeatable && len(wf.GetAll(name)) > 1 {
			return rt, fmt.Errorf("field '%v' occurs more than once in record type '%v'", name, rt.txt)
		}
	}

	// Check for required fields
	for _, f := range requiredFields {
		if !wf.Has(f) {
			return rt, fmt.Errorf("missing required field: %s", f)
		}
	}
	contentLength, _ := strconv.ParseInt(wf.Get(ContentLength), 10, 64)
	if rt != CONTINUATION && contentLength > 0 && !wf.Has(ContentType) {
		return rt, fmt.Errorf("missing required field: %s", ContentType)
	}

	// Check for illegal fields
	if (WARCINFO.id|CONVERSION.id|CONTINUATION.id)&rt.id != 0 && wf.Has(WarcConcurrentTo) {
		return rt, fmt.Errorf("field %s not allowed for record type %s :: %b %b %b", WarcConcurrentTo, rt, (WARCINFO.id | CONVERSION.id | CONTINUATION.id), rt.id, (WARCINFO.id|CONVERSION.id|CONTINUATION.id)&rt.id)
	}
	return rt, nil
}

func (wf *warcFields) resolveRecordType(opts *options) (*recordType, error) {
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
		if opts.strict {
			return rt, errors.New("missing required field WARC-Type")
		}
	}
	typeFieldValLc := strings.ToLower(typeField)
	var ok bool
	rt, ok = recordTypeStringToType[typeFieldValLc]
	if !ok {
		rt = &recordType{id: 0, txt: typeField}
		if opts.strict {
			return rt, fmt.Errorf("unrecognized value in field WARC-Type '%s'", typeField)
		}
	}

	return rt, nil
}
