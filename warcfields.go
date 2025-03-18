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
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

type nameValue struct {
	Name  string
	Value string
}

func (n *nameValue) String() string {
	return n.Name + ": " + n.Value
}

// WarcFields represents the key value pairs in a WARC-record header.
//
// It is also used for representing the record block of records with content-type "application/warc-fields".
//
// All key-manipulating functions take case-insensitive keys and modify them to their canonical form.
type WarcFields []*nameValue

func (wf *WarcFields) CanonicalHeaderKey(s string) string {

	return http.CanonicalHeaderKey(s)
}

// Get gets the first value associated with the given key. It is case-insensitive.
// If the key doesn't exist or there are no values associated with the key, Get returns the empty string.
// To access multiple values of a key, use GetAll.
func (wf *WarcFields) Get(key string) string {
	key, _ = normalizeName(key)
	for _, nv := range *wf {
		if nv.Name == key {
			return nv.Value
		}
	}
	return ""
}

// GetInt is like Get, but converts the field value to int.
func (wf *WarcFields) GetInt(key string) (int, error) {
	if !wf.Has(key) {
		return 0, fmt.Errorf("missing field %s", key)
	}
	return strconv.Atoi(wf.Get(key))
}

// GetInt64 is like Get, but converts the field value to int64.
func (wf *WarcFields) GetInt64(name string) (int64, error) {
	if !wf.Has(name) {
		return 0, fmt.Errorf("missing field %s", name)
	}
	return strconv.ParseInt(wf.Get(name), 10, 64)
}

// GetTime is like Get, but converts the field value to time.Time.
// The field is expected to be in RFC 3339 format.
func (wf *WarcFields) GetTime(name string) (time.Time, error) {
	if !wf.Has(name) {
		return time.Time{}, fmt.Errorf("missing field %s", name)
	}
	return time.Parse(time.RFC3339, wf.Get(name))
}

// GetId is like Get, but removes the surrounding '<' and '>' from the field value.
func (wf *WarcFields) GetId(name string) string {
	return strings.Trim(wf.Get(name), "<>")
}

// GetAll returns all values associated with the given key. It is case-insensitive.
func (wf *WarcFields) GetAll(name string) []string {
	name, _ = normalizeName(name)
	var result []string
	for _, nv := range *wf {
		if nv.Name == name {
			result = append(result, nv.Value)
		}
	}
	return result
}

// Has returns true if field exists.
// This can be used to separate a missing field from a field for which value is the empty string.
func (wf *WarcFields) Has(name string) bool {
	name, _ = normalizeName(name)
	for _, nv := range *wf {
		if nv.Name == name {
			return true
		}
	}
	return false
}

// Add adds the key, value pair to the header.
// It appends to any existing values associated with key. The key is case-insensitive.
func (wf *WarcFields) Add(name string, value string) {
	name, _ = normalizeName(name)
	*wf = append(*wf, &nameValue{Name: name, Value: value})
}

// AddInt adds the key, value pair to the header.
// It appends to any existing values associated with key. The key is case-insensitive.
func (wf *WarcFields) AddInt(name string, value int) {
	wf.Add(name, strconv.Itoa(value))
}

// AddInt64 adds the key, value pair to the header.
// It appends to any existing values associated with key. The key is case-insensitive.
func (wf *WarcFields) AddInt64(name string, value int64) {
	wf.Add(name, strconv.FormatInt(value, 10))
}

// AddTime adds the key, value pair to the header.
// It appends to any existing values associated with key. The key is case-insensitive.
//
// The value is converted to RFC 3339 format.
func (wf *WarcFields) AddTime(name string, value time.Time) {
	wf.Add(name, value.UTC().Format(time.RFC3339))
}

// AddId adds the key, value pair to the header.
// It appends to any existing values associated with key. The key is case-insensitive.
//
// The value is surrounded with '<' and '>' if not already present.
func (wf *WarcFields) AddId(name, value string) {
	if len(value) == 0 {
		return
	}
	if value[0] != '<' && value[len(value)-1] != '>' {
		value = "<" + value + ">"
	}
	wf.Add(name, value)
}

// Set sets the header entries associated with key to the single element value.
// It replaces any existing values associated with key. The key is case-insensitive
func (wf *WarcFields) Set(name string, value string) {
	name, _ = normalizeName(name)
	isSet := false
	for idx, nv := range *wf {
		if nv.Name == name {
			if isSet {
				*wf = slices.Delete(*wf, idx, idx+1)
			} else {
				nv.Value = value
				isSet = true
			}
		}
	}
	if !isSet {
		*wf = append(*wf, &nameValue{Name: name, Value: value})
	}
}

// SetInt sets the header entries associated with key to the single element value.
// It replaces any existing values associated with key. The key is case-insensitive
func (wf *WarcFields) SetInt(name string, value int) {
	wf.Set(name, strconv.Itoa(value))
}

// SetInt64 sets the header entries associated with key to the single element value.
// It replaces any existing values associated with key. The key is case-insensitive
func (wf *WarcFields) SetInt64(name string, value int64) {
	wf.Set(name, strconv.FormatInt(value, 10))
}

// SetTime sets the header entries associated with key to the single element value.
// It replaces any existing values associated with key. The key is case-insensitive
//
// The value is converted to RFC 3339 format.
func (wf *WarcFields) SetTime(name string, value time.Time) {
	wf.Set(name, value.UTC().Format(time.RFC3339))
}

// SetId sets the header entries associated with key to the single element value.
// It replaces any existing values associated with key. The key is case-insensitive
//
// The value is surrounded with '<' and '>' if not already present.
func (wf *WarcFields) SetId(name, value string) {
	if len(value) == 0 {
		return
	}
	if value[0] != '<' && value[len(value)-1] != '>' {
		value = "<" + value + ">"
	}
	wf.Set(name, value)
}

// Delete deletes the values associated with key. The key is case-insensitive.
func (wf *WarcFields) Delete(key string) {
	key, _ = normalizeName(key)
	var result []*nameValue
	for _, nv := range *wf {
		if nv.Name != key {
			result = append(result, nv)
		}
	}
	*wf = result
}

// Sort sorts the fields in lexicographical order.
//
// Only field names are sorted. Order of values for a repeated field is kept as is.
func (wf *WarcFields) Sort() {
	sort.SliceStable(*wf, func(i, j int) bool {
		return (*wf)[i].Name < (*wf)[j].Name
	})
}

// Write implements the io.Writer interface.
func (wf *WarcFields) Write(w io.Writer) (bytesWritten int64, err error) {
	var n int
	for _, field := range *wf {
		n, err = fmt.Fprintf(w, "%s: %s\r\n", field.Name, field.Value)
		bytesWritten += int64(n)
		if err != nil {
			return
		}
	}
	return
}

func (wf *WarcFields) String() string {
	sb := &strings.Builder{}
	if _, err := wf.Write(sb); err != nil {
		panic(err)
	}
	return sb.String()
}

// clone creates a new deep copy.
func (wf WarcFields) clone() *WarcFields {
	r := WarcFields{}
	for _, p := range wf {
		v := *p
		v2 := v
		r = append(r, &v2)
	}
	return &r
}
