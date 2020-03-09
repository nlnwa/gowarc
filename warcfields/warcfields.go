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

package warcfields

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

type WarcFields interface {
	Get(name string) string
	GetAll(name string) []string
	Has(name string) bool
	Add(name string, value string) error
	Set(name string, value string) error
	Delete(name string)
	Sort()
	Write(w io.Writer) (bytesWritten int64, err error)
}

type NameValue struct {
	Name  string
	Value string
}

type warcFields struct {
	values []NameValue
}

func New() WarcFields {
	return &warcFields{
		values: make([]NameValue, 0, 10),
	}
}

// Get gets the first value associated with the given key. It is case insensitive.
// If the key doesn't exist or there are no values associated with the key, Get returns "".
// To access multiple values of a key, use GetAll.
func (wf *warcFields) Get(name string) string {
	for _, nv := range wf.values {
		if nv.Name == name {
			return nv.Value
		}
	}
	return ""
}

func (wf *warcFields) GetAll(name string) []string {
	var result []string
	for _, nv := range wf.values {
		if nv.Name == name {
			result = append(result, nv.Value)
		}
	}
	return result
}

func (wf *warcFields) Has(name string) bool {
	for _, nv := range wf.values {
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
	wf.values = append(wf.values, NameValue{Name: name, Value: value})
	return nil
}

func (wf *warcFields) Set(name string, value string) error {
	isSet := false
	for idx, nv := range wf.values {
		if nv.Name == name {
			if isSet {
				wf.values = append(wf.values[:idx], wf.values[idx+1:]...)
			} else {
				nv.Value = value
				isSet = true
			}
		}
	}
	if !isSet {
		wf.values = append(wf.values, NameValue{Name: name, Value: value})
	}
	return nil
}

func (wf *warcFields) Delete(name string) {
	var result []NameValue
	for _, nv := range wf.values {
		if nv.Name != name {
			result = append(result, nv)
		}
	}
	wf.values = result
}

func (wf *warcFields) Sort() {
	sort.SliceStable(wf.values, func(i, j int) bool {
		return wf.values[i].Name < wf.values[j].Name
	})
}

func (wf *warcFields) Write(w io.Writer) (bytesWritten int64, err error) {
	var n int
	for _, field := range wf.values {
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
