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
	"errors"
	"fmt"
	"io"
	"mime"
	"strings"
)

var (
	COLON        = []byte{':'}
	EndOfHeaders = errors.New("EOH")
)

type namedValues struct {
	name  string
	value []string
}

type WarcFields map[string]namedValues

// Get gets the first value associated with the given key. It is case insensitive.
// If the key doesn't exist or there are no values associated with the key, Get returns "".
// To access multiple values of a key, use GetAll.
func (wf WarcFields) Get(name string) string      { return wf[name].value[0] }
func (wf WarcFields) GetAll(name string) []string { return wf[name].value }
func (wf WarcFields) Has(name string) bool {
	_, ok := wf[name]
	return ok
}
func (wf WarcFields) Names() []string {
	keys := make([]string, len(wf))
	i := 0
	for k := range wf {
		keys[i] = k
		i++
	}
	return keys
}
func (wf WarcFields) Add(name string, value string) {
	lcName := strings.ToLower(name)
	if nf, ok := wf[lcName]; ok {
		nf.value = append(nf.value, value)
		wf[lcName] = nf
	} else {
		nf = namedValues{name: name, value: []string{value}}
		wf[lcName] = nf
	}
}
func (wf WarcFields) AddAll(nameVal namedValues) {
	lcName := strings.ToLower(nameVal.name)
	if nf, ok := wf[lcName]; ok {
		nf.value = append(nf.value, nameVal.value...)
		wf[lcName] = nf
	} else {
		nf = namedValues{name: nameVal.name, value: nameVal.value}
		wf[lcName] = nf
	}
}
func (wf WarcFields) Del(name string) {
	lcName := strings.ToLower(name)
	delete(wf, lcName)
}

func NewWarcFields() WarcFields {
	return make(map[string]namedValues)
}

type warcFieldsParser struct {
	opts *WarcReaderOpts
}

func newWarcfieldParser(opts *WarcReaderOpts) *warcFieldsParser {
	return &warcFieldsParser{opts}
}

func (wfp *warcFieldsParser) parseLine(line []byte) (name string, value string, err error) {
	line = bytes.TrimRight(line, SPHTCRLF)

	// Support for ‘encoded-word’ mechanism of [RFC2047]
	d := mime.WordDecoder{}
	l, err := d.DecodeHeader(string(line))
	line = []byte(l)

	fv := bytes.SplitN(line, COLON, 2)
	if len(fv) != 2 {
		err = errors.New("could not parse header line. Missing ':' in " + string(fv[0]))
		return
	}

	name = string(bytes.Trim(fv[0], SPHTCRLF))
	value = string(bytes.Trim(fv[1], SPHTCRLF))

	return
}

func (wfp *warcFieldsParser) readLine(r *bufio.Reader) (line []byte, next byte, err error) {
	l, err := r.ReadBytes('\n')
	if err != nil {
		if err == io.EOF {
			err = EndOfHeaders
		}
		return
	}
	if wfp.opts.Strict && l[len(l)-2] != '\r' {
		err = fmt.Errorf("missing carriage return on line '%s'", bytes.Trim(l, SPHTCRLF))
		return
	}
	line = bytes.Trim(l, SPHTCRLF)

	n, err := r.Peek(1)
	if err == io.EOF {
		return line, 0, nil
	}
	if err != nil {
		return
	}

	next = n[0]
	return
}

func (wfp *warcFieldsParser) parse(r *bufio.Reader) (WarcFields, error) {
	fields := NewWarcFields()

	for {
		line, n, err := wfp.readLine(r)
		if err != nil {
			if err == EndOfHeaders {
				return fields, nil
			}
			return fields, err
		}

		// Check for continuation
		for n == SP || n == HT {
			var l []byte
			l, n, err = wfp.readLine(r)
			if err != nil {
				return fields, err
			}
			line = append(line, ' ')
			line = append(line, l...)
		}

		name, value, err := wfp.parseLine(line)

		fields.Add(name, value)

		if n == CR {
			l, err := r.ReadBytes('\n')
			if len(l) != 2 || err != nil {
				return fields, errors.New("missing End of WARC-Fields marker")
			}
			break
		}
	}

	return fields, nil
}
