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
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/nlnwa/gowarc/warcoptions"
	"io"
	"mime"
)

var (
	COLON        = []byte{':'}
	EndOfHeaders = errors.New("EOH")
)

const (
	SPHTCRLF = " \t\r\n"
	CR       = '\r'
	LF       = '\n'
	SP       = ' '
	HT       = '\t'
)

type Parser struct {
	Options *warcoptions.WarcOptions
	NewFunc func(nv []NameValue, ctx interface{}) (WarcFields, error)
}

func NewParser(options *warcoptions.WarcOptions) *Parser {
	return &Parser{Options: options}
}

func (p *Parser) parseLine(line []byte, nv []NameValue) ([]NameValue, error) {
	line = bytes.TrimRight(line, SPHTCRLF)

	// Support for ‘encoded-word’ mechanism of [RFC2047]
	d := mime.WordDecoder{}
	l, err := d.DecodeHeader(string(line))
	if err != nil {
		return nil, err
	}
	line = []byte(l)

	fv := bytes.SplitN(line, COLON, 2)
	if len(fv) != 2 {
		err = errors.New("could not parse header line. Missing ':' in " + string(fv[0]))
		return nil, err
	}

	name := string(bytes.Trim(fv[0], SPHTCRLF))
	value := string(bytes.Trim(fv[1], SPHTCRLF))

	nv = append(nv, NameValue{Name: name, Value: value})
	return nv, nil
}

func (p *Parser) readLine(r *bufio.Reader) (line []byte, next byte, err error) {
	l, err := r.ReadBytes('\n')
	if err != nil {
		if err == io.EOF {
			err = EndOfHeaders
		}
		return
	}
	if p.Options.Strict && l[len(l)-2] != '\r' {
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

func (p *Parser) Parse(r *bufio.Reader, ctx interface{}) (WarcFields, error) {
	nv := make([]NameValue, 0, 10)
	for {
		line, n, err := p.readLine(r)
		if err != nil {
			if err == EndOfHeaders {
				break
			}
			return nil, err
		}

		// Check for continuation
		for n == SP || n == HT {
			var l []byte
			l, n, err = p.readLine(r)
			if err != nil {
				return nil, err
			}
			line = append(line, ' ')
			line = append(line, l...)
		}

		nv, err = p.parseLine(line, nv)
		if err != nil {
			return nil, err
		}

		if n == CR {
			l, err := r.ReadBytes('\n')
			if len(l) != 2 || err != nil {
				return nil, errors.New("missing End of WARC-Fields marker")
			}
			break
		}
	}

	var err error
	if p.NewFunc == nil {
		wf := New()
		for _, f := range nv {
			err = wf.Add(f.Name, f.Value)
			if err != nil {
				return wf, err
			}
		}
		return wf, nil
	} else {
		return p.NewFunc(nv, ctx)
	}
}
