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

package warcrecord

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"mime"
)

var (
	COLON        = []byte{':'}
	EndOfHeaders = errors.New("EOH")
)

type warcfieldsParser struct {
	Options *options
}

func (p *warcfieldsParser) parseLine(line []byte, nv warcFields) (warcFields, error) {
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

	//nv = append(nv, &NameValue{Name: name, Value: value})
	nv.Add(name, value)
	return nv, nil
}

func (p *warcfieldsParser) readLine(r *bufio.Reader) (line []byte, nextChar byte, err error) {
	l, err := r.ReadBytes('\n')
	if err != nil {
		if err == io.EOF {
			err = EndOfHeaders
		}
		return
	}
	if p.Options.strict && l[len(l)-2] != '\r' {
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

	nextChar = n[0]
	return
}

func (p *warcfieldsParser) Parse(r *bufio.Reader, ctx interface{}) (*warcFields, error) {
	wf := warcFields{}
	//nv := make([]NameValue, 0, 10)
	for {
		line, nc, err := p.readLine(r)
		if err != nil {
			if err == EndOfHeaders {
				break
			}
			return nil, err
		}

		// Check for continuation
		for nc == SP || nc == HT {
			var l []byte
			l, nc, err = p.readLine(r)
			if err != nil {
				return nil, err
			}
			line = append(line, ' ')
			line = append(line, l...)
		}

		wf, err = p.parseLine(line, wf)
		if err != nil {
			return nil, err
		}

		if nc == CR {
			l, err := r.ReadBytes('\n')
			if len(l) != 2 || err != nil {
				return nil, errors.New("missing End of WARC-Fields marker")
			}
			break
		}
		// Handle missing carriage return in line endings
		if nc == LF {
			l, err := r.ReadBytes('\n')
			if len(l) > 2 || err != nil {
				return nil, errors.New("missing End of WARC-Fields marker")
			}
			break
		}
	}
	return &wf, nil
}
