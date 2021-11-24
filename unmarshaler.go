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
	"bufio"
	"bytes"
	"fmt"
	"github.com/klauspost/compress/gzip"
	"github.com/nlnwa/gowarc/internal/countingreader"
	"io"
	"io/ioutil"
	"strconv"
)

type Unmarshaler interface {
	Unmarshal(b *bufio.Reader) (WarcRecord, int64, *Validation, error)
}

type unmarshaler struct {
	opts             *warcRecordOptions
	warcFieldsParser *warcfieldsParser
	LastOffset       int64
	gz               *gzip.Reader // Holds gzip reader for enabling reuse
}

func NewUnmarshaler(opts ...WarcRecordOption) Unmarshaler {
	o := newOptions(opts...)

	u := &unmarshaler{
		opts:             o,
		warcFieldsParser: &warcfieldsParser{o},
	}
	return u
}

func (u *unmarshaler) Unmarshal(b *bufio.Reader) (WarcRecord, int64, *Validation, error) {
	var r *bufio.Reader
	var offset int64
	validation := &Validation{}
	isGzip := false

	magic, err := b.Peek(5)
	if err != nil {
		return nil, offset, validation, err
	}
	// Search for start of new record
	expectedRecordStartOffset := offset
	for !(magic[0] == 0x1f && magic[1] == 0x8b) && !bytes.Equal(magic, []byte("WARC/")) {
		if u.opts.errSyntax >= ErrFail {
			return nil, offset, validation, newSyntaxError("expected start of record", &position{})
		}
		if _, err = b.Discard(1); err != nil {
			return nil, offset, validation, err
		}
		offset++
		magic, err = b.Peek(5)
		if err != nil {
			return nil, offset, validation, err
		}
	}
	if u.opts.errSyntax >= ErrWarn && expectedRecordStartOffset != offset {
		validation.addError(newSyntaxError(
			fmt.Sprintf("expected start of record at offset: %d, but record was found at offset: %d",
				expectedRecordStartOffset, offset), &position{}))
	}

	if magic[0] == 0x1f && magic[1] == 0x8b {
		isGzip = true
		if u.gz == nil {
			u.gz, err = gzip.NewReader(b)
		} else {
			err = u.gz.Reset(b)
		}
		if err != nil {
			return nil, offset, validation, err
		}
		u.gz.Multistream(false)
		r = bufio.NewReader(u.gz)
	} else {
		r = b
	}

	// Find WARC version
	pos := &position{}
	l := make([]byte, 5)
	i, err := io.ReadFull(r, l)
	if err != nil {
		return nil, offset, validation, err
	}
	pos.incrLineNumber()
	if i != 5 || !bytes.Equal(l, []byte("WARC/")) {
		return nil, offset, validation, newSyntaxError("missing record version", pos)
	}
	l, err = r.ReadBytes('\n')
	if err != nil {
		return nil, offset, validation, err
	}
	if l[len(l)-2] != '\r' {
		switch u.opts.errSyntax {
		case ErrWarn:
			validation.addError(newSyntaxError(fmt.Sprintf("missing carriage return on line '%s'", bytes.Trim(l, sphtcrlf)), pos))
		case ErrFail:
			return nil, offset, validation, newSyntaxError(fmt.Sprintf("missing carriage return on line '%s'", bytes.Trim(l, sphtcrlf)), pos)
		}
	}
	version, err := u.resolveRecordVersion(string(bytes.Trim(l, sphtcrlf)), validation)
	if err != nil {
		return nil, offset, validation, err
	}

	// Parse WARC header
	wf, err := u.warcFieldsParser.Parse(r, validation, pos)
	if err != nil {
		return nil, offset, validation, err
	}
	rt, err := validateHeader(wf, version, validation, u.opts)
	if err != nil {
		return nil, offset, validation, err
	}

	record := &warcRecord{
		opts:       u.opts,
		version:    version,
		headers:    wf,
		recordType: rt,
		block:      nil,
		closer:     nil,
	}

	length, _ := strconv.ParseInt(record.headers.Get(ContentLength), 10, 64)

	content := countingreader.NewLimited(r, length)
	record.closer = func() error {
		_, err := io.Copy(ioutil.Discard, content)

		// Discarding 4 bytes which makes up the end of record marker (\r\n\r\n)
		b, e := r.Peek(4)
		switch {
		case string(b) == crlfcrlf:
			_, _ = r.Discard(4)
		case len(b) == 0:
			e = fmt.Errorf("too few bytes in end of record marker. Expected %q, was %q", crlfcrlf, b)
		case len(b) == 1 && b[0] == lf:
			e = fmt.Errorf("missing carriage return in end of record marker. Expected %q, was %q", crlfcrlf, b)
			_, _ = r.Discard(1)
		case len(b) == 2 && b[0] == lf && b[1] == lf:
			e = fmt.Errorf("missing carriage return in end of record marker. Expected %q, was %q", crlfcrlf, b)
			_, _ = r.Discard(2)
		case len(b) < 4:
			e = fmt.Errorf("too few bytes in end of record marker. Expected %q, was %q", crlfcrlf, b)
			_, _ = r.Discard(len(b))
		case e == io.EOF:
			_, _ = r.Discard(len(b))
		}
		if e != nil {
			switch u.opts.errSpec {
			case ErrFail:
				err = e
			case ErrWarn:
				validation.addError(e)
			}
		}
		if isGzip {
			// Empty gzip reader to ensure gzip checksum is validated
			b := make([]byte, 10)
			var err error
			for err == nil {
				_, err = u.gz.Read(b)
			}
			if err != io.EOF {
				_ = u.gz.Close()
				return err
			}
			if err := u.gz.Close(); err != nil {
				return err
			}
		}
		return err
	}

	err = record.parseBlock(bufio.NewReader(content), validation)

	return record, offset, validation, err
}

func (u *unmarshaler) resolveRecordVersion(s string, validation *Validation) (*WarcVersion, error) {
	switch s {
	case V1_0.txt:
		return V1_0, nil
	case V1_1.txt:
		return V1_1, nil
	default:
		switch u.opts.errSpec {
		case ErrWarn:
			validation.addError(fmt.Errorf("unsupported WARC version: %v", s))
			return &WarcVersion{txt: s}, nil
		case ErrFail:
			return nil, fmt.Errorf("unsupported WARC version: %v", s)
		default:
			return &WarcVersion{txt: s}, nil
		}
	}
}
