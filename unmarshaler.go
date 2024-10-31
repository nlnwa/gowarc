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
	"io"

	"github.com/klauspost/compress/gzip"
	"github.com/nlnwa/gowarc/internal/countingreader"
)

// Unmarshaler is the interface implemented by types that can unmarshal a WARC record. A new instance of Unmarshaler is created by calling [NewUnmarshaler].
// NewUnmarshaler accepts a number of options that can be used to control the unmarshalling process. See [WarcRecordOption] for details.
//
// Unmarshal parses the WARC record from the given reader and returns:
//   - The parsed WARC record. If an error occurred during the parsing, the returned WARC record might be nil.
//   - The offset value indicating the number of characters that have been discarded until the start of a new record is found.
//   - A pointer to a [Validation] object that stores any errors or warnings encountered during the parsing process.
//     The validation object is only populated if the error specification is set to ErrWarn or ErrFail.
//   - The standard error object in Go. If no error occurred during the parsing, this object is nil. Otherwise, it contains details about the encountered error.
//
// If the reader contains multiple records, Unmarshal parses the first record and returns.
// If the reader contains no records, Unmarshal returns an [io.EOF] error.
type Unmarshaler interface {
	Unmarshal(b *bufio.Reader) (WarcRecord, int64, *Validation, error)
}

// unmarshaler implements the Unmarshaler interface.
type unmarshaler struct {
	opts             *warcRecordOptions
	warcFieldsParser *warcfieldsParser
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

// Unmarshal implements the Unmarshal method in the Unmarshaler interface.
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
	if u.opts.errSyntax >= ErrWarn && offset != 0 {
		validation.addError(newSyntaxError(
			fmt.Sprintf("record was found %d bytes after expected offset",
				offset), &position{}))
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

	record.closer = func() error {
		if record.block != nil {
			return record.block.Close()
		}
		return nil
	}

	length, _ := record.headers.GetInt64(ContentLength)
	content := countingreader.NewLimited(r, length)

	err = record.parseBlock(bufio.NewReader(content), validation)
	if err != nil {
		return record, offset, validation, err
	}

	err = record.ValidateDigest(validation)
	if err != nil {
		return record, offset, validation, err
	}

	// Discard any remaining bytes in block not read by parseBlock
	_, err = io.Copy(io.Discard, content)
	if err != nil {
		return record, offset, validation, err
	}

	// Validate end of record marker
	buf, err := r.Peek(4)
	if string(buf) == crlfcrlf {
		_, _ = r.Discard(4)
	} else if len(buf) == 0 {
		err = fmt.Errorf("too few bytes in end of record marker. Expected %q, was %q", crlfcrlf, buf)
	} else if len(buf) == 1 && buf[0] == lf {
		err = fmt.Errorf("missing carriage return in end of record marker. Expected %q, was %q", crlfcrlf, buf)
		_, _ = r.Discard(1)
	} else if len(buf) == 2 && buf[0] == lf && buf[1] == lf {
		err = fmt.Errorf("missing carriage return in end of record marker. Expected %q, was %q", crlfcrlf, buf)
		_, _ = r.Discard(2)
	} else if len(buf) < 4 {
		err = fmt.Errorf("too few bytes in end of record marker. Expected %q, was %q", crlfcrlf, buf)
		_, _ = r.Discard(len(buf))
	} else if err == io.EOF {
		err = fmt.Errorf("unexpected end of record. Expected %q, was %q", crlfcrlf, buf)
		_, _ = r.Discard(len(buf))
	}
	if err != nil {
		switch u.opts.errSpec {
		case ErrFail:
			return record, offset, validation, err
		case ErrWarn:
			validation.addError(err)
		}
	}
	if isGzip {
		// Empty gzip reader to ensure gzip checksum is validated
		_, err = io.Copy(io.Discard, u.gz)
		if err != io.EOF {
			_ = u.gz.Close()
			return record, offset, validation, err
		}
		if err := u.gz.Close(); err != nil {
			return record, offset, validation, err
		}
	}

	return record, offset, validation, nil
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
