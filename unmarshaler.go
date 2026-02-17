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
	"errors"
	"fmt"
	"io"

	"github.com/klauspost/compress/gzip"
	"github.com/nlnwa/gowarc/v2/internal/countingreader"
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

func isGzipMagic(magic []byte) bool {
	return len(magic) >= 2 && magic[0] == 0x1f && magic[1] == 0x8b
}

func isWARCMagic(magic []byte) bool {
	return len(magic) >= 5 && bytes.Equal(magic[:5], warcMagic)
}

// Unmarshal implements the Unmarshal method in the Unmarshaler interface.
func (u *unmarshaler) Unmarshal(b *bufio.Reader) (rec WarcRecord, offset int64, validation *Validation, err error) {
	var r *bufio.Reader
	var vErr error
	validation = &Validation{}
	isGzip := false
	var buf []byte

	buf, err = b.Peek(5)
	if err != nil {
		return
	}

	// Search for start of new record
	for !isGzipMagic(buf) && !isWARCMagic(buf) {
		if u.opts.errSyntax >= ErrFail {
			err = newSyntaxError("expected start of record", &position{})
			return
		}
		if _, err = b.Discard(1); err != nil {
			return
		}
		offset++
		buf, err = b.Peek(5)
		if err != nil {
			return
		}
	}
	if u.opts.errSyntax >= ErrWarn && offset != 0 {
		validation.addError(newSyntaxError(
			fmt.Sprintf("record was found %d bytes after expected offset",
				offset), &position{}))
	}

	if isGzipMagic(buf) {
		isGzip = true
		if u.gz == nil {
			u.gz, err = gzip.NewReader(b)
		} else {
			err = u.gz.Reset(b)
		}
		if err != nil {
			return
		}
		u.gz.Multistream(false)

		defer func() {
			if err != nil {
				_ = u.gz.Close()
			}
		}()
		r = bufio.NewReader(u.gz)
	} else {
		r = b
	}

	pos := &position{}

	// Find WARC version
	buf, err = r.ReadBytes('\n')
	if err != nil {
		return
	}
	pos.incrLineNumber()

	if !isWARCMagic(buf) {
		err = newSyntaxError("missing record version", pos)
		return
	}

	var version *WarcVersion

	v := buf[5:]
	if bytes.HasPrefix(v, v1_0) {
		version = V1_0
	} else if bytes.HasPrefix(v, v1_1) {
		version = V1_1
	} else {
		version = &WarcVersion{txt: string(bytes.TrimSpace(v))}
		vErr = fmt.Errorf("unsupported WARC version: %v", version)

		switch u.opts.errSpec {
		case ErrWarn:
			validation.addError(vErr)
		case ErrFail:
			err = vErr
			return
		}
	}

	if !bytes.HasSuffix(buf, crlf) && u.opts.errSyntax > ErrIgnore {
		sErr := newSyntaxError(
			fmt.Sprintf("missing carriage return on line %q", bytes.TrimSpace(buf)),
			pos,
		)
		if u.opts.errSyntax == ErrFail {
			err = sErr
			return
		}
		validation.addError(sErr)
	}

	// Parse WARC header
	var wf *WarcFields
	wf, err = u.warcFieldsParser.Parse(r, validation, pos)
	if err != nil {
		return
	}
	var rt RecordType
	rt, err = validateHeader(wf, version, validation, u.opts)
	if err != nil {
		return
	}

	record := &warcRecord{
		opts:       u.opts,
		version:    version,
		headers:    wf,
		recordType: rt,
	}

	record.closer = func() error {
		if record.block != nil {
			return record.block.Close()
		}
		return nil
	}

	defer func() {
		if err != nil && record != nil {
			if cerr := record.Close(); cerr != nil {
				err = errors.Join(err, cerr)
			}
		}
	}()

	length, _ := record.headers.GetInt64(ContentLength)
	content := countingreader.NewLimited(r, length)

	err = record.parseBlock(content, validation)
	if err != nil {
		return
	}

	err = record.ValidateDigest(validation)
	if err != nil {
		return
	}

	// Discard any remaining bytes in block not read by parseBlock
	_, err = io.Copy(io.Discard, content)
	if err != nil {
		return
	}

	// Validate end of record marker
	buf, vErr = r.Peek(4)
	if bytes.Equal(buf, crlfcrlf) {
		_, _ = r.Discard(4)
	} else if len(buf) == 0 {
		vErr = fmt.Errorf("too few bytes in end of record marker. Expected %q, was %q", crlfcrlf, buf)
	} else if len(buf) == 1 && buf[0] == lf {
		vErr = fmt.Errorf("missing carriage return in end of record marker. Expected %q, was %q", crlfcrlf, buf)
		_, _ = r.Discard(1)
	} else if len(buf) == 2 && buf[0] == lf && buf[1] == lf {
		vErr = fmt.Errorf("missing carriage return in end of record marker. Expected %q, was %q", crlfcrlf, buf)
		_, _ = r.Discard(2)
	} else if len(buf) < 4 {
		vErr = fmt.Errorf("too few bytes in end of record marker. Expected %q, was %q", crlfcrlf, buf)
		_, _ = r.Discard(len(buf))
	} else if err == io.EOF {
		vErr = fmt.Errorf("unexpected end of record. Expected %q, was %q", crlfcrlf, buf)
		_, _ = r.Discard(len(buf))
	}
	if vErr != nil {
		switch u.opts.errSpec {
		case ErrFail:
			err = vErr
			return
		case ErrWarn:
			validation.addError(vErr)
		}
	}
	if isGzip {
		// Drain gzip reader to ensure gzip checksum is validated
		_, err = io.Copy(io.Discard, u.gz)
		if errors.Is(err, io.EOF) {
			err = nil
		}
		if cerr := u.gz.Close(); err != nil || cerr != nil {
			err = errors.Join(err, cerr)
			return
		}
	}

	rec = record
	return
}
