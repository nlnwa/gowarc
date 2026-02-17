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
	"compress/flate"
	"compress/gzip"
	"errors"
	"io"
	"mime"
)

var (
	colon           = []byte{':'}
	errEndOfHeaders = errors.New("EOH")
)

type warcfieldsParser struct {
	Options    *warcRecordOptions
	lineNumber int
}

// parseLine parses a single WARC header field line and adds it to nv.
//
// The line is expected to be a complete header field, with any line
// continuations already folded. RFC 2047 “encoded-word” decoding is
// applied before parsing the field name and value.
//
// On success, the header field is added to nv and the updated collection
// is returned. If the line cannot be parsed, a SyntaxError is returned
// describing the problem.
func (p *warcfieldsParser) parseLine(line []byte, nv WarcFields) (WarcFields, error) {
	line = bytes.TrimRight(line, sphtcrlf)

	// Support for 'encoded-word' mechanism of [RFC2047]
	d := mime.WordDecoder{}
	l, err := d.DecodeHeader(string(line))
	if err != nil {
		return nv, newWrappedSyntaxErrorAtLine("error decoding line", p.lineNumber, err)
	}
	line = []byte(l)

	fv := bytes.SplitN(line, colon, 2)
	if len(fv) != 2 {
		err = newSyntaxErrorAtLine("could not parse header line. Missing ':' in "+string(fv[0]), p.lineNumber)
		return nv, err
	}

	name := string(bytes.Trim(fv[0], sphtcrlf))
	value := string(bytes.Trim(fv[1], sphtcrlf))

	nv.Add(name, value)
	return nv, nil
}

// readLine reads the next line from r.
//
// It returns a line and an error. The error may represent a syntax problem
// (e.g. invalid line ending) or an underlying read error.
//
// On fatal read errors, line may still contain partial data; the caller
// decides whether that data can be used. When err is non-nil and line is
// non-nil, readLine has produced something that a lenient parser may
// still choose to consume.
//
// nextChar is the first byte that a subsequent call to readLine would
// process, or 0 if it could not be determined.
func (p *warcfieldsParser) readLine(r *bufio.Reader, checkCRLF bool) (line []byte, nextChar byte, err error) {
	line, err = r.ReadBytes('\n')

	// If underlying read had a fatal error, propagate it (but keep partial line trimmed).
	if isFatalReadErr(err) {
		line = bytes.Trim(line, sphtcrlf)
		return line, 0, err
	}

	// Non-fatal read error: maybe EOF (= end of headers), maybe something else.
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = errEndOfHeaders
		}
		line = bytes.Trim(line, sphtcrlf)
		return line, 0, err
	}

	// Strict CRLF check (only when we actually got '\n')
	if checkCRLF && p.Options.errSyntax > ErrIgnore && (len(line) < 2 || line[len(line)-2] != '\r') {
		err = newSyntaxErrorAtLine("missing carriage return", p.lineNumber)
		if p.Options.errSyntax == ErrFail {
			line = bytes.Trim(line, sphtcrlf)
			return line, 0, err
		}
	}
	line = bytes.Trim(line, sphtcrlf)

	// Peek next byte safely
	n, e := r.Peek(1)
	if e != nil {
		if errors.Is(e, io.EOF) {
			return line, 0, err
		}
		if isFatalReadErr(e) {
			return line, 0, e
		}
		// Non-fatal peek error: ignore it; Peek is advisory. Just no nextChar.
		return line, 0, err
	}

	if len(n) == 0 {
		return line, 0, err
	}

	return line, n[0], err
}

// Parse reads and parses a sequence of WARC header fields from r.
//
// It consumes header field lines until the end-of-fields marker (an empty
// line) is reached, and returns the parsed fields. Line continuations
// starting with space or horizontal tab are folded into the preceding
// header field.
//
// Parsing and validation errors are handled according to the configured
// syntax error policy. In lenient modes, Parse may return successfully
// with validation errors recorded even if syntactical issues were
// encountered.
//
// On fatal read errors, parsing stops immediately and the error is
// returned.
func (p *warcfieldsParser) Parse(r *bufio.Reader) (warcFields *WarcFields, validation []error, err error) {
	wf := WarcFields{}

	for {
		p.lineNumber++
		line, nc, err := p.readLine(r, true)

		if isFatalReadErr(err) {
			return nil, validation, err
		}

		if err == errEndOfHeaders {
			// EOF while reading a header line.
			if len(line) == 0 {
				return &wf, validation, nil
			}

			// Missing newline at end of last header line
			switch p.Options.errSyntax {
			case ErrIgnore:
			case ErrWarn:
				validation = append(validation, newSyntaxErrorAtLine("missing newline", p.lineNumber))
			case ErrFail:
				return nil, validation, newSyntaxErrorAtLine("missing newline", p.lineNumber)
			}

			// Parse the final line and we're done (no continuation possible at EOF).
			wf, perr := p.parseLine(line, wf)
			if perr != nil {
				switch p.Options.errSyntax {
				case ErrIgnore:
				case ErrWarn:
					validation = append(validation, perr)
				case ErrFail:
					return nil, validation, perr
				}
			}
			return &wf, validation, nil
		}

		// Non-EOH, non-fatal error on the line read (e.g. missing CR)
		if err != nil {
			switch p.Options.errSyntax {
			case ErrIgnore:
			case ErrWarn:
				validation = append(validation, err)
			case ErrFail:
				return nil, validation, err
			}
		}

		// Blank line ends WARC-Fields (EOH marker).
		// This also covers cases where lookahead (Peek) was unavailable and the
		// marker line got read as a normal line. We *must* do this before
		// continuation folding to avoid turning "" into " <stuff>".
		if len(line) == 0 {
			break
		}

		// Continuations: read subsequent lines starting with SP/HT and append
		for nc == sp || nc == ht {
			var l []byte
			p.lineNumber++
			l, nc, err = p.readLine(r, true)

			if isFatalReadErr(err) {
				return nil, validation, err
			}

			// If continuation read hit EOF, we can't continue; treat like missing newline/marker.
			if err == errEndOfHeaders {
				// Per the WARC specification, a continuation implies another line existed; at EOF this is truncated.
				switch p.Options.errSyntax {
				case ErrIgnore:
				case ErrWarn:
					validation = append(validation, newSyntaxErrorAtLine("unexpected end of headers in continuation", p.lineNumber))
				case ErrFail:
					return nil, validation, newSyntaxErrorAtLine("unexpected end of headers in continuation", p.lineNumber)
				}
				// Best effort: append what we got and then return after parsing.
				line = append(line, ' ')
				line = append(line, l...)
				nc = 0
				break
			}

			if err != nil {
				switch p.Options.errSyntax {
				case ErrIgnore:
				case ErrWarn:
					validation = append(validation, err)
				case ErrFail:
					return nil, validation, err
				}
			}

			line = append(line, ' ')
			line = append(line, l...)
		}

		// Parse header line
		wf, err = p.parseLine(line, wf)
		if err != nil {
			switch p.Options.errSyntax {
			case ErrIgnore:
			case ErrWarn:
				validation = append(validation, err)
			case ErrFail:
				return nil, validation, err
			}
		}

		// End-of-fields marker: a blank line.
		// We detect it by looking at the next char that would be read as a new line.
		if nc == cr || nc == lf {
			p.lineNumber++
			marker, _, mErr := p.readLine(r, false)

			if isFatalReadErr(mErr) {
				return nil, validation, mErr
			}

			// If we hit EOF instead of actually reading a blank marker line,
			// treat as missing EOH marker (policy-driven).
			if mErr == errEndOfHeaders {
				switch p.Options.errSyntax {
				case ErrIgnore:
					return &wf, validation, nil
				case ErrWarn:
					validation = append(validation, errors.New("missing End of WARC-Fields marker"))
					return &wf, validation, nil
				case ErrFail:
					return nil, validation, errors.New("missing End of WARC-Fields marker")
				}
			}

			// If marker line had a syntax error (e.g. missing CR), apply policy.
			if mErr != nil {
				switch p.Options.errSyntax {
				case ErrIgnore:
				case ErrWarn:
					validation = append(validation, mErr)
				case ErrFail:
					return nil, validation, mErr
				}
			}

			// Marker must be an empty line (readLine already trimmed CR/LF/space/ht).
			if len(marker) != 0 {
				switch p.Options.errSyntax {
				case ErrIgnore:
					// ignore and accept what we have
				case ErrWarn:
					validation = append(validation, errors.New("missing End of WARC-Fields marker"))
				case ErrFail:
					return nil, validation, errors.New("missing End of WARC-Fields marker")
				}
			}

			break
		}
	}

	return &wf, validation, nil
}

func isFatalReadErr(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, gzip.ErrChecksum) ||
		errors.Is(err, gzip.ErrHeader) ||
		errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	var cie flate.CorruptInputError
	return errors.As(err, &cie)
}
