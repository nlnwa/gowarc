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
	"math"
	"net/http"

	"github.com/nlnwa/gowarc/v3/internal/diskbuffer"
)

type HttpRequestBlock interface {
	PayloadBlock
	ProtocolHeaderBlock
	HttpRequestLine() string
	HttpHeader() *http.Header
}

type HttpResponseBlock interface {
	PayloadBlock
	ProtocolHeaderBlock
	HttpStatusLine() string
	HttpStatusCode() int
	HttpHeader() *http.Header
}

var errMissingEndOfHeaders = errors.New("missing line separator at end of http headers")

// baseHttpBlock contains common fields and methods for HTTP request and response blocks
type baseHttpBlock struct {
	*genericBlock
	httpHeaderBytes     []byte
	payload             io.Reader
	payloadDigestString string
	httpHeader          *http.Header
	payloadDigest       *digest
}

type httpRequestBlock struct {
	*baseHttpBlock
	httpRequestLine string
}

type httpResponseBlock struct {
	*baseHttpBlock
	httpStatusLine string
	httpStatusCode int
}

// Common methods for baseHttpBlock

func (block *baseHttpBlock) IsCached() bool {
	_, ok := block.payload.(io.Seeker)
	return ok
}

func (block *baseHttpBlock) Cache() error {
	if block.IsCached() {
		return nil
	}

	r, err := block.PayloadBytes()
	if err != nil {
		return err
	}

	buf := diskbuffer.New(block.opts.bufferOptions...)
	_, err = buf.ReadFrom(r)
	if c, ok := block.payload.(io.Closer); ok {
		_ = c.Close()
	}
	block.blockDigestString = block.blockDigest.format()
	block.payloadDigestString = block.payloadDigest.format()
	block.payload = buf
	return err
}

func (block *baseHttpBlock) Close() error {
	if c, ok := block.payload.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

func (block *baseHttpBlock) RawBytes() (io.Reader, error) {
	r, err := block.PayloadBytes()
	if err != nil {
		return nil, err
	}
	return io.MultiReader(bytes.NewReader(block.httpHeaderBytes), r), nil
}

func (block *baseHttpBlock) BlockDigest() string {
	if block.blockDigestString == "" {
		if block.filterReader == nil {
			block.filterReader = newDigestFilterReader(block.payload, block.blockDigest, block.payloadDigest)
		}
		_, _ = io.Copy(io.Discard, block.filterReader)
		block.blockDigestString = block.blockDigest.format()
		block.payloadDigestString = block.payloadDigest.format()
	}
	return block.blockDigestString
}

func (block *baseHttpBlock) PayloadBytes() (io.Reader, error) {
	if block.filterReader == nil {
		block.filterReader = newDigestFilterReader(block.payload, block.blockDigest, block.payloadDigest)
		return block.filterReader, nil
	}

	if block.blockDigestString == "" {
		block.BlockDigest()
	}

	if !block.IsCached() {
		return nil, errContentReAccessed
	}

	if _, err := block.payload.(io.Seeker).Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return newDigestFilterReader(block.payload), nil
}

func (block *baseHttpBlock) PayloadDigest() string {
	block.BlockDigest()
	return block.payloadDigestString
}

// ProtocolHeaderBytes implements ProtocolHeaderBlock
func (block *baseHttpBlock) ProtocolHeaderBytes() []byte {
	return block.httpHeaderBytes
}

func (block *baseHttpBlock) HttpHeader() *http.Header {
	return block.httpHeader
}

// Request-specific methods

func (block *httpRequestBlock) HttpRequestLine() string {
	return block.httpRequestLine
}

func (block *httpRequestBlock) parseHeaders(headerBytes []byte) (err error) {
	request, e := http.ReadRequest(bufio.NewReader(bytes.NewReader(headerBytes)))
	if e != nil {
		err = e
		return
	}
	block.httpRequestLine = request.RequestURI
	block.httpHeader = &request.Header
	return
}

func (block *httpRequestBlock) Write(w io.Writer) (int64, error) {
	p, err := block.RawBytes()
	if err != nil {
		return 0, err
	}
	bytesWritten, err := io.Copy(w, p)
	if err != nil {
		return bytesWritten, err
	}
	_, err = w.Write(crlf)
	bytesWritten += 2
	return bytesWritten, err
}

// Response-specific methods

func (block *httpResponseBlock) HttpStatusLine() string {
	return block.httpStatusLine
}

func (block *httpResponseBlock) HttpStatusCode() int {
	return block.httpStatusCode
}

func (block *httpResponseBlock) parseHeaders(headerBytes []byte) (err error) {
	response, e := http.ReadResponse(bufio.NewReader(bytes.NewReader(headerBytes)), nil)
	if e != nil {
		err = e
		return
	}
	block.httpStatusLine = response.Status
	block.httpStatusCode = response.StatusCode
	block.httpHeader = &response.Header
	return
}

func (block *httpResponseBlock) Write(w io.Writer) (int64, error) {
	p, err := block.RawBytes()
	if err != nil {
		return 0, err
	}
	bytesWritten, err := io.Copy(w, p)
	if err != nil {
		return bytesWritten, err
	}
	_, err = w.Write(crlf)
	bytesWritten += 2
	return bytesWritten, err
}

// headerBytes reads the http-headers into a byte array.
func headerBytes(r buffer) ([]byte, error) {
	sepFound := false
	result := bytes.Buffer{}
	for {
		line, err := r.ReadBytes('\n')
		result.Write(line)
		if err != nil {
			break
		}
		if len(line) < 3 {
			sepFound = true
			break
		}
	}
	var err error
	if !sepFound {
		err = errMissingEndOfHeaders
	}
	return result.Bytes(), err
}

type buffer interface {
	Read(p []byte) (n int, err error)
	ReadBytes(delim byte) ([]byte, error)
	Peek(n int) ([]byte, error)
}

func newHttpBlock(opts *warcRecordOptions, wf *WarcFields, r io.Reader, blockDigest, payloadDigest *digest) (block PayloadBlock, validation []error, err error) {
	var rb buffer
	if v, ok := r.(diskbuffer.Buffer); ok {
		rb = v
	} else {
		rb = bufio.NewReader(r)
	}

	_, err = rb.Peek(4)
	if err != nil {
		return nil, nil, fmt.Errorf("not a http block: %w", err)
	}

	hb, herr := headerBytes(rb)
	if herr != nil {
		switch opts.errSyntax {
		case ErrWarn:
			validation = append(validation, herr)
		case ErrFail:
			return nil, validation, herr
		}
	}

	if herr == errMissingEndOfHeaders && opts.fixSyntaxErrors {
		// Fix header and update content-length field
		hb = append(hb, '\r', '\n')
		l, _ := wf.GetInt64(ContentLength)
		wf.SetInt64(ContentLength, l+2)
	}

	if _, err := blockDigest.Write(hb); err != nil {
		return nil, validation, err
	}

	var payload io.Reader
	if buf, ok := rb.(diskbuffer.Buffer); ok {
		payload = io.NewSectionReader(buf, int64(len(hb)), math.MaxInt64)
	} else {
		payload = rb
	}

	if bytes.HasPrefix(hb, []byte("HTTP")) {
		resp := &httpResponseBlock{
			baseHttpBlock: &baseHttpBlock{
				genericBlock: &genericBlock{
					opts:        opts,
					blockDigest: blockDigest,
				},
				httpHeaderBytes: hb,
				payload:         payload,
				payloadDigest:   payloadDigest,
			},
		}

		if herr == errMissingEndOfHeaders && !opts.fixSyntaxErrors {
			// We have to fix the header for parsing even if we don't fix the record
			hb = append(hb, '\r', '\n')
		}
		if err := resp.parseHeaders(hb); err != nil && opts.errBlock > ErrIgnore {
			err = fmt.Errorf("error in http response block: %w", err)
			if opts.errBlock == ErrWarn {
				validation = append(validation, err)
			} else {
				return resp, validation, err
			}
		}
		return resp, validation, nil
	} else {
		resp := &httpRequestBlock{
			baseHttpBlock: &baseHttpBlock{
				genericBlock: &genericBlock{
					opts:        opts,
					blockDigest: blockDigest,
				},
				httpHeaderBytes: hb,
				payload:         payload,
				payloadDigest:   payloadDigest,
			},
		}

		if herr == errMissingEndOfHeaders && !opts.fixSyntaxErrors {
			// We have to fix the header for parsing even if we don't fix the record
			hb = append(hb, '\r', '\n')
		}
		if err := resp.parseHeaders(hb); err != nil && opts.errBlock > ErrIgnore {
			err = fmt.Errorf("error in http request block: %w", err)
			if opts.errBlock == ErrWarn {
				validation = append(validation, err)
			} else {
				return resp, validation, err
			}
		}
		return resp, validation, nil
	}
}
