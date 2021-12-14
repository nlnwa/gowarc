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
	"github.com/nlnwa/gowarc/internal/diskbuffer"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
)

type HttpRequestBlock interface {
	PayloadBlock
	HttpRequestLine() string
	HttpHeader() *http.Header
	HttpHeaderBytes() []byte
}

type HttpResponseBlock interface {
	PayloadBlock
	HttpStatusLine() string
	HttpStatusCode() int
	HttpHeader() *http.Header
	HttpHeaderBytes() []byte
}

type httpRequestBlock struct {
	opts                *warcRecordOptions
	httpRequestLine     string
	httpHeader          *http.Header
	httpHeaderBytes     []byte
	payload             io.Reader
	blockDigest         *digest
	payloadDigest       *digest
	filterReader        *digestFilterReader
	blockDigestString   string
	payloadDigestString string
	parseHeaderOnce     sync.Once
}

func (block *httpRequestBlock) IsCached() bool {
	_, ok := block.payload.(io.Seeker)
	return ok
}

func (block *httpRequestBlock) Cache() error {
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

func (block *httpRequestBlock) RawBytes() (io.Reader, error) {
	r, err := block.PayloadBytes()
	if err != nil {
		return nil, err
	}
	return io.MultiReader(bytes.NewReader(block.httpHeaderBytes), r), nil
}

func (block *httpRequestBlock) BlockDigest() string {
	if block.blockDigestString == "" {
		if block.filterReader == nil {
			block.filterReader = newDigestFilterReader(block.payload, block.blockDigest, block.payloadDigest)
		}
		_, _ = io.Copy(ioutil.Discard, block.filterReader)
		block.blockDigestString = block.blockDigest.format()
		block.payloadDigestString = block.payloadDigest.format()
	}
	return block.blockDigestString
}

func (block *httpRequestBlock) Size() int64 {
	block.BlockDigest()
	return block.blockDigest.count
}

func (block *httpRequestBlock) PayloadBytes() (io.Reader, error) {
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

func (block *httpRequestBlock) PayloadDigest() string {
	block.BlockDigest()
	return block.payloadDigestString
}

func (block *httpRequestBlock) HttpHeaderBytes() []byte {
	return block.httpHeaderBytes
}

func (block *httpRequestBlock) HttpRequestLine() string {
	if err := block.parseHeaders(); err != nil {
		panic(err)
	}
	return block.httpRequestLine
}

func (block *httpRequestBlock) HttpHeader() *http.Header {
	if err := block.parseHeaders(); err != nil {
		panic(err)
	}
	return block.httpHeader
}

func (block *httpRequestBlock) parseHeaders() (err error) {
	block.parseHeaderOnce.Do(func() {
		request, e := http.ReadRequest(bufio.NewReader(bytes.NewReader(block.httpHeaderBytes)))
		if e != nil {
			err = e
			return
		}
		block.httpRequestLine = request.RequestURI
		block.httpHeader = &request.Header
	})
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
	_, err = w.Write([]byte(crlf))
	bytesWritten += 2
	return bytesWritten, err
}

type httpResponseBlock struct {
	opts                *warcRecordOptions
	httpStatusLine      string
	httpStatusCode      int
	httpHeader          *http.Header
	httpHeaderBytes     []byte
	payload             io.Reader
	blockDigest         *digest
	payloadDigest       *digest
	filterReader        *digestFilterReader
	blockDigestString   string
	payloadDigestString string
	parseHeaderOnce     sync.Once
}

func (block *httpResponseBlock) IsCached() bool {
	_, ok := block.payload.(io.Seeker)
	return ok
}

func (block *httpResponseBlock) Cache() error {
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

func (block *httpResponseBlock) RawBytes() (io.Reader, error) {
	r, err := block.PayloadBytes()
	if err != nil {
		return nil, err
	}
	return io.MultiReader(bytes.NewReader(block.httpHeaderBytes), r), nil
}

func (block *httpResponseBlock) BlockDigest() string {
	if block.blockDigestString == "" {
		if block.filterReader == nil {
			block.filterReader = newDigestFilterReader(block.payload, block.blockDigest, block.payloadDigest)
		}
		_, _ = io.Copy(ioutil.Discard, block.filterReader)
		block.blockDigestString = block.blockDigest.format()
		block.payloadDigestString = block.payloadDigest.format()
	}
	return block.blockDigestString
}

func (block *httpResponseBlock) Size() int64 {
	block.BlockDigest()
	return block.blockDigest.count
}

func (block *httpResponseBlock) PayloadBytes() (io.Reader, error) {
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

func (block *httpResponseBlock) PayloadDigest() string {
	block.BlockDigest()
	return block.payloadDigestString
}

func (block *httpResponseBlock) HttpHeaderBytes() []byte {
	return block.httpHeaderBytes
}

func (block *httpResponseBlock) HttpStatusLine() string {
	if err := block.parseHeaders(); err != nil {
		panic(err)
	}
	return block.httpStatusLine
}

func (block *httpResponseBlock) HttpStatusCode() int {
	if err := block.parseHeaders(); err != nil {
		panic(err)
	}
	return block.httpStatusCode
}

func (block *httpResponseBlock) HttpHeader() *http.Header {
	if err := block.parseHeaders(); err != nil {
		panic(err)
	}
	return block.httpHeader
}

func (block *httpResponseBlock) parseHeaders() (err error) {
	block.parseHeaderOnce.Do(func() {
		response, e := http.ReadResponse(bufio.NewReader(bytes.NewReader(block.httpHeaderBytes)), nil)
		if e != nil {
			err = e
			return
		}
		block.httpStatusLine = response.Status
		block.httpStatusCode = response.StatusCode
		block.httpHeader = &response.Header
	})
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
	_, err = w.Write([]byte(crlf))
	bytesWritten += 2
	return bytesWritten, err
}

// headerBytes reads the http-headers into a byte array.
func headerBytes(r buffer) []byte {
	result := bytes.Buffer{}
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			break
		}
		result.Write(line)
		if len(line) < 3 {
			break
		}
	}
	return result.Bytes()
}

type buffer interface {
	Read(p []byte) (n int, err error)
	ReadBytes(delim byte) ([]byte, error)
	Peek(n int) ([]byte, error)
}

func newHttpBlock(opts *warcRecordOptions, r io.Reader, blockDigest, payloadDigest *digest) (PayloadBlock, error) {
	var rb buffer
	if v, ok := r.(diskbuffer.Buffer); ok {
		rb = v
	} else {
		rb = bufio.NewReader(r)
	}

	b, err := rb.Peek(4)
	if err != nil {
		return nil, fmt.Errorf("not a http block: %w", err)
	}

	hb := headerBytes(rb)
	if _, err := blockDigest.Write(hb); err != nil {
		return nil, err
	}

	var payload buffer
	if _, ok := rb.(diskbuffer.Buffer); ok {
		payload = rb.(diskbuffer.Buffer).Slice(int64(len(hb)), 0)
	} else {
		payload = rb
	}

	if bytes.HasPrefix(b, []byte("HTTP")) {
		resp := &httpResponseBlock{
			opts:            opts,
			httpHeaderBytes: hb,
			payload:         payload,
			blockDigest:     blockDigest,
			payloadDigest:   payloadDigest,
		}
		return resp, nil
	} else {
		resp := &httpRequestBlock{
			opts:            opts,
			httpHeaderBytes: hb,
			payload:         payload,
			blockDigest:     blockDigest,
			payloadDigest:   payloadDigest,
		}
		return resp, nil
	}
}
