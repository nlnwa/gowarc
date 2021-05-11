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

package warcrecord

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"fmt"
	"hash"
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
	httpRequestLine string
	httpHeader      *http.Header
	httpHeaderBytes []byte
	payload         io.Reader
	blockDigest     hash.Hash
	payloadDigest   hash.Hash
	readOp          readOp
	parseHeaderOnce sync.Once
}

func (block *httpRequestBlock) RawBytes() (io.Reader, error) {
	if block.readOp != opInitial {
		return nil, errContentReAccessed
	}
	block.readOp = opRawBytes
	return io.MultiReader(bytes.NewReader(block.httpHeaderBytes), block.payload), nil
}

func (block *httpRequestBlock) BlockDigest() string {
	//if block.readOp == opInitial {
	//	block.RawBytes()
	//}
	block.readOp = opRawBytes
	io.Copy(ioutil.Discard, block.payload)
	h := block.blockDigest.Sum(nil)
	return fmt.Sprintf("request digest %x", h)
	//return "request digest"
}

func (block *httpRequestBlock) PayloadBytes() (io.Reader, error) {
	if block.readOp != opInitial {
		return nil, errContentReAccessed
	}
	block.readOp = opPayloadBytes
	return block.payload, nil
}

func (block *httpRequestBlock) PayloadDigest() string {
	block.readOp = opRawBytes
	io.Copy(ioutil.Discard, block.payload)
	h := block.payloadDigest.Sum(nil)
	return fmt.Sprintf("request payload digest %x", h)
	//return "request payload digest"
}

func (block *httpRequestBlock) HttpHeaderBytes() []byte {
	return block.httpHeaderBytes
}

func (block *httpRequestBlock) HttpRequestLine() string {
	block.parseHeaders()
	return block.httpRequestLine
}

func (block *httpRequestBlock) HttpHeader() *http.Header {
	block.parseHeaders()
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
	_, err = w.Write([]byte(CRLF))
	bytesWritten += 2
	return bytesWritten, err
}

type httpResponseBlock struct {
	httpStatusLine  string
	httpStatusCode  int
	httpHeader      *http.Header
	httpHeaderBytes []byte
	payload         io.Reader
	blockDigest     hash.Hash
	payloadDigest   hash.Hash
	readOp          readOp
	parseHeaderOnce sync.Once
}

func (block *httpResponseBlock) RawBytes() (io.Reader, error) {
	if block.readOp != opInitial {
		return nil, errContentReAccessed
	}
	block.readOp = opRawBytes
	return io.MultiReader(bytes.NewReader(block.httpHeaderBytes), block.payload), nil
}

func (block *httpResponseBlock) BlockDigest() string {
	block.readOp = opRawBytes
	io.Copy(ioutil.Discard, block.payload)
	h := block.blockDigest.Sum(nil)
	return fmt.Sprintf("response digest sha1:%x", h)
}

func (block *httpResponseBlock) PayloadBytes() (io.Reader, error) {
	if block.readOp != opInitial {
		return nil, errContentReAccessed
	}
	block.readOp = opPayloadBytes
	return block.payload, nil
}

func (block *httpResponseBlock) PayloadDigest() string {
	block.readOp = opRawBytes
	io.Copy(ioutil.Discard, block.payload)
	h := block.payloadDigest.Sum(nil)
	return fmt.Sprintf("response payload digest %x", h)
}

func (block *httpResponseBlock) HttpHeaderBytes() []byte {
	return block.httpHeaderBytes
}

func (block *httpResponseBlock) HttpStatusLine() string {
	block.parseHeaders()
	return block.httpStatusLine
}

func (block *httpResponseBlock) HttpStatusCode() int {
	block.parseHeaders()
	return block.httpStatusCode
}

func (block *httpResponseBlock) HttpHeader() *http.Header {
	block.parseHeaders()
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
	_, err = w.Write([]byte(CRLF))
	bytesWritten += 2
	return bytesWritten, err
}

// headerBytes reads the http-headers into a byte array.
func headerBytes(r *bufio.Reader) []byte {
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

func NewHttpBlock(r io.Reader) (PayloadBlock, error) {
	//r := block.rawBytes
	//if err != nil {
	//	return nil, err
	//}
	rb := bufio.NewReader(r)
	b, err := rb.Peek(4)
	if err != nil {
		return nil, fmt.Errorf("not a http block: %w", err)
	}

	hb := headerBytes(rb)
	blockDigest := sha1.New()
	blockDigest.Write(hb)
	payloadDigest := sha1.New()
	payload := io.TeeReader(io.TeeReader(rb, blockDigest), payloadDigest)
	if bytes.HasPrefix(b, []byte("HTTP")) {
		resp := &httpResponseBlock{
			httpHeaderBytes: hb,
			payload:         payload,
			blockDigest:     blockDigest,
			payloadDigest:   payloadDigest,
		}
		return resp, nil
	} else {
		resp := &httpRequestBlock{
			httpHeaderBytes: hb,
			payload:         payload,
			blockDigest:     blockDigest,
			payloadDigest:   payloadDigest,
		}
		return resp, nil
	}
}
