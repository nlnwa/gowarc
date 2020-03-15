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
	"fmt"
	"io"
	"net/http"
	"sync"
)

type HttpRequestBlock interface {
	PayloadBlock
	Request() (*http.Request, error)
}

type HttpResponseBlock interface {
	PayloadBlock
	Response() (*http.Response, error)
}

type httpRequestBlock struct {
	Block
	request         *http.Request
	requestRawBytes []byte
	once            sync.Once
}

func (block *httpRequestBlock) RawBytes() (*bufio.Reader, error) {
	if block.requestRawBytes == nil {
		return block.Block.RawBytes()
	}

	r1, err := block.RequestBytes()
	if err != nil {
		return nil, err
	}
	r2, err := block.Block.RawBytes()
	if err != nil {
		return nil, err
	}
	return bufio.NewReader(io.MultiReader(r1, r2)), nil
}

func (block *httpRequestBlock) PayloadBytes() (io.Reader, error) {
	_, err := block.Request()
	if err != nil {
		return nil, err
	}
	return block.Block.RawBytes()
}

func (block *httpRequestBlock) RequestBytes() (io.Reader, error) {
	var err error
	block.once.Do(func() {
		rb, e := block.RawBytes()
		if e != nil {
			err = e
		}

		var buf bytes.Buffer
		var line []byte
		var n, l int
		for {
			line, err = rb.ReadSlice('\n')
			if err != nil {
				break
			}
			n++
			l += len(line)
			buf.Write(line)
			if len(line) < 3 {
				break
			}
		}
		block.requestRawBytes = buf.Bytes()
	})
	return bytes.NewBuffer(block.requestRawBytes), err
}

func (block *httpRequestBlock) Request() (*http.Request, error) {
	var err error
	block.once.Do(func() {
		rb, e := block.RequestBytes()
		if e != nil {
			err = e
		}
		block.request, err = http.ReadRequest(bufio.NewReader(rb))
	})
	return block.request, err
}

func (block *httpRequestBlock) Write(w io.Writer) (bytesWritten int64, err error) {
	var p *bufio.Reader
	p, err = block.RawBytes()
	if err != nil {
		return
	}
	bytesWritten, err = io.Copy(w, p)
	if err != nil {
		return
	}
	w.Write([]byte(CRLF))
	bytesWritten += 2
	return
}

type httpResponseBlock struct {
	Block
	response         *http.Response
	responseRawBytes []byte
	once             sync.Once
}

func (block *httpResponseBlock) RawBytes() (*bufio.Reader, error) {
	if block.responseRawBytes == nil {
		return block.Block.RawBytes()
	}

	r1, err := block.ResponseBytes()
	if err != nil {
		return nil, err
	}
	r2, err := block.Block.RawBytes()
	if err != nil {
		return nil, err
	}
	return bufio.NewReader(io.MultiReader(r1, r2)), nil
}

func (block *httpResponseBlock) PayloadBytes() (io.Reader, error) {
	_, err := block.Response()
	if err != nil {
		return nil, err
	}
	return block.Block.RawBytes()
}

func (block *httpResponseBlock) ResponseBytes() (io.Reader, error) {
	var err error
	block.once.Do(func() {
		rb, e := block.RawBytes()
		if e != nil {
			err = e
		}

		var buf bytes.Buffer
		var line []byte
		var n, l int
		for {
			line, err = rb.ReadSlice('\n')
			if err != nil {
				break
			}
			n++
			l += len(line)
			buf.Write(line)
			if len(line) < 3 {
				break
			}
		}
		block.responseRawBytes = buf.Bytes()
	})
	return bytes.NewBuffer(block.responseRawBytes), err
}

func (block *httpResponseBlock) Response() (*http.Response, error) {
	rb, err := block.ResponseBytes()
	if err != nil {
		return nil, err
	}
	block.response, err = http.ReadResponse(bufio.NewReader(rb), nil)
	return block.response, err
}

func (block *httpResponseBlock) Write(w io.Writer) (bytesWritten int64, err error) {
	var p *bufio.Reader
	p, err = block.RawBytes()
	if err != nil {
		return
	}
	bytesWritten, err = io.Copy(w, p)
	if err != nil {
		return
	}
	w.Write([]byte(CRLF))
	bytesWritten += 2
	return
}

func NewHttpBlock(block Block) (PayloadBlock, error) {
	rb, err := block.RawBytes()
	if err != nil {
		return nil, err
	}
	b, err := rb.Peek(4)
	if err != nil {
		return nil, fmt.Errorf("not a http block %v", err)
		return nil, err
	}
	if bytes.HasPrefix(b, []byte("HTTP")) {
		return &httpResponseBlock{Block: block}, nil
	} else {
		return &httpRequestBlock{Block: block}, nil
	}
}
