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

func (block *httpRequestBlock) PayloadBytes() (io.ReadCloser, error) {
	r, err := block.Request()
	if err != nil {
		return nil, err
	}
	return r.Body, nil
}

func (block *httpRequestBlock) Request() (*http.Request, error) {
	var err error
	block.once.Do(func() {
		rb, e := block.RawBytes()
		if e != nil {
			err = e
		}
		block.request, err = http.ReadRequest(rb)
	})
	return block.request, err
}

func (block *httpRequestBlock) Write(w io.Writer) (bytesWritten int64, err error) {
	var p io.ReadCloser
	p, err = block.PayloadBytes()
	if err != nil {
		return
	}
	fmt.Printf("########## %v\n", err)
	bytesWritten, err = io.Copy(w, p)
	if err != nil {
		return
	}
	fmt.Printf("########## %v, %v\n", err, bytesWritten)
	//w.Write([]byte(CRLF))
	//bytesWritten += 2
	return
}

type httpResponseBlock struct {
	Block
}

func (block *httpResponseBlock) PayloadBytes() (io.ReadCloser, error) {
	r, err := block.Response()
	if err != nil {
		return nil, err
	}
	return r.Body, nil
}

func (block *httpResponseBlock) Response() (*http.Response, error) {
	rb, err := block.RawBytes()
	if err != nil {
		return nil, err
	}
	return http.ReadResponse(rb, nil)
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
		return &httpResponseBlock{block}, nil
	} else {
		return &httpRequestBlock{Block: block}, nil
	}
}
