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

package gowarc

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
)

type httpRequestBlock struct {
	Block
}

func (reqb *httpRequestBlock) PayloadBytes() (io.ReadCloser, error) {
	r, err := reqb.Request()
	if err != nil {
		return nil, err
	}
	return r.Body, nil
}

func (reqb *httpRequestBlock) Request() (*http.Request, error) {
	return http.ReadRequest(reqb.RawBytes())
}

type httpResponseBlock struct {
	Block
}

func (reqb *httpResponseBlock) PayloadBytes() (io.ReadCloser, error) {
	r, err := reqb.Response()
	if err != nil {
		return nil, err
	}
	return r.Body, nil
}

func (reqb *httpResponseBlock) Response() (*http.Response, error) {
	return http.ReadResponse(reqb.RawBytes(), nil)
}

func NewHttpBlock(block Block) (PayloadBlock, error) {
	b, err := block.RawBytes().Peek(4)
	if err != nil {
		if err == io.EOF {
			fmt.Printf("!!!!! EOF\n")
			// TODO: Handle revisit block
		}
		fmt.Printf("**********PEEK ERROR******* %v %T\n", err, err)
		return nil, err
	}
	if bytes.HasPrefix(b, []byte("HTTP")) {
		return &httpResponseBlock{block}, nil
	} else {
		return &httpRequestBlock{block}, nil
	}
}
