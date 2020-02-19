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
)

const CRLF = "\r\n"

type RevisitBlock struct {
	rawBytes   *bufio.Reader
	headers    *bytes.Buffer
	dataRecord WarcRecord
}

func (block *RevisitBlock) RawBytes() (*bufio.Reader, error) {
	if block.rawBytes != nil {
		return block.rawBytes, nil
	}

	if block.dataRecord == nil {
		return nil, fmt.Errorf("revisit record is not merged with referenced data")
	}

	dataBlock := block.dataRecord.Block().(PayloadBlock)
	var data io.Reader
	var err error
	data, err = dataBlock.PayloadBytes()
	if err != nil {
		return nil, err
	}

	block.rawBytes = bufio.NewReader(io.MultiReader(block.headers, data))
	return block.rawBytes, nil
}

func (block *RevisitBlock) PayloadBytes() (io.ReadCloser, error) {
	r, err := block.Response()
	if err != nil {
		return nil, err
	}
	return r.Body, nil
}

func (block *RevisitBlock) Response() (*http.Response, error) {
	rb, err := block.RawBytes()
	if err != nil {
		return nil, err
	}
	return http.ReadResponse(rb, nil)
}

func (block *RevisitBlock) Merge(refersTo WarcRecord) {
	block.dataRecord = refersTo
}

func NewRevisitBlock(block Block) (Block, error) {
	rb, err := block.RawBytes()
	if err != nil {
		return nil, err
	}
	buf := &bytes.Buffer{}
	rb.WriteTo(buf)
	buf.WriteString(CRLF)
	return &RevisitBlock{headers: buf}, nil
}
