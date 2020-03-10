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

const CRLF = "\r\n"

type RevisitBlock struct {
	Block
	response         *http.Response
	responseRawBytes []byte
	once             sync.Once
}

func (block *RevisitBlock) RawBytes() (*bufio.Reader, error) {
	if block.responseRawBytes == nil {
		return block.Block.RawBytes()
	}

	r1, err := block.ResponseBytes()
	if err != nil {
		return nil, err
	}
	return bufio.NewReader(r1), nil
}

func (block *RevisitBlock) PayloadBytes() (io.Reader, error) {
	_, err := block.Response()
	if err != nil {
		return nil, err
	}
	return block.Block.RawBytes()
}

func (block *RevisitBlock) ResponseBytes() (io.Reader, error) {
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

func (block *RevisitBlock) Response() (*http.Response, error) {
	var err error
	block.once.Do(func() {
		rb, e := block.ResponseBytes()
		if e != nil {
			err = e
		}
		block.response, err = http.ReadResponse(bufio.NewReader(rb), nil)
	})
	return block.response, err
}

func (block *RevisitBlock) Write(w io.Writer) (bytesWritten int64, err error) {
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

func NewRevisitBlock(block Block) (Block, error) {
	return &RevisitBlock{Block: block}, nil
}

func Merge(revisit, refersTo WarcRecord) (WarcRecord, error) {
	fmt.Printf("MERGE %v -> %v\n", revisit.WarcHeader().Get(WarcRecordID), refersTo)
	return refersTo, nil
}
