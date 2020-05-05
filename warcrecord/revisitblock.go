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
	log "github.com/sirupsen/logrus"
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
	data             io.Reader
}

func (block *RevisitBlock) RawBytes() (*bufio.Reader, error) {
	if block.responseRawBytes == nil {
		return block.Block.RawBytes()
	}

	r1, err := block.ResponseBytes()
	if err != nil {
		return nil, err
	}

	r2, err := block.PayloadBytes()
	if err != nil {
		return nil, err
	}

	return bufio.NewReader(io.MultiReader(r1, r2)), nil
}

func (block *RevisitBlock) PayloadBytes() (io.Reader, error) {
	if block.data == nil {
		return &bytes.Buffer{}, nil
	}
	return block.data, nil
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
				if err == io.EOF {
					line = append(line, []byte(CRLF)...)
					err = nil
				} else {
					break
				}
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
	rb, err := block.ResponseBytes()
	if err != nil {
		return nil, err
	}
	block.response, err = http.ReadResponse(bufio.NewReader(rb), nil)
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
	m, ok := revisit.(*warcRecord)
	if !ok {
		return nil, fmt.Errorf("unknown record implementation")
	}

	m.recordType = RESPONSE
	err := m.headers.Set(WarcType, "response")
	if err != nil {
		log.Warnf("Merge err1: %v", err)
	}
	m.headers.Delete(WarcRefersTo)
	m.headers.Delete(WarcRefersToTargetURI)
	m.headers.Delete(WarcRefersToDate)
	m.headers.Delete(WarcProfile)

	b := m.block.(*RevisitBlock)
	d := refersTo.Block().(PayloadBlock)
	b.data, err = d.PayloadBytes()
	if err != nil {
		log.Warnf("Merge err2: %v", err)
	}

	log.Debugf("Merged: %v", m)

	return m, nil
}
