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
	"io"
	"io/ioutil"
)

type WarcFieldsBlock interface {
	Block
	WarcFields() *warcFields
}

type warcFieldsBlock struct {
	content    []byte
	warcFields *warcFields
}

func (b *warcFieldsBlock) WarcFields() *warcFields {
	return b.warcFields
}

func NewWarcFieldsBlock(rb io.Reader, options *options) (WarcFieldsBlock, error) {
	wfb := &warcFieldsBlock{}
	//rb, err := block.RawBytes()
	//if err != nil {
	//	return nil, err
	//}
	var err error
	wfb.content, err = ioutil.ReadAll(rb)
	p := &warcfieldsParser{options}
	wfb.warcFields, err = p.Parse(bufio.NewReader(rb), nil)
	if err != nil {
		return nil, err
	}

	return wfb, nil
}

func (b *warcFieldsBlock) RawBytes() (io.Reader, error) {
	return bytes.NewReader(b.content), nil
}

func (block *warcFieldsBlock) BlockDigest() string {
	return "warcfields digest"
}

func (b *warcFieldsBlock) Write(w io.Writer) (bytesWritten int64, err error) {
	bytesWritten, err = b.warcFields.Write(w)
	w.Write([]byte(CRLF))
	bytesWritten += 2
	return
}
