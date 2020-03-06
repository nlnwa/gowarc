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
	"github.com/nlnwa/gowarc/warcfields"
	"github.com/nlnwa/gowarc/warcoptions"
	"io"
)

type WarcFieldsBlock interface {
	Block
	WarcFields() warcfields.WarcFields
}

type warcFieldsBlock struct {
	Block
	warcFields warcfields.WarcFields
}

func (b *warcFieldsBlock) WarcFields() warcfields.WarcFields {
	return b.warcFields
}

func NewWarcFieldsBlock(block Block, options *warcoptions.WarcOptions) (WarcFieldsBlock, error) {
	rb, err := block.RawBytes()
	if err != nil {
		return nil, err
	}

	p := warcfields.NewParser(options)
	wf, err := p.Parse(rb, nil)
	if err != nil {
		return nil, err
	}

	return &warcFieldsBlock{Block: block, warcFields: wf.(warcfields.WarcFields)}, nil
}

func (b *warcFieldsBlock) Write(w io.Writer) (bytesWritten int, err error) {
	bytesWritten, err = b.warcFields.Write(w)
	w.Write([]byte(CRLF))
	bytesWritten += 2
	return
}
