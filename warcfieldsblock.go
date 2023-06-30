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
	"io"
	"sync"
)

type WarcFieldsBlock interface {
	Block
	WarcFields() *WarcFields
}

type warcFieldsBlock struct {
	content     []byte
	warcFields  *WarcFields
	blockDigest *digest
	digestOnce  sync.Once
}

func (block *warcFieldsBlock) IsCached() bool {
	return true
}

func (block *warcFieldsBlock) Cache() error {
	return nil
}
func (block *warcFieldsBlock) Close() error {
	return nil
}

func (block *warcFieldsBlock) WarcFields() *WarcFields {
	return block.warcFields
}

func (block *warcFieldsBlock) RawBytes() (io.Reader, error) {
	return bytes.NewReader(block.content), nil
}

func (block *warcFieldsBlock) BlockDigest() string {
	block.digestOnce.Do(func() {
		if _, err := block.blockDigest.Write(block.content); err != nil {
			panic(err)
		}
	})
	return block.blockDigest.format()
}

func (block *warcFieldsBlock) Size() int64 {
	return int64(len(block.content))
}

func (block *warcFieldsBlock) Write(w io.Writer) (bytesWritten int64, err error) {
	bytesWritten, err = block.warcFields.Write(w)
	if err != nil {
		return
	}
	n, err := w.Write([]byte(crlf))
	if err != nil {
		return
	}
	bytesWritten += int64(n)
	return
}

func newWarcFieldsBlock(options *warcRecordOptions, wf *WarcFields, rb io.Reader, d *digest, validation *Validation) (WarcFieldsBlock, error) {
	wfb := &warcFieldsBlock{blockDigest: d}
	var err error
	wfb.content, err = io.ReadAll(rb)
	if err != nil && options.errSyntax > ErrIgnore {
		switch options.errSyntax {
		case ErrWarn:
			validation.addError(err)
		case ErrFail:
			return wfb, err
		}
	}
	p := &warcfieldsParser{options}
	blockValidation := Validation{}
	wfb.warcFields, err = p.Parse(bufio.NewReader(bytes.NewReader(wfb.content)), &blockValidation, &position{})
	for _, e := range blockValidation {
		validation.addError(newWrappedSyntaxError("error in warc fields block", nil, e))
	}

	if !blockValidation.Valid() && options.fixWarcFieldsBlockErrors {
		// Write corrected warc fields block to content buffer
		b := bytes.Buffer{}
		_, err = wfb.WarcFields().Write(&b)
		if err == nil {
			wfb.content = b.Bytes()
		}
	}

	return wfb, err
}
