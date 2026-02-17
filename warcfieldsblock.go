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
	digestOnce  sync.Once
	warcFields  *WarcFields
	blockDigest *digest
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

func (block *warcFieldsBlock) Write(w io.Writer) (n int64, err error) {
	m, err := block.warcFields.Write(w)
	n += m
	if err != nil {
		return
	}
	k, err := w.Write(crlf)
	n += int64(k)
	if err != nil {
		return
	}
	return
}

func newWarcFieldsBlock(options *warcRecordOptions, _ *WarcFields, rb io.Reader, d *digest) (WarcFieldsBlock, []error, error) {
	var validation []error
	wfb := &warcFieldsBlock{blockDigest: d}
	var err error
	wfb.content, err = io.ReadAll(rb)
	if err != nil && options.errSyntax > ErrIgnore {
		switch options.errSyntax {
		case ErrWarn:
			validation = append(validation, err)
		case ErrFail:
			return wfb, validation, err
		}
	}
	p := &warcfieldsParser{options}
	var blockValidation []error
	wfb.warcFields, blockValidation, err = p.Parse(bufio.NewReader(bytes.NewReader(wfb.content)), &position{})
	if options.errBlock > ErrIgnore && len(blockValidation) > 0 {
		switch options.errBlock {
		case ErrWarn:
			for _, e := range blockValidation {
				validation = append(validation, newWrappedSyntaxError("error in warc fields block", nil, e))
			}
		case ErrFail:
			if len(blockValidation) > 0 {
				err = newWrappedSyntaxError("error in warc fields block", nil, blockValidation[0])
				return wfb, validation, err
			}
		}
	}

	if options.fixWarcFieldsBlockErrors && len(blockValidation) > 0 {
		// Write corrected warc fields block to content buffer
		b := bytes.Buffer{}
		_, err = wfb.WarcFields().Write(&b)
		if err == nil {
			wfb.content = b.Bytes()
		}
	}

	return wfb, validation, err
}
