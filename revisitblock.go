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
	"bytes"
	"fmt"
	"io"
)

type revisitBlock struct {
	opts                *warcRecordOptions
	headerBytes         []byte
	blockDigest         *digest
	blockDigestString   string
	payloadDigestString string
}

func (block *revisitBlock) IsCached() bool {
	return true
}

func (block *revisitBlock) Cache() error {
	return nil
}

func (block *revisitBlock) Close() error {
	return nil
}

func (block *revisitBlock) RawBytes() (io.Reader, error) {
	return bytes.NewReader(block.headerBytes), nil
}

// ProtocolHeaderBytes implements ProtocolHeaderBlock
func (block *revisitBlock) ProtocolHeaderBytes() []byte {
	return block.headerBytes
}

func (block *revisitBlock) PayloadBytes() (io.Reader, error) {
	return &bytes.Buffer{}, nil
}

func (block *revisitBlock) BlockDigest() string {
	return block.blockDigestString
}

func (block *revisitBlock) PayloadDigest() string {
	return block.payloadDigestString
}

func (block *revisitBlock) Size() int64 {
	return int64(len(block.headerBytes))
}

func (block *revisitBlock) Write(w io.Writer) (int64, error) {
	p, err := block.RawBytes()
	if err != nil {
		return 0, err
	}
	bytesWritten, err := io.Copy(w, p)
	if err != nil {
		return bytesWritten, err
	}
	return bytesWritten, err
}

// newRevisitBlock creates a revisitBlock from a PayloadBlock
func newRevisitBlock(opts *warcRecordOptions, src Block) (*revisitBlock, error) {
	block := &revisitBlock{
		opts: opts,
	}

	switch v := src.(type) {
	case HttpRequestBlock:
		block.headerBytes = v.ProtocolHeaderBytes()
		block.payloadDigestString = v.PayloadDigest()
	case HttpResponseBlock:
		block.headerBytes = v.ProtocolHeaderBytes()
		block.payloadDigestString = v.PayloadDigest()
	case *genericBlock:
	default:
		return nil, fmt.Errorf("making revisit of %T not supported", v)
	}

	blockDigest, _ := newDigest(block.opts.defaultDigestAlgorithm, block.opts.defaultDigestEncoding)
	if _, err := blockDigest.Write(block.headerBytes); err != nil {
		return nil, err
	}
	block.blockDigestString = blockDigest.format()
	block.blockDigest = blockDigest

	return block, nil
}

// parseRevisitBlock creates a new revisitBlock from a reader
func parseRevisitBlock(opts *warcRecordOptions, r io.Reader, blockDigest *digest, payloadDigest string) (*revisitBlock, error) {
	block := &revisitBlock{
		opts:                opts,
		payloadDigestString: payloadDigest,
	}

	content := &bytes.Buffer{}
	rr := io.TeeReader(r, blockDigest)
	if _, err := io.Copy(content, rr); err != nil {
		return nil, err
	}
	block.headerBytes = content.Bytes()

	block.blockDigestString = blockDigest.format()
	block.blockDigest = blockDigest

	return block, nil
}
