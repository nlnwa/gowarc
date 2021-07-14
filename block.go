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
	"errors"
	"github.com/nlnwa/gowarc/internal/diskbuffer"
	"io"
	"io/ioutil"
	"sync"
)

// Block is the interface used to represent the content of a WARC record as specified by the WARC specification:
// https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#warc-record-content-block
//
// A Block might be cached or non-cached. Calling RawBytes or BlockDigest more than once will fail if the block is not
// cached.
//
// NOTE: Blocks are not required to be thread safe.
type Block interface {
	// RawBytes returns the bytes of the Block
	RawBytes() (io.Reader, error)
	BlockDigest() string
	IsCached() bool
	Cache() error
}

// PayloadBlock is a Block with a well defined payload.
//
// Ref: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#warc-record-payload
type PayloadBlock interface {
	Block
	PayloadBytes() (io.Reader, error)
	PayloadDigest() string
}

type genericBlock struct {
	rawBytes    io.Reader
	blockDigest *digest
	digestOnce  sync.Once
	readOp      readOp
	cached      bool
}

func newGenericBlock(r io.Reader, d *digest) *genericBlock {
	b := &genericBlock{rawBytes: r, blockDigest: d}
	if _, ok := r.(io.Seeker); ok {
		b.cached = true
	}
	return b
}

func (block *genericBlock) IsCached() bool {
	return block.cached
}

func (block *genericBlock) Cache() error {
	if block.cached {
		return nil
	}
	if block.readOp != opInitial {
		return errContentReAccessed
	}
	buf := diskbuffer.New()
	if _, err := buf.ReadFrom(block.rawBytes); err != nil {
		return err
	}
	if c, ok := block.rawBytes.(io.Closer); ok {
		_ = c.Close()
	}
	block.rawBytes = buf
	block.cached = true
	return nil
}

func (block *genericBlock) RawBytes() (io.Reader, error) {
	if block.cached {
		if _, err := block.rawBytes.(io.Seeker).Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		return block.rawBytes, nil
	}

	// Block is not cached. Guard against calling more than once
	if block.readOp != opInitial {
		return nil, errContentReAccessed
	}
	block.readOp = opRawBytes

	block.rawBytes = io.TeeReader(block.rawBytes, block.blockDigest)
	return block.rawBytes, nil
}

func (block *genericBlock) BlockDigest() string {
	block.digestOnce.Do(func() {
		if block.cached {
			if _, err := block.rawBytes.(io.Seeker).Seek(0, io.SeekStart); err != nil {
				panic(err)
			}
			block.blockDigest.Reset()
			_, _ = io.Copy(block.blockDigest, block.rawBytes)
			return
		}

		if block.readOp == opInitial {
			_, _ = block.RawBytes()
		}
		block.readOp = opRawBytes
		_, _ = io.Copy(ioutil.Discard, block.rawBytes)
	})
	return block.blockDigest.format()
}

// The readOp constants describe access to RawBytes() or PayloadBytes() on a PayloadBlock(),
// so that RawBytes and PayloadBytes() can check for invalid usage.
type readOp int8

const (
	opInitial      readOp = 0 // Initial value.
	opRawBytes     readOp = 1
	opPayloadBytes readOp = 2
)

var errContentReAccessed = errors.New("gowarc.Block: tried to access content twice")
