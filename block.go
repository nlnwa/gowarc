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
	"crypto/sha1"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
)

type Block interface {
	RawBytes() (io.Reader, error)
	BlockDigest() string
}

type PayloadBlock interface {
	Block
	PayloadBytes() (io.Reader, error)
	PayloadDigest() string
}

type genericBlock struct {
	rawBytes    io.Reader
	blockDigest hash.Hash
	readOp      readOp
}

func (block *genericBlock) RawBytes() (io.Reader, error) {
	if block.readOp != opInitial {
		return nil, errContentReAccessed
	}
	block.readOp = opRawBytes

	block.blockDigest = sha1.New()
	block.rawBytes = io.TeeReader(block.rawBytes, block.blockDigest)
	return block.rawBytes, nil
}

func (block *genericBlock) BlockDigest() string {
	if block.readOp == opInitial {
		block.RawBytes()
	}
	block.readOp = opRawBytes
	io.Copy(ioutil.Discard, block.rawBytes)
	h := block.blockDigest.Sum(nil)
	return fmt.Sprintf("generic digest %x", h)
}

// The readOp constants describe access to RawBytes() or PayloadBytes() on a PayloadBlock(),
// so that RawBytes and PayloadBytes() can check for invalid usage.
type readOp int8

const (
	opInitial      readOp = 0 // Initial value.
	opRawBytes     readOp = 1 // Read rune of size 1.
	opPayloadBytes readOp = 2 // Read rune of size 2.
)

var errContentReAccessed = errors.New("gowarc.Block: tried to access content twice")
