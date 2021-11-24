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
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"hash"
	"io"
	"strings"
)

type digest struct {
	hash.Hash
	name  string
	hash  string
	count int64
}

// Write (via the embedded io.Writer interface) adds more data to the running hash.
// It never returns an error.
func (d *digest) Write(p []byte) (n int, err error) {
	d.count += int64(len(p))
	return d.Hash.Write(p)
}

// Sum appends the current hash to b and returns the resulting slice.
// It does not change the underlying hash state.
func (d *digest) Sum(b []byte) []byte {
	d.count += int64(len(b))
	return d.Hash.Sum(b)
}

func (d *digest) format() string {
	return fmt.Sprintf("%s:%X", d.name, d.Sum(nil))
}

func (d *digest) validate() error {
	computed := fmt.Sprintf("%X", d.Sum(nil))
	if d.hash != computed {
		return fmt.Errorf("wrong digest: expected %s:%s, computed: %s:%s", d.name, d.hash, d.name, computed)
	}
	return nil
}

func newDigest(digestString string) (*digest, error) {
	t := strings.SplitN(digestString, ":", 2)
	algorithm := t[0]
	algorithm = strings.ToLower(algorithm)
	var hash string
	if len(t) > 1 {
		hash = strings.ToUpper(t[1])
	}
	switch algorithm {
	case "md5":
		return &digest{md5.New(), algorithm, hash, 0}, nil
	case "sha1":
		return &digest{sha1.New(), algorithm, hash, 0}, nil
	case "sha256":
		return &digest{sha256.New(), algorithm, hash, 0}, nil
	case "sha512":
		return &digest{sha512.New(), algorithm, hash, 0}, nil
	case "":
		return &digest{sha1.New(), "sha1", hash, 0}, nil
	default:
		return nil, fmt.Errorf("unsupported digest algorithm '%s'", algorithm)
	}
}

func newDigestFromField(wr *warcRecord, warcDigestField string) (d *digest, err error) {
	if wr.WarcHeader().Has(warcDigestField) {
		d, err = newDigest(wr.WarcHeader().Get(warcDigestField))
	} else {
		d, err = newDigest(wr.opts.defaultDigestAlgorithm)
	}
	return
}

type digestFilterReader struct {
	src     io.Reader
	digests []*digest
}

func newDigestFilterReader(src io.Reader, digests ...*digest) *digestFilterReader {
	return &digestFilterReader{src: src, digests: digests}
}

func (d digestFilterReader) Read(p []byte) (n int, err error) {
	n, err = d.src.Read(p)
	if n > 0 {
		pp := p[:n]
		for _, dd := range d.digests {
			// OK to ignore error. The digest might be wrong, but client gets wanted data.
			_, _ = dd.Write(pp)
		}
	}
	return
}
