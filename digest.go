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
	"encoding/base32"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"strings"
)

type digestEncoding uint8

func (d digestEncoding) encode(digest *digest) string {
	dig := digest.Sum(nil)
	switch d {
	case Base16:
		return strings.ToUpper(hex.EncodeToString(dig))
	case Base32:
		return base32.StdEncoding.EncodeToString(dig)
	case Base64:
		return base64.StdEncoding.EncodeToString(dig)
	default:
		return string(dig)
	}
}

const (
	unknown digestEncoding = 0
	Base16  digestEncoding = 1
	Base32  digestEncoding = 2
	Base64  digestEncoding = 3
)

func detectEncoding(algorithm, digest string, defaultEncoding digestEncoding) digestEncoding {
	var algorithmLenght int
	switch algorithm {
	case "md5":
		if len(digest) == 32 {
			// Special handling for md5 where encoded length are the same for base16 and base32.
			// Distinction can be done on base32 padding
			if strings.HasSuffix(digest, "=") {
				return Base32
			} else {
				return Base16
			}
		}
		algorithmLenght = md5.Size
	case "sha1":
		algorithmLenght = sha1.Size
	case "sha256":
		algorithmLenght = sha256.Size
	case "sha512":
		algorithmLenght = sha512.Size
	}
	switch len(digest) {
	case algorithmLenght * 2:
		return Base16
	case base32.StdEncoding.EncodedLen(algorithmLenght):
		return Base32
	case base64.StdEncoding.EncodedLen(algorithmLenght):
		return Base64
	}
	return defaultEncoding
}

type digest struct {
	hash.Hash
	name     string
	hash     string
	count    int64
	encoding digestEncoding
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
	return fmt.Sprintf("%s:%s", d.name, d.encoding.encode(d))
	//return fmt.Sprintf("%s:%X", d.name, d.Sum(nil))
}

func (d *digest) validate() error {
	computed := d.encoding.encode(d)
	if d.hash != computed {
		return fmt.Errorf("wrong digest: expected %s:%s, computed: %s:%s", d.name, d.hash, d.name, computed)
	}
	return nil
}

func newDigest(digestString string, defaultEncoding digestEncoding) (*digest, error) {
	t := strings.SplitN(digestString, ":", 2)
	algorithm := t[0]
	algorithm = strings.ToLower(algorithm)
	if algorithm == "" {
		algorithm = "sha1"
	}
	var hash string
	if len(t) > 1 {
		hash = t[1]
	}
	encoding := detectEncoding(algorithm, hash, defaultEncoding)
	switch algorithm {
	case "md5":
		return &digest{md5.New(), algorithm, hash, 0, encoding}, nil
	case "sha1":
		return &digest{sha1.New(), algorithm, hash, 0, encoding}, nil
	case "sha256":
		return &digest{sha256.New(), algorithm, hash, 0, encoding}, nil
	case "sha512":
		return &digest{sha512.New(), algorithm, hash, 0, encoding}, nil
	case "":
		return &digest{sha1.New(), "sha1", hash, 0, encoding}, nil
	default:
		return nil, fmt.Errorf("unsupported digest algorithm '%s'", algorithm)
	}
}

func newDigestFromField(wr *warcRecord, warcDigestField string) (d *digest, err error) {
	if wr.WarcHeader().Has(warcDigestField) {
		d, err = newDigest(wr.WarcHeader().Get(warcDigestField), wr.opts.defaultDigestEncoding)
	} else {
		d, err = newDigest(wr.opts.defaultDigestAlgorithm, wr.opts.defaultDigestEncoding)
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
