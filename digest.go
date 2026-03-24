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

// DigestEncoding represents the encoding used for WARC digest values.
type DigestEncoding uint8

var (
	base32NoPaddingEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)
	base64NoPaddingEncoding = base64.StdEncoding.WithPadding(base64.NoPadding)
)

func (d DigestEncoding) encode(digest *digest) string {
	dig := digest.Sum(nil)
	switch d {
	case Base16:
		return strings.ToLower(hex.EncodeToString(dig))
	case Base32:
		return base32NoPaddingEncoding.EncodeToString(dig)
	case Base64:
		return base64NoPaddingEncoding.EncodeToString(dig)
	default:
		return string(dig)
	}
}

func (d DigestEncoding) decode(s string) ([]byte, error) {
	switch d {
	case Base16:
		return hex.DecodeString(s)
	case Base32:
		if strings.HasSuffix(s, "=") {
			return base32.StdEncoding.DecodeString(s)
		}
		return base32NoPaddingEncoding.DecodeString(s)
	case Base64:
		if strings.HasSuffix(s, "=") {
			return base64.StdEncoding.DecodeString(s)
		}
		return base64NoPaddingEncoding.DecodeString(s)
	default:
		return []byte(s), nil
	}
}

const (
	unknown DigestEncoding = 0
	Base16  DigestEncoding = 1
	Base32  DigestEncoding = 2
	Base64  DigestEncoding = 3
)

// recommendedEncoding returns the WARC spec community-recommended encoding for the
// given algorithm. SHA-1 uses Base32 (no padding needed). All others use Base16 to
// avoid the need for padding characters which are forbidden in digest-value tokens.
func recommendedEncoding(algorithm string) DigestEncoding {
	switch algorithm {
	case "sha1":
		return Base32
	default:
		return Base16
	}
}

func detectEncoding(algorithm, digest string, defaultEncoding DigestEncoding) DigestEncoding {
	var algorithmLength int
	switch algorithm {
	case "md5":
		if len(digest) == 32 {
			// Special handling for md5 where padded base32 encoded length (32) is the same as base16.
			// Distinction can be done on base32 padding suffix.
			if strings.HasSuffix(digest, "=") {
				return Base32
			} else {
				return Base16
			}
		}
		algorithmLength = md5.Size
	case "sha1":
		algorithmLength = sha1.Size
	case "sha256":
		algorithmLength = sha256.Size
	case "sha512":
		algorithmLength = sha512.Size
	default:
		return defaultEncoding
	}
	switch l := len(digest); {
	case l == algorithmLength*2:
		return Base16
	case l == base32.StdEncoding.EncodedLen(algorithmLength) || l == base32NoPaddingEncoding.EncodedLen(algorithmLength):
		return Base32
	case l == base64.StdEncoding.EncodedLen(algorithmLength) || l == base64NoPaddingEncoding.EncodedLen(algorithmLength):
		return Base64
	}
	return defaultEncoding
}

// normalizeAlgorithmName normalizes the algorithm name to the format used in WARC digest-fields.
func normalizeAlgorithmName(algorithm string) string {
	algorithm = strings.ToLower(algorithm)

	switch algorithm {
	case "sha-1":
		return "sha1"
	case "sha-256":
		return "sha256"
	case "sha-512":
		return "sha512"
	default:
		return algorithm
	}
}

// digest is a utility for parsing, creation and validation of WARC block and payload digests.
//
// Typical usage is to create a digest from a WARC record's WARC-Block-Digest or WARC-Payload-Digest fields.
// Then write the content to the digest which implements io.Writer. When all is written, call validate to check if the
// submitted digest value equals the computed value. For this usage create the digest with newDigestFromField.
//
// For new records a digest can be calculated by creating a new digest with newDigest with the preferred algorithm as
// parameter. Then write the content to digest and call format to get a string suitable for WARC digest-fields.
type digest struct {
	hash.Hash
	name     string
	hash     string
	count    int64
	encoding DigestEncoding
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

// format creates a string in the format expected in WARC-Block-Digest and WARC-Payload-Digest fields.
func (d *digest) format() string {
	return fmt.Sprintf("%s:%s", d.name, d.encoding.encode(d))
}

// validate compares the computed digest-value against the digest-string submitted as part of the instantiation of the
// digest.
func (d *digest) validate() error {
	computed := d.encoding.encode(d)
	dig, err := d.encoding.decode(d.hash)
	if err != nil {
		return err
	}
	if !bytes.Equal(dig, d.Sum(nil)) {
		return &DigestError{
			Algorithm: d.name,
			Expected:  d.hash,
			Computed:  computed,
		}
	}
	return nil
}

// updateDigest updates the digest-string to the computed value.
func (d *digest) updateDigest() {
	d.hash = d.encoding.encode(d)
}

// newDigest creates a new digest from the value of a WARC digest-field or from scratch.
//
// digestString has the format: <algorithm>[:[<digestValue>]] where algorithm is one of md5, sha1, sha256, or sha512.
//
// The encoding is deduced from the length of the digestValue. In the case where only the algorithm is submitted
// or the length of the digestValue is of wrong length for the supported encodings, the value of defaultEncoding is used.
func newDigest(digestString string, defaultEncoding DigestEncoding) (*digest, error) {
	algorithm, hash, _ := strings.Cut(digestString, ":")
	algorithm = normalizeAlgorithmName(algorithm)
	if defaultEncoding == unknown {
		defaultEncoding = recommendedEncoding(algorithm)
	}
	encoding := detectEncoding(algorithm, hash, defaultEncoding)
	switch encoding {
	case Base16:
		hash = strings.ToLower(hash)
	case Base32:
		hash = strings.ToUpper(hash)
	}

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
		return &digest{sha256.New(), "sha256", hash, 0, encoding}, nil
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedDigestAlgorithm, algorithm)
	}
}

// newDigestFromField takes a warcRecord and a digest-field name and creates a new digest from it.
//
// If the digest-field is missing from the warcRecord a digest is created with the default algorithm and encoding set
// in the warcRecord's options. If no encoding is configured (unknown), the spec-recommended encoding for the
// algorithm is used.
func newDigestFromField(wr *warcRecord, warcDigestField string) (d *digest, err error) {
	var digestString string
	if wr.WarcHeader().Has(warcDigestField) {
		digestString = wr.WarcHeader().Get(warcDigestField)
	} else {
		digestString = wr.opts.defaultDigestAlgorithm
	}
	d, err = newDigest(digestString, wr.opts.defaultDigestEncoding)
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
