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

package warcrecord

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"hash"
	"strings"
)

type digest struct {
	hash.Hash
	name string
	hash string
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
		return &digest{md5.New(), algorithm, hash}, nil
	case "sha1":
		return &digest{sha1.New(), algorithm, hash}, nil
	case "sha256":
		return &digest{sha256.New(), algorithm, hash}, nil
	case "sha512":
		return &digest{sha512.New(), algorithm, hash}, nil
	default:
		return nil, fmt.Errorf("unsupported digest algorithm '%s'", algorithm)
	}
}
