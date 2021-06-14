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
	assert "github.com/stretchr/testify/assert"
	"testing"
)

func Test_newDigest(t *testing.T) {
	tests := []struct {
		name           string
		algorithm      string
		digestString   string
		wantDigestName string
		wantDigest     string
		wantErr        bool
	}{
		{"md5", "md5", "Some content", "md5", "md5:B53227DA4280F0E18270F21DD77C91D0", false},
		{"md5 with digest", "md5:12345", "Some content", "md5", "md5:B53227DA4280F0E18270F21DD77C91D0", false},
		{"sha1", "sha1", "Some content", "sha1", "sha1:9F1A6ECF74E9F9B1AE52E8EB581D420E63E8453A", false},
		{"sha1 with digest", "sha1:12345", "Some content", "sha1", "sha1:9F1A6ECF74E9F9B1AE52E8EB581D420E63E8453A", false},
		{"sha256", "sha256", "Some content", "sha256", "sha256:9C6609FC5111405EA3F5BB3D1F6B5A5EFD19A0CEC53D85893FD96D265439CD5B", false},
		{"sha256 with digest", "sha256:12345", "Some content", "sha256", "sha256:9C6609FC5111405EA3F5BB3D1F6B5A5EFD19A0CEC53D85893FD96D265439CD5B", false},
		{"sha512", "sha512", "Some content", "sha512", "sha512:B20D977718ED67F2BF7620EE2D982FD850C4883EC8D048440FE7B6A86CF6322FD791C47B0C7469DBEEF3E339032E1ABC4BCEBE5EFC104BC19A117BFEF4478605", false},
		{"sha512 with digest", "sha512:12345", "Some content", "sha512", "sha512:B20D977718ED67F2BF7620EE2D982FD850C4883EC8D048440FE7B6A86CF6322FD791C47B0C7469DBEEF3E339032E1ABC4BCEBE5EFC104BC19A117BFEF4478605", false},
		{"unknown algorithm", "mysecret:12345", "Some content", "mysecret", "mysecret:123", true},
		{"unknown algorithm with digest", "mysecret:12345", "Some content", "mysecret", "mysecret:123", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := newDigest(tt.algorithm)

			assert := assert.New(t)
			if tt.wantErr {
				assert.Error(err)
				return
			} else {
				assert.NoError(err)
				if err != nil {
					return
				}
			}
			d.Write([]byte(tt.digestString))

			assert.Equal(tt.wantDigestName, d.name)
			assert.Equal(tt.wantDigest, d.format())
		})
	}
}

func Test_digest_validate(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		digestString string
		wantValid    bool
	}{
		{"md5", "Some content", "md5", false},
		{"md5 with digest", "Some content", "md5:B53227DA4280F0E18270F21DD77C91D0", true},
		{"md5 with wrong digest", "Some content", "md5:123", false},
		{"sha1", "Some content", "sha1", false},
		{"sha1 with digest", "Some content", "sha1:9F1A6ECF74E9F9B1AE52E8EB581D420E63E8453A", true},
		{"sha1 with wrong digest", "Some content", "sha1:123", false},
		{"sha256", "Some content", "sha256", false},
		{"sha256 with digest", "Some content", "sha256:9C6609FC5111405EA3F5BB3D1F6B5A5EFD19A0CEC53D85893FD96D265439CD5B", true},
		{"sha256 with wrong digest", "Some content", "sha256:123", false},
		{"sha512", "Some content", "sha512", false},
		{"sha512 with digest", "Some content", "sha512:B20D977718ED67F2BF7620EE2D982FD850C4883EC8D048440FE7B6A86CF6322FD791C47B0C7469DBEEF3E339032E1ABC4BCEBE5EFC104BC19A117BFEF4478605", true},
		{"sha512 with wrong digest", "Some content", "sha512:123", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, _ := newDigest(tt.digestString)

			assert := assert.New(t)
			d.Write([]byte(tt.input))

			err := d.validate()
			if !tt.wantValid {
				assert.Error(err)
			} else {
				assert.NoError(err)
			}
		})
	}
}
