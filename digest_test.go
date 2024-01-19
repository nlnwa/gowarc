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
		name            string
		algorithm       string
		digestString    string
		defaultEncoding digestEncoding
		wantDigestName  string
		wantDigest      string
		wantErr         bool
	}{
		{"md5", "md5", "Some content", Base16, "md5", "md5:b53227da4280f0e18270f21dd77c91d0", false},
		{"md5 with base16 digest", "md5:12345", "Some content", Base16, "md5", "md5:b53227da4280f0e18270f21dd77c91d0", false},
		{"md5 with base32 digest", "md5:12345", "Some content", Base32, "md5", "md5:WUZCPWSCQDYODATQ6IO5O7ER2A======", false},
		{"md5 with base64 digest", "md5:12345", "Some content", Base64, "md5", "md5:tTIn2kKA8OGCcPId13yR0A==", false},
		{"sha1", "sha1", "Some content", Base16, "sha1", "sha1:9f1a6ecf74e9f9b1ae52e8eb581d420e63e8453a", false},
		{"sha1 with base16 digest", "sha1:12345", "Some content", Base16, "sha1", "sha1:9f1a6ecf74e9f9b1ae52e8eb581d420e63e8453a", false},
		{"sha-1 with base16 digest", "sha-1:12345", "Some content", Base16, "sha1", "sha1:9f1a6ecf74e9f9b1ae52e8eb581d420e63e8453a", false},
		{"sha1 with base32 digest", "sha1:12345", "Some content", Base32, "sha1", "sha1:T4NG5T3U5H43DLSS5DVVQHKCBZR6QRJ2", false},
		{"sha1 with base64 digest", "sha1:12345", "Some content", Base64, "sha1", "sha1:nxpuz3Tp+bGuUujrWB1CDmPoRTo=", false},
		{"sha256", "sha256", "Some content", Base16, "sha256", "sha256:9c6609fc5111405ea3f5bb3d1f6b5a5efd19a0cec53d85893fd96d265439cd5b", false},
		{"sha-256", "sha256", "Some content", Base16, "sha256", "sha256:9c6609fc5111405ea3f5bb3d1f6b5a5efd19a0cec53d85893fd96d265439cd5b", false},
		{"sha256 with base16 digest", "sha256:12345", "Some content", Base16, "sha256", "sha256:9c6609fc5111405ea3f5bb3d1f6b5a5efd19a0cec53d85893fd96d265439cd5b", false},
		{"sha256 with base32 digest", "sha256:12345", "Some content", Base32, "sha256", "sha256:TRTAT7CRCFAF5I7VXM6R6222L36RTIGOYU6YLCJ73FWSMVBZZVNQ====", false},
		{"sha256 with base64 digest", "sha256:12345", "Some content", Base64, "sha256", "sha256:nGYJ/FERQF6j9bs9H2taXv0ZoM7FPYWJP9ltJlQ5zVs=", false},
		{"sha512", "sha512", "Some content", Base16, "sha512", "sha512:b20d977718ed67f2bf7620ee2d982fd850c4883ec8d048440fe7b6a86cf6322fd791c47b0c7469dbeef3e339032e1abc4bcebe5efc104bc19a117bfef4478605", false},
		{"sha512 with base16 digest", "sha512:12345", "Some content", Base16, "sha512", "sha512:b20d977718ed67f2bf7620ee2d982fd850c4883ec8d048440fe7b6a86cf6322fd791c47b0c7469dbeef3e339032e1abc4bcebe5efc104bc19a117bfef4478605", false},
		{"sha512 with base32 digest", "sha512:12345", "Some content", Base32, "sha512", "sha512:WIGZO5YY5VT7FP3WEDXC3GBP3BIMJCB6ZDIEQRAP463KQ3HWGIX5PEOEPMGHI2O353Z6GOIDFYNLYS6OXZPPYECLYGNBC6766RDYMBI=", false},
		{"sha512 with base64 digest", "sha512:12345", "Some content", Base64, "sha512", "sha512:sg2XdxjtZ/K/diDuLZgv2FDEiD7I0EhED+e2qGz2Mi/XkcR7DHRp2+7z4zkDLhq8S86+XvwQS8GaEXv+9EeGBQ==", false},
		{"sha-512 with base64 digest", "sha512:12345", "Some content", Base64, "sha512", "sha512:sg2XdxjtZ/K/diDuLZgv2FDEiD7I0EhED+e2qGz2Mi/XkcR7DHRp2+7z4zkDLhq8S86+XvwQS8GaEXv+9EeGBQ==", false},
		{"unknown algorithm", "mysecret:12345", "Some content", Base16, "mysecret", "mysecret:123", true},
		{"unknown algorithm with digest", "mysecret:12345", "Some content", Base16, "mysecret", "mysecret:123", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := newDigest(tt.algorithm, tt.defaultEncoding)

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
			_, err = d.Write([]byte(tt.digestString))
			assert.NoError(err)

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
		{"md5 with base16 digest", "Some content", "md5:b53227da4280f0e18270f21dd77c91d0", true},
		{"md5 with base32 digest", "Some content", "md5:WUZCPWSCQDYODATQ6IO5O7ER2A======", true},
		{"md5 with base64 digest", "Some content", "md5:tTIn2kKA8OGCcPId13yR0A==", true},
		{"md5 with wrong digest", "Some content", "md5:123", false},
		{"sha1", "Some content", "sha1", false},
		{"sha1 with base16 digest", "Some content", "sha1:9f1a6ecf74e9f9b1ae52e8eb581d420e63e8453a", true},
		{"SHA-1 with base16 digest", "Some content", "SHA-1:9f1a6ecf74e9f9b1ae52e8eb581d420e63e8453a", true},
		{"sha1 with base32 digest", "Some content", "sha1:T4NG5T3U5H43DLSS5DVVQHKCBZR6QRJ2", true},
		{"sha1 with base64 digest", "Some content", "sha1:nxpuz3Tp+bGuUujrWB1CDmPoRTo=", true},
		{"sha1 with wrong digest", "Some content", "sha1:123", false},
		{"sha256", "Some content", "sha256", false},
		{"sha256 with base16 digest", "Some content", "sha256:9c6609fc5111405ea3f5bb3d1f6b5a5efd19a0cec53d85893fd96d265439cd5b", true},
		{"SHA-256 with base16 digest", "Some content", "SHA-256:9c6609fc5111405ea3f5bb3d1f6b5a5efd19a0cec53d85893fd96d265439cd5b", true},
		{"sha256 with base32 digest", "Some content", "sha256:TRTAT7CRCFAF5I7VXM6R6222L36RTIGOYU6YLCJ73FWSMVBZZVNQ====", true},
		{"sha256 with base64 digest", "Some content", "sha256:nGYJ/FERQF6j9bs9H2taXv0ZoM7FPYWJP9ltJlQ5zVs=", true},
		{"sha256 with wrong digest", "Some content", "sha256:123", false},
		{"sha512", "Some content", "sha512", false},
		{"sha512 with base16 digest", "Some content", "sha512:b20d977718ed67f2bf7620ee2d982fd850c4883ec8d048440fe7b6a86cf6322fd791c47b0c7469dbeef3e339032e1abc4bcebe5efc104bc19a117bfef4478605", true},
		{"sha512 with base32 digest", "Some content", "sha512:WIGZO5YY5VT7FP3WEDXC3GBP3BIMJCB6ZDIEQRAP463KQ3HWGIX5PEOEPMGHI2O353Z6GOIDFYNLYS6OXZPPYECLYGNBC6766RDYMBI=", true},
		{"sha512 with base64 digest", "Some content", "sha512:sg2XdxjtZ/K/diDuLZgv2FDEiD7I0EhED+e2qGz2Mi/XkcR7DHRp2+7z4zkDLhq8S86+XvwQS8GaEXv+9EeGBQ==", true},
		{"sha512 with wrong digest", "Some content", "sha512:123", false},
		{"uppercase base16 encoding", "Some content", "sha1:9F1A6ECF74E9F9B1AE52E8EB581D420E63E8453A", true},
		{"lovercase base32 encoding", "Some content", "sha1:t4ng5t3u5h43dlss5dvvqhkcbzr6qrj2", true},
		{"lovercase base64 encoding", "Some content", "sha1:nxpuz3tp+bguuujrwb1cdmporto=", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)

			d, err := newDigest(tt.digestString, unknown)
			assert.NoError(err)
			assert.NotNil(d)

			_, err = d.Write([]byte(tt.input))
			assert.NoError(err)

			err = d.validate()
			if !tt.wantValid {
				assert.Error(err)
			} else {
				assert.NoError(err)
				//assert.Equal(tt.digestString, d.format())
			}
		})
	}
}
