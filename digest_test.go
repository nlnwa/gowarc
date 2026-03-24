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
	"io"
	"strings"
	"testing"

	assert "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_newDigest(t *testing.T) {
	tests := []struct {
		name            string
		algorithm       string
		digestString    string
		defaultEncoding DigestEncoding
		wantDigestName  string
		wantDigest      string
		wantErr         bool
	}{
		{"md5", "md5", "Some content", Base16, "md5", "md5:b53227da4280f0e18270f21dd77c91d0", false},
		{"md5 with base16 digest", "md5:12345", "Some content", Base16, "md5", "md5:b53227da4280f0e18270f21dd77c91d0", false},
		{"md5 with base32 digest", "md5:12345", "Some content", Base32, "md5", "md5:WUZCPWSCQDYODATQ6IO5O7ER2A", false},
		{"md5 with base64 digest", "md5:12345", "Some content", Base64, "md5", "md5:tTIn2kKA8OGCcPId13yR0A", false},
		{"sha1", "sha1", "Some content", Base16, "sha1", "sha1:9f1a6ecf74e9f9b1ae52e8eb581d420e63e8453a", false},
		{"sha1 with base16 digest", "sha1:12345", "Some content", Base16, "sha1", "sha1:9f1a6ecf74e9f9b1ae52e8eb581d420e63e8453a", false},
		{"sha-1 with base16 digest", "sha-1:12345", "Some content", Base16, "sha1", "sha1:9f1a6ecf74e9f9b1ae52e8eb581d420e63e8453a", false},
		{"sha1 with base32 digest", "sha1:12345", "Some content", Base32, "sha1", "sha1:T4NG5T3U5H43DLSS5DVVQHKCBZR6QRJ2", false},
		{"sha1 with base64 digest", "sha1:12345", "Some content", Base64, "sha1", "sha1:nxpuz3Tp+bGuUujrWB1CDmPoRTo", false},
		{"sha256", "sha256", "Some content", Base16, "sha256", "sha256:9c6609fc5111405ea3f5bb3d1f6b5a5efd19a0cec53d85893fd96d265439cd5b", false},
		{"sha-256", "sha256", "Some content", Base16, "sha256", "sha256:9c6609fc5111405ea3f5bb3d1f6b5a5efd19a0cec53d85893fd96d265439cd5b", false},
		{"sha256 with base16 digest", "sha256:12345", "Some content", Base16, "sha256", "sha256:9c6609fc5111405ea3f5bb3d1f6b5a5efd19a0cec53d85893fd96d265439cd5b", false},
		{"sha256 with base32 digest", "sha256:12345", "Some content", Base32, "sha256", "sha256:TRTAT7CRCFAF5I7VXM6R6222L36RTIGOYU6YLCJ73FWSMVBZZVNQ", false},
		{"sha256 with base64 digest", "sha256:12345", "Some content", Base64, "sha256", "sha256:nGYJ/FERQF6j9bs9H2taXv0ZoM7FPYWJP9ltJlQ5zVs", false},
		{"sha512", "sha512", "Some content", Base16, "sha512", "sha512:b20d977718ed67f2bf7620ee2d982fd850c4883ec8d048440fe7b6a86cf6322fd791c47b0c7469dbeef3e339032e1abc4bcebe5efc104bc19a117bfef4478605", false},
		{"sha512 with base16 digest", "sha512:12345", "Some content", Base16, "sha512", "sha512:b20d977718ed67f2bf7620ee2d982fd850c4883ec8d048440fe7b6a86cf6322fd791c47b0c7469dbeef3e339032e1abc4bcebe5efc104bc19a117bfef4478605", false},
		{"sha512 with base32 digest", "sha512:12345", "Some content", Base32, "sha512", "sha512:WIGZO5YY5VT7FP3WEDXC3GBP3BIMJCB6ZDIEQRAP463KQ3HWGIX5PEOEPMGHI2O353Z6GOIDFYNLYS6OXZPPYECLYGNBC6766RDYMBI", false},
		{"sha512 with base64 digest", "sha512:12345", "Some content", Base64, "sha512", "sha512:sg2XdxjtZ/K/diDuLZgv2FDEiD7I0EhED+e2qGz2Mi/XkcR7DHRp2+7z4zkDLhq8S86+XvwQS8GaEXv+9EeGBQ", false},
		{"sha-512 with base64 digest", "sha512:12345", "Some content", Base64, "sha512", "sha512:sg2XdxjtZ/K/diDuLZgv2FDEiD7I0EhED+e2qGz2Mi/XkcR7DHRp2+7z4zkDLhq8S86+XvwQS8GaEXv+9EeGBQ", false},
		{"unknown algorithm", "mysecret:12345", "Some content", Base16, "mysecret", "mysecret:123", true},
		{"unknown algorithm with digest", "mysecret:12345", "Some content", Base16, "mysecret", "mysecret:123", true},
		{"empty algorithm defaults to sha256", "", "Some content", Base16, "sha256", "sha256:9c6609fc5111405ea3f5bb3d1f6b5a5efd19a0cec53d85893fd96d265439cd5b", false},
		{"unsupported algorithm sha3", "sha3", "Some content", Base16, "sha3", "", true},
		{"unsupported algorithm blake2", "blake2", "Some content", Base16, "blake2", "", true},
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
		{"md5 with unpadded base32 digest", "Some content", "md5:WUZCPWSCQDYODATQ6IO5O7ER2A", true},
		{"md5 with base64 digest", "Some content", "md5:tTIn2kKA8OGCcPId13yR0A==", true},
		{"md5 with unpadded base64 digest", "Some content", "md5:tTIn2kKA8OGCcPId13yR0A", true},
		{"md5 with wrong digest", "Some content", "md5:123", false},
		{"sha1", "Some content", "sha1", false},
		{"sha1 with base16 digest", "Some content", "sha1:9f1a6ecf74e9f9b1ae52e8eb581d420e63e8453a", true},
		{"SHA-1 with base16 digest", "Some content", "SHA-1:9f1a6ecf74e9f9b1ae52e8eb581d420e63e8453a", true},
		{"sha1 with base32 digest", "Some content", "sha1:T4NG5T3U5H43DLSS5DVVQHKCBZR6QRJ2", true},
		{"sha1 with base64 digest", "Some content", "sha1:nxpuz3Tp+bGuUujrWB1CDmPoRTo=", true},
		{"sha1 with unpadded base64 digest", "Some content", "sha1:nxpuz3Tp+bGuUujrWB1CDmPoRTo", true},
		{"sha1 with wrong digest", "Some content", "sha1:123", false},
		{"sha256", "Some content", "sha256", false},
		{"sha256 with base16 digest", "Some content", "sha256:9c6609fc5111405ea3f5bb3d1f6b5a5efd19a0cec53d85893fd96d265439cd5b", true},
		{"SHA-256 with base16 digest", "Some content", "SHA-256:9c6609fc5111405ea3f5bb3d1f6b5a5efd19a0cec53d85893fd96d265439cd5b", true},
		{"sha256 with base32 digest", "Some content", "sha256:TRTAT7CRCFAF5I7VXM6R6222L36RTIGOYU6YLCJ73FWSMVBZZVNQ====", true},
		{"sha256 with unpadded base32 digest", "Some content", "sha256:TRTAT7CRCFAF5I7VXM6R6222L36RTIGOYU6YLCJ73FWSMVBZZVNQ", true},
		{"sha256 with base64 digest", "Some content", "sha256:nGYJ/FERQF6j9bs9H2taXv0ZoM7FPYWJP9ltJlQ5zVs=", true},
		{"sha256 with unpadded base64 digest", "Some content", "sha256:nGYJ/FERQF6j9bs9H2taXv0ZoM7FPYWJP9ltJlQ5zVs", true},
		{"sha256 with wrong digest", "Some content", "sha256:123", false},
		{"sha512", "Some content", "sha512", false},
		{"sha512 with base16 digest", "Some content", "sha512:b20d977718ed67f2bf7620ee2d982fd850c4883ec8d048440fe7b6a86cf6322fd791c47b0c7469dbeef3e339032e1abc4bcebe5efc104bc19a117bfef4478605", true},
		{"sha512 with base32 digest", "Some content", "sha512:WIGZO5YY5VT7FP3WEDXC3GBP3BIMJCB6ZDIEQRAP463KQ3HWGIX5PEOEPMGHI2O353Z6GOIDFYNLYS6OXZPPYECLYGNBC6766RDYMBI=", true},
		{"sha512 with unpadded base32 digest", "Some content", "sha512:WIGZO5YY5VT7FP3WEDXC3GBP3BIMJCB6ZDIEQRAP463KQ3HWGIX5PEOEPMGHI2O353Z6GOIDFYNLYS6OXZPPYECLYGNBC6766RDYMBI", true},
		{"sha512 with base64 digest", "Some content", "sha512:sg2XdxjtZ/K/diDuLZgv2FDEiD7I0EhED+e2qGz2Mi/XkcR7DHRp2+7z4zkDLhq8S86+XvwQS8GaEXv+9EeGBQ==", true},
		{"sha512 with unpadded base64 digest", "Some content", "sha512:sg2XdxjtZ/K/diDuLZgv2FDEiD7I0EhED+e2qGz2Mi/XkcR7DHRp2+7z4zkDLhq8S86+XvwQS8GaEXv+9EeGBQ", true},
		{"sha512 with wrong digest", "Some content", "sha512:123", false},
		{"uppercase base16 encoding", "Some content", "sha1:9F1A6ECF74E9F9B1AE52E8EB581D420E63E8453A", true},
		{"lowercase base32 encoding", "Some content", "sha1:t4ng5t3u5h43dlss5dvvqhkcbzr6qrj2", true},
		{"lowercase base64 encoding", "Some content", "sha1:nxpuz3tp+bguuujrwb1cdmporto=", false},
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

func TestDigestEncoding_Decode(t *testing.T) {
	tests := []struct {
		name      string
		encoding  DigestEncoding
		input     string
		wantError bool
	}{
		{
			name:      "valid Base16",
			encoding:  Base16,
			input:     "098f6bcd4621d373cade4e832627b4f6",
			wantError: false,
		},
		{
			name:      "valid Base32",
			encoding:  Base32,
			input:     "BT7V3XGGA4OVPYPVNTNJUMTXGY======",
			wantError: false,
		},
		{
			name:      "valid Base32 unpadded",
			encoding:  Base32,
			input:     "BT7V3XGGA4OVPYPVNTNJUMTXGY",
			wantError: false,
		},
		{
			name:      "valid Base64",
			encoding:  Base64,
			input:     "CY9rzUYh03PK3k6DJie09g==",
			wantError: false,
		},
		{
			name:      "valid Base64 unpadded",
			encoding:  Base64,
			input:     "CY9rzUYh03PK3k6DJie09g",
			wantError: false,
		},
		{
			name:      "invalid Base16",
			encoding:  Base16,
			input:     "ZZZZ",
			wantError: true,
		},
		{
			name:      "invalid Base32",
			encoding:  Base32,
			input:     "!!!invalid!!!",
			wantError: true,
		},
		{
			name:      "invalid Base64",
			encoding:  Base64,
			input:     "not@valid#base64",
			wantError: true,
		},
		{
			name:      "empty string",
			encoding:  Base16,
			input:     "",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.encoding.decode(tt.input)
			if tt.wantError && err == nil {
				t.Error("decode() expected error but got none")
			}
			if !tt.wantError && err != nil {
				t.Errorf("decode() unexpected error: %v", err)
			}
		})
	}
}

func TestDetectEncoding(t *testing.T) {
	tests := []struct {
		name            string
		algorithm       string
		digest          string
		defaultEncoding DigestEncoding
		want            DigestEncoding
	}{
		{
			name:            "md5 base16 (32 chars, no padding)",
			algorithm:       "md5",
			digest:          "098f6bcd4621d373cade4e832627b4f6",
			defaultEncoding: Base32,
			want:            Base16,
		},
		{
			name:            "md5 base32 (32 chars with padding)",
			algorithm:       "md5",
			digest:          "BT7V3XGGA4OVPYPVNTNJUMTXGY======",
			defaultEncoding: Base16,
			want:            Base32,
		},
		{
			name:            "md5 base64",
			algorithm:       "md5",
			digest:          "CY9rzUYh03PK3k6DJie09g==",
			defaultEncoding: Base16,
			want:            Base64,
		},
		{
			name:            "sha1 base16",
			algorithm:       "sha1",
			digest:          "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3",
			defaultEncoding: Base32,
			want:            Base16,
		},
		{
			name:            "sha1 base32",
			algorithm:       "sha1",
			digest:          "VFKJT6OMWGNKUHDIBR2TSHHJQ6MB7PJT",
			defaultEncoding: Base16,
			want:            Base32,
		},
		{
			name:            "sha1 base64",
			algorithm:       "sha1",
			digest:          "qUqP5cyxm6YcTAhz05Hph5gvu9M=",
			defaultEncoding: Base16,
			want:            Base64,
		},
		{
			name:            "sha256 base16",
			algorithm:       "sha256",
			digest:          "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
			defaultEncoding: Base32,
			want:            Base16,
		},
		{
			name:            "sha256 base32",
			algorithm:       "sha256",
			digest:          "T6DNAGGIJR6WTGRL5KQMKVNQCWRD7RYNFMFYELFNLRTBWMHQBEA=====",
			defaultEncoding: Base16,
			want:            Base32,
		},
		{
			name:            "sha256 base64",
			algorithm:       "sha256",
			digest:          "n4bQgYhMfWWaL+qgxVrQFaO/TxsrC4Is0V1sFbDwCgg=",
			defaultEncoding: Base16,
			want:            Base64,
		},
		{
			name:            "sha512 base16",
			algorithm:       "sha512",
			digest:          strings.Repeat("a", 128),
			defaultEncoding: Base32,
			want:            Base16,
		},
		{
			name:            "unknown length uses default",
			algorithm:       "sha1",
			digest:          "short",
			defaultEncoding: Base32,
			want:            Base32,
		},
		{
			name:            "empty digest uses default",
			algorithm:       "md5",
			digest:          "",
			defaultEncoding: Base16,
			want:            Base16,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := detectEncoding(tt.algorithm, tt.digest, tt.defaultEncoding)
			if got != tt.want {
				t.Errorf("detectEncoding() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNormalizeAlgorithmName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "sha-1 to sha1",
			input: "sha-1",
			want:  "sha1",
		},
		{
			name:  "SHA-1 to sha1",
			input: "SHA-1",
			want:  "sha1",
		},
		{
			name:  "sha-256 to sha256",
			input: "sha-256",
			want:  "sha256",
		},
		{
			name:  "SHA-256 to sha256",
			input: "SHA-256",
			want:  "sha256",
		},
		{
			name:  "sha-512 to sha512",
			input: "sha-512",
			want:  "sha512",
		},
		{
			name:  "SHA-512 to sha512",
			input: "SHA-512",
			want:  "sha512",
		},
		{
			name:  "md5 stays md5",
			input: "md5",
			want:  "md5",
		},
		{
			name:  "MD5 to md5",
			input: "MD5",
			want:  "md5",
		},
		{
			name:  "sha1 stays sha1",
			input: "sha1",
			want:  "sha1",
		},
		{
			name:  "SHA1 to sha1",
			input: "SHA1",
			want:  "sha1",
		},
		{
			name:  "unknown algorithm lowercased",
			input: "CUSTOM",
			want:  "custom",
		},
		{
			name:  "empty string",
			input: "",
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeAlgorithmName(tt.input)
			if got != tt.want {
				t.Errorf("normalizeAlgorithmName() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDigest_UpdateDigest(t *testing.T) {
	d, err := newDigest("sha1:oldvalue", Base16)
	require.NoError(t, err)

	_, err = d.Write([]byte("test"))
	require.NoError(t, err)

	// Save old hash
	oldHash := d.hash

	// Update digest
	d.updateDigest()

	// Hash should have changed
	if d.hash == oldHash {
		t.Error("updateDigest() should update the hash value")
	}

	// New hash should be the computed value
	expected := d.encoding.encode(d)
	if d.hash != expected {
		t.Errorf("updateDigest() hash = %q, want %q", d.hash, expected)
	}
}

func TestDigestFilterReader(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{
			name:    "small content",
			content: "test",
		},
		{
			name:    "empty content",
			content: "",
		},
		{
			name:    "large content",
			content: strings.Repeat("a", 10000),
		},
		{
			name:    "binary content",
			content: string([]byte{0, 1, 2, 3, 255, 254}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d1, err := newDigest("sha1", Base16)
			require.NoError(t, err)

			d2, err := newDigest("md5", Base16)
			require.NoError(t, err)

			src := strings.NewReader(tt.content)
			reader := newDigestFilterReader(src, d1, d2)

			// Read all content
			data, err := io.ReadAll(reader)
			require.NoError(t, err)

			// Verify content is unchanged
			if string(data) != tt.content {
				t.Error("digestFilterReader should not modify content")
			}

			// Verify both digests were computed
			if d1.count != int64(len(tt.content)) {
				t.Errorf("d1.count = %d, want %d", d1.count, len(tt.content))
			}
			if d2.count != int64(len(tt.content)) {
				t.Errorf("d2.count = %d, want %d", d2.count, len(tt.content))
			}
		})
	}
}

func TestDigestFilterReader_PartialReads(t *testing.T) {
	content := "hello world test content"
	d, err := newDigest("sha1", Base16)
	require.NoError(t, err)

	src := strings.NewReader(content)
	reader := newDigestFilterReader(src, d)

	// Read in small chunks
	buf := make([]byte, 5)
	var result bytes.Buffer

	for {
		n, err := reader.Read(buf)
		if n > 0 {
			result.Write(buf[:n])
		}
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}

	// Verify content
	if result.String() != content {
		t.Error("partial reads should produce same content")
	}

	// Verify digest count
	if d.count != int64(len(content)) {
		t.Errorf("digest count = %d, want %d", d.count, len(content))
	}
}

func Test_digest_validate_DecodeError(t *testing.T) {
	// Create a digest with a Base32 encoding but an invalid Base32 hash string.
	// This triggers the decode error path in validate().
	d, err := newDigest("sha1:!!!INVALID_BASE32!!!", Base32)
	require.NoError(t, err)

	_, _ = d.Write([]byte("some data"))

	err = d.validate()
	assert.Error(t, err, "validate() should return error when decode fails on malformed hash")
}
