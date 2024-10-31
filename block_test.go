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
	"io"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/nlnwa/gowarc/v2/internal/diskbuffer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_genericBlock_BlockDigest(t *testing.T) {
	content := "foo"
	digest := "sha1:0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33"

	tests := []blockDigestTest{
		{
			"strings.Reader",
			strings.NewReader(content),
		},
		{
			"diskbuffer.Buffer",
			func() io.Reader { d := diskbuffer.New(); _, _ = d.WriteString(content); return d }(),
		},
		{
			"iotest.HalfReader",
			iotest.HalfReader(strings.NewReader(content)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := newDigest("sha1", Base16)
			require.NoError(t, err)
			block := newGenericBlock(&warcRecordOptions{}, tt.data, d)

			validateBlockDigestTest(t, block, digest)
		})
	}
}

func Test_genericBlock_Cache(t *testing.T) {
	content := "foo"
	digest := "sha1:0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33"

	tests := []cacheTest{
		{
			"strings.Reader",
			strings.NewReader(content),
			false,
		},
		{
			"diskbuffer.Buffer",
			func() io.Reader { d := diskbuffer.New(); _, _ = d.WriteString(content); return d }(),
			false,
		},
		{
			"iotest.HalfReader",
			iotest.HalfReader(strings.NewReader(content)),
			false,
		},
		{
			"ReplaceErrReader",
			ReplaceErrReader(strings.NewReader(content), io.ErrUnexpectedEOF),
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := newDigest("sha1", Base16)
			require.NoError(t, err)
			block := newGenericBlock(&warcRecordOptions{}, tt.data, d)

			validateCacheTest(t, block, content, digest, tt.wantCacheErr)
		})
	}
}

func Test_genericBlock_IsCached(t *testing.T) {
	content := "foo"

	tests := []isCachedTest{
		{
			"strings.Reader",
			strings.NewReader(content),
			true,
		},
		{
			"diskbuffer.Buffer",
			func() io.Reader { d := diskbuffer.New(); _, _ = d.WriteString(content); return d }(),
			true,
		},
		{
			"iotest.HalfReader",
			iotest.HalfReader(strings.NewReader(content)),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := newDigest("sha1", Base16)
			require.NoError(t, err)
			block := newGenericBlock(&warcRecordOptions{}, tt.data, d)

			got := block.IsCached()
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_genericBlock_RawBytes(t *testing.T) {
	content := "foo"
	digest := "sha1:0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33"

	tests := []rawBytesTest{
		{
			"strings.Reader",
			strings.NewReader(content),
			false,
		},
		{
			"diskbuffer.Buffer",
			func() io.Reader { d := diskbuffer.New(); _, _ = d.WriteString(content); return d }(),
			false,
		},
		{
			"iotest.HalfReader",
			iotest.HalfReader(strings.NewReader(content)),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := newDigest("sha1", Base16)
			require.NoError(t, err)
			block := newGenericBlock(&warcRecordOptions{}, tt.data, d)

			validateRawBytesTest(t, tt, block, content, digest)
		})
	}
}

func Test_warcfieldsBlock_BlockDigest(t *testing.T) {
	content := "foo: bar\r\ncontent-type:bb\r\n"
	digest := "sha1:a1d43d400c5985bee035c4e5a2e08f3d57989596"

	tests := []blockDigestTest{
		{
			"strings.Reader",
			strings.NewReader(content),
		},
		{
			"diskbuffer.Buffer",
			func() io.Reader { d := diskbuffer.New(); _, _ = d.WriteString(content); return d }(),
		},
		{
			"iotest.HalfReader",
			iotest.HalfReader(strings.NewReader(content)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := newDigest("sha1", Base16)
			require.NoError(t, err)
			validation := &Validation{}
			o := defaultWarcRecordOptions()
			block, err := newWarcFieldsBlock(&o, &WarcFields{}, tt.data, d, validation)
			require.NoError(t, err)
			require.True(t, validation.Valid(), validation)

			validateBlockDigestTest(t, block, digest)
		})
	}
}

func Test_warcfieldsBlock_Cache(t *testing.T) {
	content := "foo: bar\r\ncontent-type:bb\r\n"
	digest := "sha1:a1d43d400c5985bee035c4e5a2e08f3d57989596"

	tests := []cacheTest{
		{
			"strings.Reader",
			strings.NewReader(content),
			false,
		},
		{
			"diskbuffer.Buffer",
			func() io.Reader { d := diskbuffer.New(); _, _ = d.WriteString(content); return d }(),
			false,
		},
		{
			"iotest.HalfReader",
			iotest.HalfReader(strings.NewReader(content)),
			false,
		},
		{
			"ReplaceErrReader",
			ReplaceErrReader(strings.NewReader(content), io.ErrUnexpectedEOF),
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := newDigest("sha1", Base16)
			require.NoError(t, err)
			validation := &Validation{}
			o := defaultWarcRecordOptions()
			block, err := newWarcFieldsBlock(&o, &WarcFields{}, tt.data, d, validation)
			require.NoError(t, err)
			if tt.wantCacheErr {
				require.False(t, validation.Valid(), validation)
			} else {
				require.True(t, validation.Valid(), validation)
			}

			validateCacheTest(t, block, content, digest, false)
		})
	}
}

func Test_warcfieldsBlock_IsCached(t *testing.T) {
	content := "foo: bar\r\ncontent-type:bb\r\n"

	tests := []isCachedTest{
		{
			"strings.Reader",
			strings.NewReader(content),
			true,
		},
		{
			"diskbuffer.Buffer",
			func() io.Reader { d := diskbuffer.New(); _, _ = d.WriteString(content); return d }(),
			true,
		},
		{
			"iotest.HalfReader",
			iotest.HalfReader(strings.NewReader(content)),
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := newDigest("sha1", Base16)
			require.NoError(t, err)
			validation := &Validation{}
			o := defaultWarcRecordOptions()
			block, err := newWarcFieldsBlock(&o, &WarcFields{}, tt.data, d, validation)
			require.NoError(t, err)
			require.True(t, validation.Valid(), validation)

			got := block.IsCached()
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_warcfieldsBlock_RawBytes(t *testing.T) {
	content := "foo: bar\r\ncontent-type:bb\r\n"
	digest := "sha1:a1d43d400c5985bee035c4e5a2e08f3d57989596"

	tests := []rawBytesTest{
		{
			"strings.Reader",
			strings.NewReader(content),
			false,
		},
		{
			"diskbuffer.Buffer",
			func() io.Reader { d := diskbuffer.New(); _, _ = d.WriteString(content); return d }(),
			false,
		},
		{
			"iotest.HalfReader",
			iotest.HalfReader(strings.NewReader(content)),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := newDigest("sha1", Base16)
			require.NoError(t, err)
			validation := &Validation{}
			o := defaultWarcRecordOptions()
			block, err := newWarcFieldsBlock(&o, &WarcFields{}, tt.data, d, validation)
			require.NoError(t, err)
			require.True(t, validation.Valid(), validation)

			validateRawBytesTest(t, tt, block, content, digest)
		})
	}
}

func Test_httpRequestBlock_BlockDigest(t *testing.T) {
	content := "GET / HTTP/1.0\n" +
		"Host: example.com\n" +
		"Accept-Language: en-US,en;q=0.8,ru;q=0.6\n" +
		"Referer: http://example.com/foo.html\n" +
		"Connection: close\n" +
		"User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36\n\n"
	digest := "sha1:a3781ff1fc3fb52318f623e22c85d63d74c12932"
	payloadDigest := "sha1:da39a3ee5e6b4b0d3255bfef95601890afd80709"

	tests := []blockDigestTest{
		{
			"strings.Reader",
			strings.NewReader(content),
		},
		{
			"diskbuffer.Buffer",
			func() io.Reader { d := diskbuffer.New(); _, _ = d.WriteString(content); return d }(),
		},
		{
			"iotest.HalfReader",
			iotest.HalfReader(strings.NewReader(content)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blockDigest, err := newDigest("sha1", Base16)
			require.NoError(t, err)
			pDigest, err := newDigest("sha1", Base16)
			require.NoError(t, err)
			val := &Validation{}
			block, err := newHttpBlock(&warcRecordOptions{}, &WarcFields{}, tt.data, blockDigest, pDigest, val)
			require.NoError(t, err)
			require.True(t, val.Valid(), val.String())

			validateBlockDigestTest(t, block, digest)
			validatePayloadDigestTest(t, block, payloadDigest)
		})
	}
}

func Test_httpRequestBlock_Cache(t *testing.T) {
	content := "GET / HTTP/1.0\n" +
		"Host: example.com\n" +
		"Accept-Language: en-US,en;q=0.8,ru;q=0.6\n" +
		"Referer: http://example.com/foo.html\n" +
		"Connection: close\n" +
		"User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36\n\n"
	digest := "sha1:a3781ff1fc3fb52318f623e22c85d63d74c12932"

	tests := []cacheTest{
		{
			"strings.Reader",
			strings.NewReader(content),
			false,
		},
		{
			"diskbuffer.Buffer",
			func() io.Reader { d := diskbuffer.New(); _, _ = d.WriteString(content); return d }(),
			false,
		},
		{
			"iotest.HalfReader",
			iotest.HalfReader(strings.NewReader(content)),
			false,
		},
		{
			"ReplaceErrReader",
			ReplaceErrReader(strings.NewReader(content), io.ErrUnexpectedEOF),
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blockDigest, err := newDigest("sha1", Base16)
			require.NoError(t, err)
			pDigest, err := newDigest("sha1", Base16)
			require.NoError(t, err)
			val := &Validation{}
			block, err := newHttpBlock(&warcRecordOptions{}, &WarcFields{}, tt.data, blockDigest, pDigest, val)
			require.NoError(t, err)
			require.True(t, val.Valid(), val.String())

			validateCacheTest(t, block, content, digest, tt.wantCacheErr)
		})
	}
}

func Test_httpRequestBlock_IsCached(t *testing.T) {
	content := "GET / HTTP/1.0\n" +
		"Host: example.com\n" +
		"Accept-Language: en-US,en;q=0.8,ru;q=0.6\n" +
		"Referer: http://example.com/foo.html\n" +
		"Connection: close\n" +
		"User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36\n\n"

	tests := []isCachedTest{
		{
			"strings.Reader",
			strings.NewReader(content),
			false,
		},
		{
			"diskbuffer.Buffer",
			func() io.Reader { d := diskbuffer.New(); _, _ = d.WriteString(content); return d }(),
			true,
		},
		{
			"iotest.HalfReader",
			iotest.HalfReader(strings.NewReader(content)),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blockDigest, err := newDigest("sha1", Base16)
			require.NoError(t, err)
			pDigest, err := newDigest("sha1", Base16)
			require.NoError(t, err)
			val := &Validation{}
			block, err := newHttpBlock(&warcRecordOptions{}, &WarcFields{}, tt.data, blockDigest, pDigest, val)
			require.NoError(t, err)
			require.True(t, val.Valid(), val.String())

			got := block.IsCached()
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_httpRequestBlock_RawBytes(t *testing.T) {
	content := "GET / HTTP/1.0\n" +
		"Host: example.com\n" +
		"Accept-Language: en-US,en;q=0.8,ru;q=0.6\n" +
		"Referer: http://example.com/foo.html\n" +
		"Connection: close\n" +
		"User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36\n\n"
	digest := "sha1:a3781ff1fc3fb52318f623e22c85d63d74c12932"

	tests := []rawBytesTest{
		{
			"strings.Reader",
			strings.NewReader(content),
			false,
		},
		{
			"diskbuffer.Buffer",
			func() io.Reader { d := diskbuffer.New(); _, _ = d.WriteString(content); return d }(),
			false,
		},
		{
			"iotest.HalfReader",
			iotest.HalfReader(strings.NewReader(content)),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blockDigest, err := newDigest("sha1", Base16)
			require.NoError(t, err)
			pDigest, err := newDigest("sha1", Base16)
			require.NoError(t, err)
			val := &Validation{}
			block, err := newHttpBlock(&warcRecordOptions{}, &WarcFields{}, tt.data, blockDigest, pDigest, val)
			require.NoError(t, err)
			require.True(t, val.Valid(), val.String())

			validateRawBytesTest(t, tt, block, content, digest)
		})
	}
}

func Test_httpResponseBlock_BlockDigest(t *testing.T) {
	content := "HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n" +
		"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n" +
		"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the content"
	digest := "sha1:b285747ad7cc57aa74bce2e30b453c8d1cb71ba4"
	payloadDigest := "sha1:c37ffb221569c553a2476c22c7dad429f3492977"

	tests := []blockDigestTest{
		{
			"strings.Reader",
			strings.NewReader(content),
		},
		{
			"diskbuffer.Buffer",
			func() io.Reader { d := diskbuffer.New(); _, _ = d.WriteString(content); return d }(),
		},
		{
			"iotest.HalfReader",
			iotest.HalfReader(strings.NewReader(content)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blockDigest, err := newDigest("sha1", Base16)
			require.NoError(t, err)
			pDigest, err := newDigest("sha1", Base16)
			require.NoError(t, err)
			val := &Validation{}
			block, err := newHttpBlock(&warcRecordOptions{}, &WarcFields{}, tt.data, blockDigest, pDigest, val)
			require.NoError(t, err)
			require.True(t, val.Valid(), val.String())

			validateBlockDigestTest(t, block, digest)
			validatePayloadDigestTest(t, block, payloadDigest)
		})
	}
}

func Test_httpResponseBlock_Cache(t *testing.T) {
	content := "HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n" +
		"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n" +
		"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the content"
	digest := "sha1:b285747ad7cc57aa74bce2e30b453c8d1cb71ba4"

	tests := []cacheTest{
		{
			"strings.Reader",
			strings.NewReader(content),
			false,
		},
		{
			"diskbuffer.Buffer",
			func() io.Reader { d := diskbuffer.New(); _, _ = d.WriteString(content); return d }(),
			false,
		},
		{
			"iotest.HalfReader",
			iotest.HalfReader(strings.NewReader(content)),
			false,
		},
		{
			"ReplaceErrReader",
			ReplaceErrReader(strings.NewReader(content), io.ErrUnexpectedEOF),
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blockDigest, err := newDigest("sha1", Base16)
			require.NoError(t, err)
			pDigest, err := newDigest("sha1", Base16)
			require.NoError(t, err)
			val := &Validation{}
			block, err := newHttpBlock(&warcRecordOptions{}, &WarcFields{}, tt.data, blockDigest, pDigest, val)
			require.NoError(t, err)
			require.True(t, val.Valid(), val.String())

			validateCacheTest(t, block, content, digest, tt.wantCacheErr)
		})
	}
}

func Test_httpResponseBlock_IsCached(t *testing.T) {
	content := "HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n" +
		"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n" +
		"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the content"

	tests := []isCachedTest{
		{
			"strings.Reader",
			strings.NewReader(content),
			false,
		},
		{
			"diskbuffer.Buffer",
			func() io.Reader { d := diskbuffer.New(); _, _ = d.WriteString(content); return d }(),
			true,
		},
		{
			"iotest.HalfReader",
			iotest.HalfReader(strings.NewReader(content)),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blockDigest, err := newDigest("sha1", Base16)
			require.NoError(t, err)
			pDigest, err := newDigest("sha1", Base16)
			require.NoError(t, err)
			val := &Validation{}
			block, err := newHttpBlock(&warcRecordOptions{}, &WarcFields{}, tt.data, blockDigest, pDigest, val)
			require.NoError(t, err)
			require.True(t, val.Valid(), val.String())

			got := block.IsCached()
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_httpResponseBlock_RawBytes(t *testing.T) {
	content := "HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n" +
		"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n" +
		"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the content"
	digest := "sha1:b285747ad7cc57aa74bce2e30b453c8d1cb71ba4"

	tests := []rawBytesTest{
		{
			"strings.Reader",
			strings.NewReader(content),
			false,
		},
		{
			"diskbuffer.Buffer",
			func() io.Reader { d := diskbuffer.New(); _, _ = d.WriteString(content); return d }(),
			false,
		},
		{
			"iotest.HalfReader",
			iotest.HalfReader(strings.NewReader(content)),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blockDigest, err := newDigest("sha1", Base16)
			require.NoError(t, err)
			pDigest, err := newDigest("sha1", Base16)
			require.NoError(t, err)
			val := &Validation{}
			block, err := newHttpBlock(&warcRecordOptions{}, &WarcFields{}, tt.data, blockDigest, pDigest, val)
			require.NoError(t, err)
			require.True(t, val.Valid(), val.String())

			validateRawBytesTest(t, tt, block, content, digest)
		})
	}
}

type cacheTest struct {
	name         string
	data         io.Reader
	wantCacheErr bool
}

func validateCacheTest(t *testing.T, block Block, expectedContent string, expectedDigest string, wantCacheErr bool) {
	err := block.Cache()
	if wantCacheErr {
		assert.Error(t, err)
	} else {
		assert.NoError(t, err)
	}
	assert.True(t, block.IsCached())

	// Reading content twice should be ok
	got, err := block.RawBytes()
	require.NoError(t, err)
	content, err := io.ReadAll(got)
	require.NoError(t, err)
	assert.Equal(t, expectedContent, string(content))
	got, err = block.RawBytes()
	require.NoError(t, err)
	content, err = io.ReadAll(got)
	require.NoError(t, err)
	assert.Equal(t, expectedContent, string(content))

	// BlockDigest should be ok
	gotDigest := block.BlockDigest()
	assert.Equal(t, expectedDigest, gotDigest)
}

type blockDigestTest struct {
	name string
	data io.Reader
}

func validateBlockDigestTest(t *testing.T, block Block, expectedDigest string) {
	got := block.BlockDigest()
	assert.Equal(t, expectedDigest, got)

	// Repeated call to BlockDigest should be ok
	got = block.BlockDigest()
	assert.Equal(t, expectedDigest, got)

	if block.IsCached() {
		// Call to RawBytes after call to BlockDigest should be ok
		_, err := block.RawBytes()
		require.NoError(t, err)
	} else {
		// Call to RawBytes after call to BlockDigest should fail
		_, err := block.RawBytes()
		require.Error(t, err)
	}
}

func validatePayloadDigestTest(t *testing.T, block Block, expectedDigest string) {
	if payloadBlock, ok := block.(PayloadBlock); ok {
		got := payloadBlock.PayloadDigest()
		assert.Equal(t, expectedDigest, got)

		// Repeated call to PayloadDigest should be ok
		got = payloadBlock.PayloadDigest()
		assert.Equal(t, expectedDigest, got)

		if payloadBlock.IsCached() {
			// Call to RawBytes after call to PayloadDigest should be ok
			_, err := payloadBlock.RawBytes()
			require.NoError(t, err)
		} else {
			// Call to RawBytes after call to PayloadDigest should fail
			_, err := payloadBlock.RawBytes()
			require.Error(t, err)
		}
	} else {
		panic("not a payload block")
	}
}

type isCachedTest struct {
	name string
	data io.Reader
	want bool
}

type rawBytesTest struct {
	name    string
	data    io.Reader
	wantErr bool
}

func validateRawBytesTest(t *testing.T, tt rawBytesTest, block Block, expectedContent string, expectedDigest string) {
	got, err := block.RawBytes()
	if tt.wantErr {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
	}

	content, err := io.ReadAll(got)
	require.NoError(t, err)
	assert.Equal(t, expectedContent, string(content))

	if block.IsCached() {
		// Repeated call to RawBytes should be ok
		got, err := block.RawBytes()
		require.NoError(t, err)

		content, err := io.ReadAll(got)
		require.NoError(t, err)
		assert.Equal(t, expectedContent, string(content))
	} else {
		// Repeated call to RawBytes should fail
		_, err := block.RawBytes()
		require.Error(t, err)
	}

	// Call to BlockDigest after call to RawBytes should be ok
	gotDigest := block.BlockDigest()
	assert.Equal(t, expectedDigest, gotDigest)
}

// ReplaceErrReader returns an io.Reader that returns err instead of io.EOF.
func ReplaceErrReader(r io.Reader, err error) io.Reader {
	return &replaceErrReader{r: r, err: err}
}

type replaceErrReader struct {
	r   io.Reader
	err error
}

func (r *replaceErrReader) Read(p []byte) (int, error) {
	i, e := r.r.Read(p)
	if e == io.EOF {
		e = r.err
	}
	return i, e
}
