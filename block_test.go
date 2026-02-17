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
	"fmt"
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

func Test_genericBlock_RawBytes_NonCached_ReAccess(t *testing.T) {
	d, err := newDigest("sha1", Base16)
	require.NoError(t, err)
	// Use strings.Reader which is NOT a Seeker (wrapped)
	block := newGenericBlock(&warcRecordOptions{}, iotest.HalfReader(strings.NewReader("foo")), d)

	r, err := block.RawBytes()
	require.NoError(t, err)
	_, _ = io.ReadAll(r)

	// Second access on non-cached block should fail
	_, err = block.RawBytes()
	require.Error(t, err)
	assert.Equal(t, errContentReAccessed, err)
}

func Test_genericBlock_RawBytes_Cached_ReAccess(t *testing.T) {
	d, err := newDigest("sha1", Base16)
	require.NoError(t, err)
	block := newGenericBlock(&warcRecordOptions{}, strings.NewReader("foo"), d)

	require.NoError(t, block.Cache())
	assert.True(t, block.IsCached())

	r1, err := block.RawBytes()
	require.NoError(t, err)
	data1, _ := io.ReadAll(r1)

	// Re-access on cached block should succeed
	r2, err := block.RawBytes()
	require.NoError(t, err)
	data2, _ := io.ReadAll(r2)
	assert.Equal(t, string(data1), string(data2))
}

func Test_genericBlock_Close_NonCloser(t *testing.T) {
	d, err := newDigest("sha1", Base16)
	require.NoError(t, err)
	// strings.Reader does not implement io.Closer
	block := newGenericBlock(&warcRecordOptions{}, strings.NewReader("foo"), d)

	err = block.Close()
	assert.NoError(t, err)
}

func Test_genericBlock_Close_Closer(t *testing.T) {
	d, err := newDigest("sha1", Base16)
	require.NoError(t, err)
	buf := diskbuffer.New()
	_, _ = buf.WriteString("foo")
	block := newGenericBlock(&warcRecordOptions{}, buf, d)

	err = block.Close()
	assert.NoError(t, err)
}

func Test_genericBlock_Cache_AlreadyCached(t *testing.T) {
	d, err := newDigest("sha1", Base16)
	require.NoError(t, err)
	buf := diskbuffer.New()
	_, _ = buf.WriteString("foo")
	block := newGenericBlock(&warcRecordOptions{}, buf, d)

	assert.True(t, block.IsCached())
	require.NoError(t, block.Cache())
}

func Test_genericBlock_Size(t *testing.T) {
	d, err := newDigest("sha1", Base16)
	require.NoError(t, err)
	block := newGenericBlock(&warcRecordOptions{}, strings.NewReader("hello"), d)

	assert.Equal(t, int64(5), block.Size())
}

// pipeReader is a simple reader that does NOT implement io.Seeker.
type pipeReader struct {
	data []byte
	pos  int
}

func (p *pipeReader) Read(b []byte) (int, error) {
	if p.pos >= len(p.data) {
		return 0, io.EOF
	}
	n := copy(b, p.data[p.pos:])
	p.pos += n
	return n, nil
}

func Test_genericBlock_Cache_RawBytesError(t *testing.T) {
	// Exercise the Cache() error path where RawBytes() fails.
	// Use a non-seekable reader so IsCached() returns false.
	d, err := newDigest("sha1", Base16)
	require.NoError(t, err)
	block := newGenericBlock(&warcRecordOptions{}, &pipeReader{data: []byte("hello")}, d)

	assert.False(t, block.IsCached())

	// First RawBytes() succeeds (creates and returns filter reader)
	r, err := block.RawBytes()
	require.NoError(t, err)
	_, _ = io.ReadAll(r)

	// Now calling Cache() should fail because RawBytes() returns errContentReAccessed
	err = block.Cache()
	assert.Error(t, err)
	assert.ErrorIs(t, err, errContentReAccessed)
}

type failSeeker struct {
	*strings.Reader
	seekErr error
}

func (f *failSeeker) Seek(offset int64, whence int) (int64, error) {
	return 0, f.seekErr
}

func Test_genericBlock_RawBytes_SeekError(t *testing.T) {
	// Exercise the Seek error path in RawBytes() for a cached (seekable) block
	d, err := newDigest("sha1", Base16)
	require.NoError(t, err)

	fs := &failSeeker{Reader: strings.NewReader("hello"), seekErr: fmt.Errorf("seek failed")}
	block := &genericBlock{
		rawBytes:    fs,
		blockDigest: d,
		opts:        &warcRecordOptions{},
	}

	// First call succeeds (creates filterReader)
	r, err := block.RawBytes()
	require.NoError(t, err)
	_, _ = io.ReadAll(r)

	// Second call attempts Seek, which fails
	_, err = block.RawBytes()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "seek failed")
}

