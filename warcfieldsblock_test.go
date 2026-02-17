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
	"testing/iotest"

	"github.com/nlnwa/gowarc/v2/internal/diskbuffer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func Test_warcfieldsBlock_Write(t *testing.T) {
	tests := []struct {
		name        string
		content     string
		wantContain []string
	}{
		{
			name:        "single field",
			content:     "software: Heritrix\r\n",
			wantContain: []string{"Software: Heritrix"},
		},
		{
			name:        "multiple fields",
			content:     "software: Heritrix\r\nformat: WARC File Format 1.1\r\n",
			wantContain: []string{"Software: Heritrix", "Format: WARC File Format 1.1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := newDigest("sha1", Base16)
			require.NoError(t, err)
			validation := &Validation{}
			o := defaultWarcRecordOptions()
			block, err := newWarcFieldsBlock(&o, &WarcFields{}, strings.NewReader(tt.content), d, validation)
			require.NoError(t, err)

			wfblock := block.(*warcFieldsBlock)
			var buf bytes.Buffer
			n, err := wfblock.Write(&buf)
			require.NoError(t, err)
			assert.Greater(t, n, int64(0))

			output := buf.String()
			for _, want := range tt.wantContain {
				assert.Contains(t, output, want)
			}
			// Write should end with CRLF
			assert.True(t, strings.HasSuffix(output, "\r\n"))
		})
	}
}

func Test_newWarcFieldsBlock_ReadError_ErrFail(t *testing.T) {
	d, _ := newDigest("sha1", Base16)
	validation := &Validation{}
	opts := &warcRecordOptions{errSyntax: ErrFail}

	_, err := newWarcFieldsBlock(opts, &WarcFields{}, iotest.ErrReader(io.ErrUnexpectedEOF), d, validation)
	require.Error(t, err)
}

func Test_newWarcFieldsBlock_ReadError_ErrWarn(t *testing.T) {
	d, _ := newDigest("sha1", Base16)
	validation := &Validation{}
	opts := &warcRecordOptions{errSyntax: ErrWarn}

	block, err := newWarcFieldsBlock(opts, &WarcFields{}, iotest.ErrReader(io.ErrUnexpectedEOF), d, validation)
	require.NoError(t, err)
	assert.NotNil(t, block)
	assert.False(t, validation.Valid())
}

func Test_newWarcFieldsBlock_BlockValidationError_ErrBlockFail(t *testing.T) {
	d, _ := newDigest("sha1", Base16)
	validation := &Validation{}
	opts := &warcRecordOptions{errBlock: ErrFail, errSyntax: ErrWarn}

	// Malformed warc fields content - errSyntax=ErrWarn puts error in blockValidation, errBlock=ErrFail returns it
	_, err := newWarcFieldsBlock(opts, &WarcFields{}, strings.NewReader("invalid-no-colon-line\r\n"), d, validation)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "warc fields block")
}

func Test_newWarcFieldsBlock_BlockValidationError_ErrBlockWarn(t *testing.T) {
	d, _ := newDigest("sha1", Base16)
	validation := &Validation{}
	opts := &warcRecordOptions{errBlock: ErrWarn, errSyntax: ErrWarn}

	block, err := newWarcFieldsBlock(opts, &WarcFields{}, strings.NewReader("invalid-no-colon-line\r\n"), d, validation)
	require.NoError(t, err)
	assert.NotNil(t, block)
	assert.False(t, validation.Valid())
}

func Test_newWarcFieldsBlock_FixWarcFieldsBlockErrors(t *testing.T) {
	d, _ := newDigest("sha1", Base16)
	validation := &Validation{}
	opts := &warcRecordOptions{fixWarcFieldsBlockErrors: true}

	// Even with invalid content, fix should not error out
	block, err := newWarcFieldsBlock(opts, &WarcFields{}, strings.NewReader("software: test\r\n"), d, validation)
	require.NoError(t, err)
	assert.NotNil(t, block)
}

func Test_warcfieldsBlock_Write_Error(t *testing.T) {
	d, _ := newDigest("sha1", Base16)
	o := defaultWarcRecordOptions()
	validation := &Validation{}
	block, err := newWarcFieldsBlock(&o, &WarcFields{}, strings.NewReader("name: value\r\n"), d, validation)
	require.NoError(t, err)

	wfblock := block.(*warcFieldsBlock)
	w := &failWriterAt{failAfter: 0, err: io.ErrClosedPipe}
	_, err = wfblock.Write(w)
	assert.Error(t, err)
}
