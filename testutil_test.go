package gowarc

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type cacheTest struct {
	name         string
	data         io.Reader
	wantCacheErr bool
}

func validateCacheTest(t *testing.T, block Block, expectedContent string, expectedDigest string, wantCacheErr bool) {
	t.Helper()

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
	t.Helper()

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
	t.Helper()

	payloadBlock, ok := block.(PayloadBlock)
	if !ok {
		t.Fatalf("block is not a PayloadBlock")
	}

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
	t.Helper()

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
