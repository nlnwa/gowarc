package diskbuffer

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileBuffer_Basic(t *testing.T) {
	fb, err := newFileBuffer(100, t.TempDir())
	require.NoError(t, err)
	defer func() { assert.NoError(t, fb.close()) }()

	assert.Equal(t, int64(0), fb.Size())
	assert.Equal(t, int64(100), fb.Limit())

	n, err := fb.write([]byte("hello"))
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, int64(5), fb.Size())
}

func TestFileBuffer_Write_ExceedsLimit(t *testing.T) {
	fb, err := newFileBuffer(5, t.TempDir())
	require.NoError(t, err)
	defer func() { assert.NoError(t, fb.close()) }()

	// Write more than limit
	n, err := fb.write([]byte("hello world"))
	assert.IsType(t, ErrMaxSizeExceeded(0), err)
	assert.Equal(t, 5, n) // partial write

	// Writing when no free space
	n, err = fb.write([]byte("x"))
	assert.IsType(t, ErrMaxSizeExceeded(0), err)
	assert.Equal(t, 0, n)
}

func TestFileBuffer_ReadAt(t *testing.T) {
	fb, err := newFileBuffer(100, t.TempDir())
	require.NoError(t, err)
	defer func() { assert.NoError(t, fb.close()) }()

	_, _ = fb.write([]byte("ABCDE"))

	buf := make([]byte, 3)
	n, rerr := fb.readAt(2, buf)
	require.NoError(t, rerr)
	assert.Equal(t, 3, n)
	assert.Equal(t, "CDE", string(buf))

	// Empty buffer case
	fb2, err := newFileBuffer(100, t.TempDir())
	require.NoError(t, err)
	defer func() { assert.NoError(t, fb2.close()) }()

	_, rerr = fb2.readAt(0, buf)
	assert.ErrorIs(t, rerr, io.EOF)
}

func TestFileBuffer_ReadByteAt(t *testing.T) {
	fb, err := newFileBuffer(100, t.TempDir())
	require.NoError(t, err)
	defer func() { assert.NoError(t, fb.close()) }()

	_, _ = fb.write([]byte("XY"))
	c, rerr := fb.readByteAt(0)
	require.NoError(t, rerr)
	assert.Equal(t, byte('X'), c)

	c, rerr = fb.readByteAt(1)
	require.NoError(t, rerr)
	assert.Equal(t, byte('Y'), c)
}

func TestFileBuffer_WriteToAt(t *testing.T) {
	fb, err := newFileBuffer(100, t.TempDir())
	require.NoError(t, err)
	defer func() { assert.NoError(t, fb.close()) }()

	_, _ = fb.write([]byte("ABCDE"))

	var out bytes.Buffer
	n, werr := fb.writeToAt(2, &out)
	require.NoError(t, werr)
	assert.Equal(t, int64(3), n)
	assert.Equal(t, "CDE", out.String())

	// Negative offset
	_, werr = fb.writeToAt(-1, &out)
	assert.ErrorIs(t, werr, io.EOF)

	// Beyond size
	_, werr = fb.writeToAt(10, &out)
	assert.ErrorIs(t, werr, io.EOF)
}

func TestFileBuffer_Close_Nil(t *testing.T) {
	fb, err := newFileBuffer(100, t.TempDir())
	require.NoError(t, err)

	err = fb.close()
	require.NoError(t, err)

	// Second close (file is nil)
	err = fb.close()
	require.NoError(t, err)
}
