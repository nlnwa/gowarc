/*
 * Copyright 2020 National Library of Norway.
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

// nolint
package diskbuffer

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createReaderOfSize(size int64) (io.Reader, string) {
	b := make([]byte, int(size))
	for i := range b {
		b[i] = byte(i*31 + 7) // deterministic
	}
	h := md5.Sum(b)
	return bytes.NewReader(b), hex.EncodeToString(h[:])
}

func hashOfReader(r io.Reader) string {
	h := md5.New()
	_, _ = io.Copy(h, r)
	return hex.EncodeToString(h.Sum(nil))
}

func TestSmallBuffer(t *testing.T) {
	r, hash := createReaderOfSize(1)
	bb := New()
	defer bb.Close()
	_, err := io.Copy(bb, r)
	assert.NoError(t, err)
	assert.Equal(t, hash, hashOfReader(bb))
}

func TestBigBuffer(t *testing.T) {
	r, hash := createReaderOfSize(13631488)
	bb := New()
	defer bb.Close()
	_, err := io.Copy(bb, r)
	assert.NoError(t, err)
	assert.Equal(t, hash, hashOfReader(bb))
}

func TestSeek(t *testing.T) {
	tlen := int64(1057576)
	r, hash := createReaderOfSize(tlen)
	bb := New()
	defer bb.Close()
	_, err := io.Copy(bb, r)

	assert.NoError(t, err)
	assert.Equal(t, hash, hashOfReader(bb))
	l := bb.Size()
	assert.Equal(t, tlen, l)

	bb.Seek(0, io.SeekStart)
	assert.Equal(t, hash, hashOfReader(bb))
	l = bb.Size()
	assert.Equal(t, tlen, l)
}

func TestSeekWithFile(t *testing.T) {
	tlen := int64(1057576)
	r, hash := createReaderOfSize(tlen)
	bb := New(WithMaxMemBytes(1))
	defer bb.Close()
	_, err := io.Copy(bb, r)

	assert.NoError(t, err)
	assert.Equal(t, hash, hashOfReader(bb))
	l := bb.Size()
	assert.Equal(t, tlen, l)

	bb.Seek(0, io.SeekStart)
	assert.Equal(t, hash, hashOfReader(bb))
	l = bb.Size()
	assert.Equal(t, tlen, l)
}

func TestLimitDoesNotExceed(t *testing.T) {
	requestSize := int64(1057576)
	r, hash := createReaderOfSize(requestSize)
	bb := New(WithMaxMemBytes(1024), WithMaxTotalBytes(requestSize+1))
	defer bb.Close()
	_, err := io.Copy(bb, r)
	assert.NoError(t, err)
	assert.Equal(t, hash, hashOfReader(bb))
	size := bb.Size()
	assert.Equal(t, requestSize, size)
}

func TestLimitExceeds(t *testing.T) {
	requestSize := int64(1057576)
	r, _ := createReaderOfSize(requestSize)
	bb := New(WithMaxMemBytes(1024), WithMaxTotalBytes(requestSize-1))
	defer bb.Close()
	_, err := io.Copy(bb, r)
	assert.IsType(t, ErrMaxSizeExceeded(0), err)
}

func TestLimitExceedsMemBytes(t *testing.T) {
	requestSize := int64(1057576)
	r, _ := createReaderOfSize(requestSize)
	bb := New(WithMaxMemBytes(int(requestSize+1)), WithMaxTotalBytes(requestSize-1))
	defer bb.Close()
	_, err := io.Copy(bb, r)
	assert.IsType(t, ErrMaxSizeExceeded(0), err)
}

func TestWriteToBigBuffer(t *testing.T) {
	l := int64(13631488)
	r, hash := createReaderOfSize(l)
	bb := New()
	defer bb.Close()
	_, err := io.Copy(bb, r)
	assert.NoError(t, err)

	other := &bytes.Buffer{}
	wrote, err := bb.WriteTo(other)
	assert.NoError(t, err)
	assert.Equal(t, l, wrote)
	assert.Equal(t, hash, hashOfReader(other))
}

func TestWriteToSmallBuffer(t *testing.T) {
	l := int64(1)
	r, hash := createReaderOfSize(l)
	bb := New()
	defer bb.Close()
	_, err := io.Copy(bb, r)
	assert.NoError(t, err)

	other := &bytes.Buffer{}
	wrote, err := bb.WriteTo(other)
	assert.NoError(t, err)
	assert.Equal(t, l, wrote)
	assert.Equal(t, hash, hashOfReader(other))
}

func TestReadFromSmallBuffer(t *testing.T) {
	r, hash := createReaderOfSize(1)

	bb := New()
	defer bb.Close()

	total, err := bb.ReadFrom(r)
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(1), total)

	assert.Equal(t, hash, hashOfReader(bb))
}

func TestReadFromBigBuffer(t *testing.T) {
	size := int64(13631488)
	r, hash := createReaderOfSize(size)

	bb := New()
	defer bb.Close()

	total, err := bb.ReadFrom(r)
	assert.Equal(t, nil, err)
	assert.Equal(t, size, total)

	assert.Equal(t, hash, hashOfReader(bb))
}

func TestWriterOnceMaxSizeExceeded(t *testing.T) {
	size := int64(1000)
	r, _ := createReaderOfSize(size)

	bb := New(WithMaxMemBytes(10), WithMaxTotalBytes(100))
	defer bb.Close()

	_, err := io.Copy(bb, r)
	assert.Error(t, err)
}

func TestSeekInvalidWhence(t *testing.T) {
	bb := New()
	defer bb.Close()

	_, _ = bb.Write([]byte("abc"))
	_, err := bb.Seek(0, 12345)
	assert.Error(t, err)
}

func TestSeekBounds(t *testing.T) {
	bb := New()
	defer bb.Close()
	_, _ = bb.Write([]byte("abc"))

	_, err := bb.Seek(-1, io.SeekStart)
	assert.Error(t, err)

	// Standard io.Seeker: seeking past end is allowed
	pos, err := bb.Seek(4, io.SeekStart)
	assert.NoError(t, err)
	assert.Equal(t, int64(4), pos)

	_, err = bb.Seek(-1, io.SeekEnd)
	assert.NoError(t, err) // valid: position size-1
}

func TestTotalLimitExactThenExceedByOne(t *testing.T) {
	bb := New(WithMaxMemBytes(4), WithMaxTotalBytes(5))
	defer bb.Close()

	n, err := bb.Write([]byte("12345"))
	assert.Equal(t, 5, n)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), bb.Size())

	n, err = bb.Write([]byte("x"))
	assert.Equal(t, 0, n)
	assert.IsType(t, ErrMaxSizeExceeded(0), err)
	assert.Equal(t, int64(5), bb.Size())
}

func TestMemLimitBoundarySpillsToDisk(t *testing.T) {
	bb := New(WithMaxMemBytes(3), WithMaxTotalBytes(10))
	defer bb.Close()

	_, err := bb.Write([]byte("abc")) // fills mem
	assert.NoError(t, err)

	_, err = bb.Write([]byte("def")) // should go to file
	assert.NoError(t, err)

	bb.Seek(0, io.SeekStart)
	out, _ := io.ReadAll(bb)
	assert.Equal(t, "abcdef", string(out))
	assert.Equal(t, int64(6), bb.Size())
}

func TestReadAtAcrossMemDiskBoundary(t *testing.T) {
	bb := New(WithMaxMemBytes(4), WithMaxTotalBytes(20))
	defer bb.Close()

	_, err := bb.Write([]byte("0123456789")) // mem:0123 disk:456789
	assert.NoError(t, err)

	b, ok := bb.(*buffer)
	assert.True(t, ok)

	p := make([]byte, 6)
	n, err := b.ReadAt(p, 2) // "234567"
	assert.Equal(t, 6, n)
	assert.NoError(t, err)
	assert.Equal(t, "234567", string(p))
}

func TestReadByteAtBoundary(t *testing.T) {
	bb := New(WithMaxMemBytes(4))
	defer bb.Close()
	_, _ = bb.Write([]byte("012345"))

	b, ok := bb.(*buffer)
	assert.True(t, ok)

	c, err := b.ReadByteAt(3) // last in mem
	assert.NoError(t, err)
	assert.Equal(t, byte('3'), c)

	c, err = b.ReadByteAt(4) // first in disk
	assert.NoError(t, err)
	assert.Equal(t, byte('4'), c)

	_, err = b.ReadByteAt(6) // out of range
	assert.Equal(t, io.EOF, err)
}

func TestPeekDoesNotAdvanceAndEOF(t *testing.T) {
	bb := New()
	defer bb.Close()
	_, _ = bb.Write([]byte("abc"))

	p, err := bb.Peek(2)
	assert.NoError(t, err)
	assert.Equal(t, "ab", string(p))
	assert.Equal(t, int64(0), bb.(*buffer).pos) // if you can access; otherwise read and compare

	buf := make([]byte, 2)
	n, err := bb.Read(buf)
	assert.Equal(t, 2, n)
	assert.NoError(t, err)
	assert.Equal(t, "ab", string(buf))

	p, err = bb.Peek(5)
	assert.Error(t, err)
	assert.True(t, len(p) <= 5) // if you return short slices, this becomes exact
}

func TestReadFromExceedsTotalLimitReportsCount(t *testing.T) {
	bb := New(WithMaxMemBytes(4), WithMaxTotalBytes(6))
	defer bb.Close()

	r := bytes.NewReader([]byte("0123456789"))
	n, err := bb.ReadFrom(r)
	assert.Equal(t, int64(6), n)
	assert.IsType(t, ErrMaxSizeExceeded(0), err)
	assert.Equal(t, int64(6), bb.Size())
}

func TestReadOnlyRejectsWrites(t *testing.T) {
	bb := New(WithReadOnly(true))
	defer bb.Close()

	_, err := bb.Write([]byte("x"))
	assert.ErrorIs(t, err, ErrReadOnly)

	err = bb.WriteByte('x')
	assert.ErrorIs(t, err, ErrReadOnly)

	_, err = bb.WriteString("x")
	assert.ErrorIs(t, err, ErrReadOnly)

	_, err = bb.ReadFrom(bytes.NewReader([]byte("x")))
	assert.ErrorIs(t, err, ErrReadOnly)
}

func TestCloseIdempotent(t *testing.T) {
	bb := New(WithMaxMemBytes(1))
	_ = bb.Close()
	err := bb.Close()
	assert.NoError(t, err)
}

func TestUseAfterClose(t *testing.T) {
	bb := New()
	_, _ = bb.Write([]byte("hello"))
	require.NoError(t, bb.Close())

	// All operations should return ErrClosed
	_, err := bb.Write([]byte("x"))
	assert.ErrorIs(t, err, ErrClosed)

	_, err = bb.WriteString("x")
	assert.ErrorIs(t, err, ErrClosed)

	err = bb.WriteByte('x')
	assert.ErrorIs(t, err, ErrClosed)

	_, err = bb.ReadFrom(strings.NewReader("x"))
	assert.ErrorIs(t, err, ErrClosed)

	_, err = bb.Seek(0, io.SeekStart)
	assert.ErrorIs(t, err, ErrClosed)

	_, err = bb.Read(make([]byte, 1))
	assert.ErrorIs(t, err, ErrClosed)

	_, err = bb.ReadAt(make([]byte, 1), 0)
	assert.ErrorIs(t, err, ErrClosed)

	_, err = bb.ReadByte()
	assert.ErrorIs(t, err, ErrClosed)

	_, err = bb.ReadBytes('\n')
	assert.ErrorIs(t, err, ErrClosed)

	_, err = bb.Peek(1)
	assert.ErrorIs(t, err, ErrClosed)

	_, err = bb.WriteTo(io.Discard)
	assert.ErrorIs(t, err, ErrClosed)

	// Size/Len/Limit return 0
	assert.Equal(t, int64(0), bb.Size())
}

func TestPeekNegativeCount(t *testing.T) {
	bb := New()
	defer bb.Close()
	_, _ = bb.Write([]byte("abc"))

	_, err := bb.Peek(-1)
	assert.ErrorIs(t, err, errNegativeCount)
}

func TestSliceFiniteSize(t *testing.T) {
	bb := New()
	defer bb.Close()
	_, _ = bb.Write([]byte("0123456789"))

	s := io.NewSectionReader(bb, 3, 4) // "3456"
	out, err := io.ReadAll(s)
	assert.NoError(t, err)
	assert.Equal(t, "3456", string(out))
}

func TestSliceSeekEnd(t *testing.T) {
	bb := New()
	defer bb.Close()
	_, _ = bb.Write([]byte("0123456789"))

	s := io.NewSectionReader(bb, 3, 4) // size 4
	_, err := s.Seek(-1, io.SeekEnd)
	assert.NoError(t, err)

	b := make([]byte, 1)
	n, err := s.Read(b)
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.Equal(t, "6", string(b))
}

func TestBuffer_EmptyBuffer(t *testing.T) {
	bb := New()
	defer bb.Close()

	// Size should be 0
	if bb.Size() != 0 {
		t.Errorf("Empty buffer Size() = %d, want 0", bb.Size())
	}

	// Read should return EOF
	buf := make([]byte, 10)
	n, err := bb.Read(buf)
	if err != io.EOF {
		t.Errorf("Read() on empty buffer should return EOF, got: %v", err)
	}
	if n != 0 {
		t.Errorf("Read() on empty buffer returned %d bytes, want 0", n)
	}
}

func TestBuffer_SingleByte(t *testing.T) {
	bb := New()
	defer bb.Close()

	// Write single byte
	n, err := bb.Write([]byte{42})
	require.NoError(t, err)
	require.Equal(t, 1, n)

	// Seek to start
	_, err = bb.Seek(0, io.SeekStart)
	require.NoError(t, err)

	// Read single byte
	buf := make([]byte, 1)
	n, err = bb.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, byte(42), buf[0])
}

func TestBuffer_ZeroByteWrite(t *testing.T) {
	bb := New()
	defer bb.Close()

	// Write zero bytes
	n, err := bb.Write([]byte{})
	require.NoError(t, err)
	require.Equal(t, 0, n)

	// Size should still be 0
	if bb.Size() != 0 {
		t.Errorf("After zero-byte write, Size() = %d, want 0", bb.Size())
	}
}

func TestBuffer_ZeroByteRead(t *testing.T) {
	bb := New()
	defer bb.Close()

	_, err := bb.Write([]byte("test"))
	require.NoError(t, err)

	_, err = bb.Seek(0, io.SeekStart)
	require.NoError(t, err)

	// Read zero bytes
	buf := make([]byte, 0)
	n, err := bb.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 0, n)
}

func TestBuffer_SeekBeyondEnd(t *testing.T) {
	bb := New()
	defer bb.Close()

	data := []byte("test data")
	_, err := bb.Write(data)
	require.NoError(t, err)

	// Standard io.Seeker: seeking past end is allowed
	pos, err := bb.Seek(100, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(100), pos)

	// Read at past-end position returns 0, io.EOF
	buf := make([]byte, 10)
	n, readErr := bb.Read(buf)
	assert.Equal(t, 0, n)
	assert.ErrorIs(t, readErr, io.EOF)
}

func TestBuffer_SeekNegative(t *testing.T) {
	bb := New()
	defer bb.Close()

	_, err := bb.Write([]byte("test"))
	require.NoError(t, err)

	// Seek to negative position
	_, err = bb.Seek(-1, io.SeekStart)
	if err == nil {
		t.Error("Seek to negative position should return error")
	}
}

func TestBuffer_SeekCurrent(t *testing.T) {
	bb := New()
	defer bb.Close()

	data := []byte("0123456789")
	_, err := bb.Write(data)
	require.NoError(t, err)

	// Seek to start
	_, err = bb.Seek(0, io.SeekStart)
	require.NoError(t, err)

	// Read 5 bytes
	buf := make([]byte, 5)
	_, err = bb.Read(buf)
	require.NoError(t, err)

	// Seek forward 2 from current
	pos, err := bb.Seek(2, io.SeekCurrent)
	require.NoError(t, err)
	if pos != 7 {
		t.Errorf("Seek(2, Current) from position 5 = %d, want 7", pos)
	}

	// Read should get byte at position 7
	buf = make([]byte, 1)
	n, err := bb.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	if buf[0] != '7' {
		t.Errorf("Read after seek = %c, want '7'", buf[0])
	}
}

func TestBuffer_SeekEnd(t *testing.T) {
	bb := New()
	defer bb.Close()

	data := []byte("0123456789")
	_, err := bb.Write(data)
	require.NoError(t, err)

	// Seek to end
	pos, err := bb.Seek(0, io.SeekEnd)
	require.NoError(t, err)
	if pos != int64(len(data)) {
		t.Errorf("Seek(0, End) = %d, want %d", pos, len(data))
	}

	// Seek backward from end
	pos, err = bb.Seek(-5, io.SeekEnd)
	require.NoError(t, err)
	if pos != int64(len(data)-5) {
		t.Errorf("Seek(-5, End) = %d, want %d", pos, len(data)-5)
	}

	// Read should get the last 5 bytes
	buf := make([]byte, 5)
	n, err := bb.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	if string(buf) != "56789" {
		t.Errorf("Read after Seek(-5, End) = %q, want %q", string(buf), "56789")
	}
}

func TestBuffer_MultipleClose(t *testing.T) {
	bb := New()

	// First close
	err := bb.Close()
	require.NoError(t, err)

	// Second close should not panic
	err = bb.Close()
	if err != nil {
		t.Logf("Second Close() returned error (may be acceptable): %v", err)
	}
}

func TestBuffer_WriteAfterClose(t *testing.T) {
	bb := New()
	bb.Close()

	// Write after close - behavior is implementation-specific
	_, err := bb.Write([]byte("test"))
	t.Logf("Write() after Close() returned: %v", err)
}

func TestBuffer_ReadAfterClose(t *testing.T) {
	bb := New()
	_, _ = bb.Write([]byte("test"))
	bb.Close()

	// Read after close - behavior is implementation-specific
	buf := make([]byte, 10)
	_, err := bb.Read(buf)
	t.Logf("Read() after Close() returned: %v", err)
}

func TestBuffer_ConcurrentWrites(t *testing.T) {
	// This test may expose race conditions if run with -race flag
	bb := New()
	defer bb.Close()

	done := make(chan bool)
	errors := make(chan error, 2)

	// Two goroutines writing
	go func() {
		for i := 0; i < 100; i++ {
			_, err := bb.Write([]byte("a"))
			if err != nil {
				errors <- err
				done <- true
				return
			}
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			_, err := bb.Write([]byte("b"))
			if err != nil {
				errors <- err
				done <- true
				return
			}
		}
		done <- true
	}()

	// Wait for both
	<-done
	<-done
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Concurrent write error: %v", err)
	}
}

func TestBuffer_AlternatingWriteRead(t *testing.T) {
	bb := New()
	defer bb.Close()

	for i := 0; i < 10; i++ {
		// Write
		data := []byte{byte(i)}
		_, err := bb.Write(data)
		require.NoError(t, err)
	}

	// Seek to beginning
	_, err := bb.Seek(0, io.SeekStart)
	require.NoError(t, err)

	// Read back
	for i := 0; i < 10; i++ {
		buf := make([]byte, 1)
		n, err := bb.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 1, n)
		if buf[0] != byte(i) {
			t.Errorf("Read[%d] = %d, want %d", i, buf[0], i)
		}
	}
}

func TestBuffer_WriteTo_Empty(t *testing.T) {
	bb := New()
	defer bb.Close()

	var buf bytes.Buffer
	n, err := bb.WriteTo(&buf)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)
	require.Equal(t, 0, buf.Len())
}

func TestBuffer_WriteTo_AfterPartialRead(t *testing.T) {
	bb := New()
	defer bb.Close()

	data := []byte("0123456789")
	_, err := bb.Write(data)
	require.NoError(t, err)

	// Seek to start and read 5 bytes
	_, err = bb.Seek(0, io.SeekStart)
	require.NoError(t, err)

	buf := make([]byte, 5)
	_, err = bb.Read(buf)
	require.NoError(t, err)

	// WriteTo should write remaining 5 bytes
	var outBuf bytes.Buffer
	n, err := bb.WriteTo(&outBuf)
	require.NoError(t, err)
	require.Equal(t, int64(5), n)
	require.Equal(t, "56789", outBuf.String())
}

func TestBuffer_ReadFrom_Error(t *testing.T) {
	bb := New(WithMaxTotalBytes(10))
	defer bb.Close()

	// Try to read more than max
	r := strings.NewReader(strings.Repeat("a", 100))
	_, err := bb.ReadFrom(r)

	assert.Error(t, err)
	assert.IsType(t, ErrMaxSizeExceeded(0), err)
}

func TestBuffer_Size_AfterSeek(t *testing.T) {
	bb := New()
	defer bb.Close()

	data := []byte("test data")
	_, err := bb.Write(data)
	require.NoError(t, err)

	size1 := bb.Size()

	// Seek to middle
	_, err = bb.Seek(4, io.SeekStart)
	require.NoError(t, err)

	size2 := bb.Size()

	// Size should not change after seek
	if size1 != size2 {
		t.Errorf("Size changed after Seek: before=%d, after=%d", size1, size2)
	}
}

func TestBuffer_TransitionFromMemToFile(t *testing.T) {
	// Force transition by using small memory limit
	bb := New(WithMaxMemBytes(10))
	defer bb.Close()

	// Write small amount (should stay in memory)
	small := []byte("small")
	_, err := bb.Write(small)
	require.NoError(t, err)

	// Write large amount (should transition to file)
	large := bytes.Repeat([]byte("a"), 100)
	_, err = bb.Write(large)
	require.NoError(t, err)

	// Verify all data is readable
	_, err = bb.Seek(0, io.SeekStart)
	require.NoError(t, err)

	result, err := io.ReadAll(bb)
	require.NoError(t, err)

	expected := append(small, large...)
	if !bytes.Equal(result, expected) {
		t.Error("Data corrupted during memory-to-file transition")
	}
}

func TestBuffer_BinaryData(t *testing.T) {
	bb := New()
	defer bb.Close()

	// Binary data with all byte values
	data := make([]byte, 256)
	for i := 0; i < 256; i++ {
		data[i] = byte(i)
	}

	_, err := bb.Write(data)
	require.NoError(t, err)

	_, err = bb.Seek(0, io.SeekStart)
	require.NoError(t, err)

	result, err := io.ReadAll(bb)
	require.NoError(t, err)

	if !bytes.Equal(result, data) {
		t.Error("Binary data corrupted")
	}
}

func TestBuffer_SeekBeyondSize(t *testing.T) {
	bb := New()
	defer bb.Close()

	_, err := bb.Write([]byte("test"))
	require.NoError(t, err)

	// Try to seek way beyond
	_, err = bb.Seek(1000, io.SeekStart)

	// Behavior may vary - just ensure it doesn't crash
	t.Logf("Seek beyond size returned error: %v", err)
}

func TestBuffer_MaxMemBytes_Boundary(t *testing.T) {
	// Test exactly at boundary
	maxMem := 100
	bb := New(WithMaxMemBytes(maxMem))
	defer bb.Close()

	// Write exactly maxMemBytes
	data := bytes.Repeat([]byte("a"), maxMem)
	_, err := bb.Write(data)
	require.NoError(t, err)

	// Write one more byte (should trigger file)
	_, err = bb.Write([]byte("b"))
	require.NoError(t, err)

	// Verify data
	_, err = bb.Seek(0, io.SeekStart)
	require.NoError(t, err)

	result, err := io.ReadAll(bb)
	require.NoError(t, err)
	require.Equal(t, maxMem+1, len(result))
}

func TestBuffer_WriteString(t *testing.T) {
	bb := New()
	defer bb.Close()

	text := "Hello, World!"
	n, err := bb.WriteString(text)
	require.NoError(t, err)
	require.Equal(t, len(text), n)

	_, err = bb.Seek(0, io.SeekStart)
	require.NoError(t, err)

	result, err := io.ReadAll(bb)
	require.NoError(t, err)
	require.Equal(t, text, string(result))
}

func TestBuffer_WriteString_AfterMaxSize(t *testing.T) {
	bb := New(WithMaxTotalBytes(5))
	defer bb.Close()

	_, err := bb.WriteString("Hello, World!")
	assert.Error(t, err)
	assert.IsType(t, ErrMaxSizeExceeded(0), err)
}

// ---- Tests for previously uncovered code paths ----

func TestErrMaxSizeExceeded_Error(t *testing.T) {
	e := ErrMaxSizeExceeded(1024)
	assert.Equal(t, "diskbuffer: maximum size 1024 exceeded", e.Error())
}

func TestBuffer_Limit(t *testing.T) {
	bb := New(WithMaxTotalBytes(42))
	defer bb.Close()
	assert.Equal(t, int64(42), bb.(*buffer).Limit())

	// Unlimited
	bb2 := New()
	defer bb2.Close()
	assert.Equal(t, int64(unlimited), bb2.(*buffer).Limit())
}

func TestBuffer_Len(t *testing.T) {
	bb := New()
	defer bb.Close()

	_, _ = bb.Write([]byte("0123456789"))
	assert.Equal(t, int64(10), bb.(*buffer).Len())

	// After seeking forward, Len decreases
	_, _ = bb.Seek(3, io.SeekStart)
	assert.Equal(t, int64(7), bb.(*buffer).Len())

	// After seeking to end, Len is 0
	_, _ = bb.Seek(0, io.SeekEnd)
	assert.Equal(t, int64(0), bb.(*buffer).Len())
}

func TestBuffer_WriteByte(t *testing.T) {
	bb := New()
	defer bb.Close()

	for i := 0; i < 10; i++ {
		err := bb.WriteByte(byte('a' + i))
		require.NoError(t, err)
	}
	assert.Equal(t, int64(10), bb.Size())

	_, _ = bb.Seek(0, io.SeekStart)
	data, err := io.ReadAll(bb)
	require.NoError(t, err)
	assert.Equal(t, "abcdefghij", string(data))
}

func TestBuffer_WriteByte_ReadOnly(t *testing.T) {
	bb := New(WithReadOnly(true))
	defer bb.Close()
	err := bb.WriteByte('x')
	assert.ErrorIs(t, err, ErrReadOnly)
}

func TestBuffer_WriteByte_MaxSize(t *testing.T) {
	// maxTotalBytes=0 means unlimited in New(), so use 1 to set a real limit
	bb := New(WithMaxMemBytes(1), WithMaxTotalBytes(1))
	defer bb.Close()
	// First byte goes into memory
	err := bb.WriteByte('a')
	require.NoError(t, err)
	// Second should exceed total limit
	err = bb.WriteByte('b')
	assert.IsType(t, ErrMaxSizeExceeded(0), err)
}

func TestBuffer_ReadByte_Sequential(t *testing.T) {
	bb := New()
	defer bb.Close()

	_, _ = bb.Write([]byte("ABCDE"))
	_, _ = bb.Seek(0, io.SeekStart)

	for i, expected := range []byte("ABCDE") {
		c, err := bb.ReadByte()
		require.NoError(t, err, "ReadByte at position %d", i)
		assert.Equal(t, expected, c)
	}

	// Next read should return EOF
	_, err := bb.ReadByte()
	assert.ErrorIs(t, err, io.EOF)
}

func TestBuffer_ReadByte_AcrossBoundary(t *testing.T) {
	// Data spans memory and disk
	bb := New(WithMaxMemBytes(3))
	defer bb.Close()

	_, _ = bb.Write([]byte("ABCDE"))
	_, _ = bb.Seek(0, io.SeekStart)

	for i, expected := range []byte("ABCDE") {
		c, err := bb.ReadByte()
		require.NoError(t, err, "ReadByte at position %d", i)
		assert.Equal(t, expected, c)
	}
}

func TestBuffer_ReadByteAt_Memory(t *testing.T) {
	bb := New()
	defer bb.Close()
	_, _ = bb.Write([]byte("Hello"))

	c, err := bb.(*buffer).ReadByteAt(0)
	require.NoError(t, err)
	assert.Equal(t, byte('H'), c)

	c, err = bb.(*buffer).ReadByteAt(4)
	require.NoError(t, err)
	assert.Equal(t, byte('o'), c)
}

func TestBuffer_ReadByteAt_File(t *testing.T) {
	bb := New(WithMaxMemBytes(3))
	defer bb.Close()
	_, _ = bb.Write([]byte("ABCDE"))

	// Byte at offset 4 is on disk
	c, err := bb.(*buffer).ReadByteAt(4)
	require.NoError(t, err)
	assert.Equal(t, byte('E'), c)
}

func TestBuffer_ReadByteAt_OutOfRange(t *testing.T) {
	bb := New()
	defer bb.Close()
	_, _ = bb.Write([]byte("Hi"))

	_, err := bb.(*buffer).ReadByteAt(10)
	assert.ErrorIs(t, err, io.EOF)

	_, err = bb.(*buffer).ReadByteAt(-1)
	assert.ErrorIs(t, err, errInvalidOffset)
}

func TestBuffer_ReadBytes_InMemory(t *testing.T) {
	bb := New()
	defer bb.Close()
	_, _ = bb.Write([]byte("line1\nline2\nline3"))
	_, _ = bb.Seek(0, io.SeekStart)

	line, err := bb.(*buffer).ReadBytes('\n')
	require.NoError(t, err)
	assert.Equal(t, "line1\n", string(line))

	line, err = bb.(*buffer).ReadBytes('\n')
	require.NoError(t, err)
	assert.Equal(t, "line2\n", string(line))

	// Third line has no trailing delimiter
	line, err = bb.(*buffer).ReadBytes('\n')
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, "line3", string(line))
}

func TestBuffer_ReadBytes_AcrossBoundary(t *testing.T) {
	// Memory holds 5 bytes, rest spills to disk
	bb := New(WithMaxMemBytes(5))
	defer bb.Close()

	_, _ = bb.Write([]byte("abc\ndefgh\nij"))
	_, _ = bb.Seek(0, io.SeekStart)

	// First line: "abc\n" — all in memory
	line, err := bb.(*buffer).ReadBytes('\n')
	require.NoError(t, err)
	assert.Equal(t, "abc\n", string(line))

	// Second line: "defgh\n" — starts in memory, delimiter is on disk
	line, err = bb.(*buffer).ReadBytes('\n')
	require.NoError(t, err)
	assert.Equal(t, "defgh\n", string(line))

	// Third: "ij" — all on disk, no delimiter
	line, err = bb.(*buffer).ReadBytes('\n')
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, "ij", string(line))
}

func TestBuffer_ReadBytes_Empty(t *testing.T) {
	bb := New()
	defer bb.Close()

	_, _ = bb.Seek(0, io.SeekStart)
	_, err := bb.(*buffer).ReadBytes('\n')
	assert.ErrorIs(t, err, io.EOF)
}

func TestBuffer_ReadBytes_OnlyOnDisk(t *testing.T) {
	// maxMem=0 forces immediate file spill
	bb := New(WithMaxMemBytes(0))
	defer bb.Close()

	_, _ = bb.Write([]byte("aa\nbb"))
	_, _ = bb.Seek(0, io.SeekStart)

	line, err := bb.(*buffer).ReadBytes('\n')
	require.NoError(t, err)
	assert.Equal(t, "aa\n", string(line))

	line, err = bb.(*buffer).ReadBytes('\n')
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, "bb", string(line))
}

func TestBuffer_Peek_Negative(t *testing.T) {
	bb := New()
	defer bb.Close()
	_, _ = bb.Write([]byte("data"))
	_, _ = bb.Seek(0, io.SeekStart)

	_, err := bb.(*buffer).Peek(-1)
	assert.Error(t, err)
}

func TestBuffer_Peek_Zero(t *testing.T) {
	bb := New()
	defer bb.Close()
	_, _ = bb.Write([]byte("data"))
	_, _ = bb.Seek(0, io.SeekStart)

	p, err := bb.(*buffer).Peek(0)
	require.NoError(t, err)
	assert.Empty(t, p)
}

func TestBuffer_Peek_ExceedsLen(t *testing.T) {
	bb := New()
	defer bb.Close()
	_, _ = bb.Write([]byte("hi"))
	_, _ = bb.Seek(0, io.SeekStart)

	p, err := bb.(*buffer).Peek(100)
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, "hi", string(p))
}

func TestBuffer_Peek_AtEnd(t *testing.T) {
	bb := New()
	defer bb.Close()
	_, _ = bb.Write([]byte("data"))
	_, _ = bb.Seek(0, io.SeekEnd)

	p, err := bb.(*buffer).Peek(1)
	assert.ErrorIs(t, err, io.EOF)
	assert.Empty(t, p)
}

func TestBuffer_Peek_AcrossBoundary(t *testing.T) {
	bb := New(WithMaxMemBytes(3))
	defer bb.Close()
	_, _ = bb.Write([]byte("ABCDE"))
	_, _ = bb.Seek(1, io.SeekStart)

	// Peek 4 bytes: B(mem) C(mem) D(file) E(file)
	p, err := bb.(*buffer).Peek(4)
	require.NoError(t, err)
	assert.Equal(t, "BCDE", string(p))

	// Position should not have changed
	assert.Equal(t, int64(1), bb.(*buffer).pos)
}

func TestBuffer_Seek_InvalidWhence(t *testing.T) {
	bb := New()
	defer bb.Close()

	_, err := bb.Seek(0, 99)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid whence")
}

func TestBuffer_ReadAt_NegativeOffset(t *testing.T) {
	bb := New()
	defer bb.Close()
	_, _ = bb.Write([]byte("test"))

	buf := make([]byte, 4)
	_, err := bb.ReadAt(buf, -1)
	assert.Error(t, err)
}

func TestBuffer_ReadAt_EmptyBuffer(t *testing.T) {
	bb := New()
	defer bb.Close()

	// Empty read should succeed
	buf := make([]byte, 0)
	n, err := bb.ReadAt(buf, 0)
	assert.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestBuffer_WriteString_SpillsToDisk(t *testing.T) {
	bb := New(WithMaxMemBytes(5))
	defer bb.Close()

	n, err := bb.(*buffer).WriteString("Hello, World!")
	require.NoError(t, err)
	assert.Equal(t, 13, n)

	_, _ = bb.Seek(0, io.SeekStart)
	data, err := io.ReadAll(bb)
	require.NoError(t, err)
	assert.Equal(t, "Hello, World!", string(data))
}

func TestBuffer_WriteString_ReadOnly(t *testing.T) {
	bb := New(WithReadOnly(true))
	defer bb.Close()

	_, err := bb.WriteString("test")
	assert.ErrorIs(t, err, ErrReadOnly)
}

func TestBuffer_WriteString_Empty(t *testing.T) {
	bb := New()
	defer bb.Close()

	n, err := bb.WriteString("")
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestBuffer_ReadFrom_ReadError(t *testing.T) {
	bb := New()
	defer bb.Close()

	r := &errReader{err: io.ErrUnexpectedEOF}
	_, err := bb.ReadFrom(r)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

type errReader struct{ err error }

func (r *errReader) Read(p []byte) (int, error) {
	return 0, r.err
}

func TestBuffer_ReadFrom_ReadOnly(t *testing.T) {
	bb := New(WithReadOnly(true))
	defer bb.Close()

	_, err := bb.ReadFrom(strings.NewReader("data"))
	assert.ErrorIs(t, err, ErrReadOnly)
}

func TestBuffer_WriteTo_WithFileBuffer(t *testing.T) {
	bb := New(WithMaxMemBytes(3))
	defer bb.Close()

	_, _ = bb.Write([]byte("ABCDE"))
	_, _ = bb.Seek(0, io.SeekStart)

	var out bytes.Buffer
	n, err := bb.WriteTo(&out)
	require.NoError(t, err)
	assert.Equal(t, int64(5), n)
	assert.Equal(t, "ABCDE", out.String())
}

func TestBuffer_WriteTo_FromMiddle(t *testing.T) {
	// Start reading from a position that's inside the file portion
	bb := New(WithMaxMemBytes(3))
	defer bb.Close()

	_, _ = bb.Write([]byte("ABCDE"))
	_, _ = bb.Seek(4, io.SeekStart) // position is in file portion

	var out bytes.Buffer
	n, err := bb.WriteTo(&out)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
	assert.Equal(t, "E", out.String())
}

func TestBuffer_Write_ReadOnly(t *testing.T) {
	bb := New(WithReadOnly(true))
	defer bb.Close()

	_, err := bb.Write([]byte("test"))
	assert.ErrorIs(t, err, ErrReadOnly)
}

func TestBuffer_Options_WithTmpDir(t *testing.T) {
	tmpDir := t.TempDir()
	bb := New(WithMaxMemBytes(0), WithTmpDir(tmpDir))
	defer bb.Close()

	_, err := bb.Write([]byte("data"))
	require.NoError(t, err)

	_, _ = bb.Seek(0, io.SeekStart)
	data, err := io.ReadAll(bb)
	require.NoError(t, err)
	assert.Equal(t, "data", string(data))
}

func TestBuffer_Options_WithMemBufferSizeHint(t *testing.T) {
	bb := New(WithMemBufferSizeHint(64))
	defer bb.Close()

	_, err := bb.Write([]byte("test"))
	require.NoError(t, err)
	assert.Equal(t, int64(4), bb.Size())
}

func TestBuffer_Options_MemExceedsTotal(t *testing.T) {
	// maxMemBytes larger than maxTotalBytes should be clamped
	bb := New(WithMaxMemBytes(1000), WithMaxTotalBytes(10))
	defer bb.Close()

	// Write exactly 10 bytes (total limit)
	data := []byte("0123456789")
	_, err := bb.Write(data)
	require.NoError(t, err)

	// Writing one more should fail
	_, err = bb.Write([]byte("x"))
	assert.IsType(t, ErrMaxSizeExceeded(0), err)
}

func TestBuffer_Write_SpillCreatesFileBuffer(t *testing.T) {
	bb := New(WithMaxMemBytes(5), WithMaxTotalBytes(20))
	defer bb.Close()

	// Write 5 bytes to fill memory
	_, err := bb.Write([]byte("12345"))
	require.NoError(t, err)
	assert.Nil(t, bb.(*buffer).fileBuf)

	// Write 1 more byte to trigger file creation
	_, err = bb.Write([]byte("6"))
	require.NoError(t, err)
	assert.NotNil(t, bb.(*buffer).fileBuf)

	// Read back all data
	_, _ = bb.Seek(0, io.SeekStart)
	data, err := io.ReadAll(bb)
	require.NoError(t, err)
	assert.Equal(t, "123456", string(data))
}

func TestBuffer_Write_NoSpaceLeft(t *testing.T) {
	// maxMem=3, maxTotal=3 → no file buffer allowed
	bb := New(WithMaxMemBytes(3), WithMaxTotalBytes(3))
	defer bb.Close()

	_, err := bb.Write([]byte("abc"))
	require.NoError(t, err)

	n, err := bb.Write([]byte("d"))
	assert.IsType(t, ErrMaxSizeExceeded(0), err)
	assert.Equal(t, 0, n)
}

func TestBuffer_WriteString_NoSpaceLeft(t *testing.T) {
	bb := New(WithMaxMemBytes(3), WithMaxTotalBytes(3))
	defer bb.Close()

	_, err := bb.WriteString("abc")
	require.NoError(t, err)

	_, err = bb.WriteString("d")
	assert.IsType(t, ErrMaxSizeExceeded(0), err)
}

func TestBuffer_ReadFrom_SpillsToDisk(t *testing.T) {
	bb := New(WithMaxMemBytes(5))
	defer bb.Close()

	r := strings.NewReader("hello world!!")
	n, err := bb.ReadFrom(r)
	require.NoError(t, err)
	assert.Equal(t, int64(13), n)

	_, _ = bb.Seek(0, io.SeekStart)
	data, err := io.ReadAll(bb)
	require.NoError(t, err)
	assert.Equal(t, "hello world!!", string(data))
}

func TestBuffer_ReadAt_AcrossBoundary(t *testing.T) {
	bb := New(WithMaxMemBytes(4))
	defer bb.Close()

	_, _ = bb.Write([]byte("ABCDEFGH"))

	buf := make([]byte, 6)
	n, err := bb.ReadAt(buf, 1) // B C D(mem) E F G(file)
	require.NoError(t, err)
	assert.Equal(t, 6, n)
	assert.Equal(t, "BCDEFG", string(buf))
}

func TestBuffer_Read_AdvancesPosition(t *testing.T) {
	bb := New()
	defer bb.Close()

	_, _ = bb.Write([]byte("ABCDE"))
	_, _ = bb.Seek(0, io.SeekStart)

	buf := make([]byte, 2)
	n, err := bb.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, "AB", string(buf))

	n, err = bb.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, "CD", string(buf))

	buf = make([]byte, 5)
	n, err = bb.Read(buf)
	assert.Equal(t, 1, n)
	assert.Equal(t, byte('E'), buf[0])
}

func TestBuffer_WriteTo_WriterError(t *testing.T) {
	bb := New()
	defer bb.Close()
	_, _ = bb.Write([]byte("test data"))
	_, _ = bb.Seek(0, io.SeekStart)

	w := &failWriter{}
	_, err := bb.WriteTo(w)
	assert.Error(t, err)
}

type failWriter struct{}

func (w *failWriter) Write(p []byte) (int, error) {
	return 0, io.ErrClosedPipe
}

func TestBuffer_WriteTo_FileWriterError(t *testing.T) {
	bb := New(WithMaxMemBytes(3))
	defer bb.Close()
	_, _ = bb.Write([]byte("ABCDE"))
	_, _ = bb.Seek(0, io.SeekStart)

	// Read past memory portion first
	buf := make([]byte, 3)
	_, _ = bb.Read(buf)

	// Now WriteTo writes from file, which should fail
	w := &failWriter{}
	_, err := bb.WriteTo(w)
	assert.Error(t, err)
}
