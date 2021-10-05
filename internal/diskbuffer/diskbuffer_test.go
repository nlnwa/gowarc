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

//nolint
package diskbuffer

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func createReaderOfSize(size int64) (reader io.Reader, hash string) {
	f, err := os.Open("/dev/urandom")
	if err != nil {
		panic(err)
	}

	b := make([]byte, int(size))

	_, err = io.ReadFull(f, b)

	if err != nil {
		panic(err)
	}

	h := md5.New()
	h.Write(b)
	return bytes.NewReader(b), hex.EncodeToString(h.Sum(nil))
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
	bb := New(WithMaxMemBytes(requestSize+1), WithMaxTotalBytes(requestSize-1))
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

func TestReadStringMemory(t *testing.T) {
	bb := New(WithMaxMemBytes(100))
	defer bb.Close()

	bb.WriteString("line1\n")
	bb.WriteString("line2")

	line, err := bb.ReadString('\n')
	assert.NoError(t, err)
	assert.Equal(t, "line1\n", line)

	line, err = bb.ReadString('\n')
	assert.Error(t, err)
	assert.Equal(t, "line2", line)
}

func TestReadString(t *testing.T) {
	bb := New(WithMaxMemBytes(10))
	defer bb.Close()

	bb.WriteString("line1\n")
	bb.WriteString("line2\n")
	bb.WriteString("line3\n")
	bb.WriteString("line4")

	line, err := bb.ReadString('\n')
	assert.NoError(t, err)
	assert.Equal(t, "line1\n", line)

	line, err = bb.ReadString('\n')
	assert.NoError(t, err)
	assert.Equal(t, "line2\n", line)

	line, err = bb.ReadString('\n')
	assert.NoError(t, err)
	assert.Equal(t, "line3\n", line)

	line, err = bb.ReadString('\n')
	assert.Error(t, err)
	assert.Equal(t, "line4", line)
}
