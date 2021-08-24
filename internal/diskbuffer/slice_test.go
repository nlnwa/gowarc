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
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSliceReadMemBuffer(t *testing.T) {
	bb := New(WithMaxMemBytes(100))
	defer bb.Close()

	bb.WriteString("line1\n")
	bb.WriteString("line2\n")
	bb.WriteString("line3")

	slice := bb.Slice(6, 100)
	p := make([]byte, 100)
	total, err := slice.Read(p)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, 11, total)
	assert.Equal(t, "line2\nline3", string(p[:total]))

	slice = bb.Slice(6, 0)
	total, err = slice.Read(p)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, 11, total)
	assert.Equal(t, "line2\nline3", string(p[:total]))

	slice = bb.Slice(6, 4)
	total, err = slice.Read(p)
	assert.Equal(t, nil, err)
	assert.Equal(t, 4, total)
	assert.Equal(t, "line", string(p[:total]))

	total, err = slice.Read(p)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, 0, total)
	assert.Equal(t, "", string(p[:total]))

	total, err = slice.Read(p)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, 0, total)
	assert.Equal(t, "", string(p[:total]))
}

func TestSliceReadDiskBuffer(t *testing.T) {
	bb := New(WithMaxMemBytes(7))
	defer bb.Close()

	bb.WriteString("line1\n")
	bb.WriteString("line2")

	slice := bb.Slice(6, 100)
	p := make([]byte, 100)
	total, err := slice.Read(p)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, 5, total)
	assert.Equal(t, "line2", string(p[:total]))

	slice = bb.Slice(6, 0)
	total, err = slice.Read(p)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, 5, total)
	assert.Equal(t, "line2", string(p[:total]))

	slice = bb.Slice(6, 4)
	total, err = slice.Read(p)
	assert.Equal(t, nil, err)
	assert.Equal(t, 4, total)
	assert.Equal(t, "line", string(p[:total]))
}

func TestSliceWriteToDiskBuffer(t *testing.T) {
	bb := New(WithMaxMemBytes(7))
	defer bb.Close()

	data := bytes.Buffer{}
	for i := 0; i < 4000; i++ {
		data.WriteString(fmt.Sprintf("line%04d\n", i))
	}
	bb.Write(data.Bytes())

	slice := bb.Slice(7, 100)
	w := bytes.Buffer{}
	total, err := slice.WriteTo(&w)
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(100), total)
	assert.Equal(t, data.String()[7:107], w.String())

	slice = bb.Slice(7, 0)
	w.Reset()
	total, err = slice.WriteTo(&w)
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(9*4000-7), total)
	assert.Equal(t, data.String()[7:], w.String())

	slice = bb.Slice(7, 50)
	w.Reset()
	total, err = slice.WriteTo(&w)
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(50), total)
	assert.Equal(t, data.String()[7:50+7], w.String())
}

func TestSliceReadString(t *testing.T) {
	bb := New(WithMaxMemBytes(100))
	defer bb.Close()

	bb.WriteString("line1\n")
	bb.WriteString("line2\n")
	bb.WriteString("line3")

	slice := bb.Slice(6, 0)
	line, err := slice.ReadString('\n')
	assert.NoError(t, err)
	assert.Equal(t, "line2\n", line)

	line, err = slice.ReadString('\n')
	assert.Error(t, err)
	assert.Equal(t, "line3", line)
}

func TestSliceReadBytes(t *testing.T) {
	bb := New(WithMaxMemBytes(100))
	defer bb.Close()

	bb.WriteString("line1\n")
	bb.WriteString("line2\n")
	bb.WriteString("line3")

	slice := bb.Slice(6, 0)
	line, err := slice.ReadBytes('\n')
	assert.NoError(t, err)
	assert.Equal(t, []byte("line2\n"), line)

	line, err = slice.ReadBytes('\n')
	assert.Error(t, err)
	assert.Equal(t, []byte("line3"), line)
}

func TestSlicePeek(t *testing.T) {
	bb := New(WithMaxMemBytes(10))
	defer bb.Close()

	bb.WriteString("line1\n")
	bb.WriteString("line2\n")
	bb.WriteString("line3")

	slice := bb.Slice(6, 0)
	peek, err := slice.Peek(5)
	assert.NoError(t, err)
	assert.Equal(t, []byte("line2"), peek)

	peek, err = slice.Peek(5)
	assert.NoError(t, err)
	assert.Equal(t, []byte("line2"), peek)

	line, err := slice.ReadBytes('\n')
	assert.NoError(t, err)
	assert.Equal(t, []byte("line2\n"), line)

	peek, err = slice.Peek(5)
	assert.NoError(t, err)
	assert.Equal(t, []byte("line3"), peek)

	line, err = slice.ReadBytes('\n')
	assert.Error(t, err)
	assert.Equal(t, []byte("line3"), line)
}

func TestSliceSeek(t *testing.T) {
	bb := New(WithMaxMemBytes(10))
	defer bb.Close()

	bb.WriteString("line1\n")
	bb.WriteString("line2\n")
	bb.WriteString("line3")

	slice := bb.Slice(6, 0)

	pos, err := slice.Seek(0, io.SeekStart)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), pos)
	peek, err := slice.Peek(5)
	assert.NoError(t, err)
	assert.Equal(t, []byte("line2"), peek)

	pos, err = slice.Seek(4, io.SeekStart)
	assert.NoError(t, err)
	assert.Equal(t, int64(4), pos)
	peek, err = slice.Peek(5)
	assert.NoError(t, err)
	assert.Equal(t, []byte("2\nlin"), peek)

	pos, err = slice.Seek(2, io.SeekCurrent)
	assert.NoError(t, err)
	assert.Equal(t, int64(6), pos)
	peek, err = slice.Peek(5)
	assert.NoError(t, err)
	assert.Equal(t, []byte("line3"), peek)

	pos, err = slice.Seek(4, io.SeekEnd)
	assert.NoError(t, err)
	assert.Equal(t, int64(7), pos)
	peek, err = slice.Peek(4)
	assert.NoError(t, err)
	assert.Equal(t, []byte("ine3"), peek)
}
