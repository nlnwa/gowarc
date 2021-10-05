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

package diskbuffer

import (
	"bytes"
	"io"
)

type Slice interface {
	io.Reader
	io.WriterTo
	io.Closer
	io.Seeker
	ReadBytes(delim byte) (line []byte, err error)
	ReadString(delim byte) (line string, err error)
	Peek(n int) (p []byte, err error)
	Size() int64
}

// Slice returns a read only subset of a buffer.
func (b *buffer) Slice(offset, len int64) Slice {
	if len <= 0 {
		len = unlimited
	}
	return &slice{
		buf: b,
		off: offset,
		len: len,
	}
}

type slice struct {
	buf *buffer
	len int64 // Lenght of slice
	off int64 // Offset in buffer where this slice starts
	pos int64 // Current position in slice
}

func (s *slice) ReadAtOffset(off int64, p []byte) (n int, err error) {
	start := s.off + off
	l := s.len - s.pos
	if l <= 0 {
		return 0, io.EOF
	}

	pp := p
	if int64(len(p)) > l {
		pp = p[:l]
	}
	n, err = s.buf.ReadAtOffset(start, pp)
	return n, err
}

func (s *slice) Read(p []byte) (n int, err error) {
	n, err = s.ReadAtOffset(s.pos, p)
	s.pos += int64(n)
	return
}

func (s *slice) WriteTo(w io.Writer) (n int64, err error) {
	p := make([]byte, 32*1024)
	for {
		l1, e1 := s.Read(p)
		l2, e2 := w.Write(p[:l1])
		n += int64(l2)
		if e1 != nil {
			if e1 != io.EOF {
				err = e1
				return
			}
		}
		if e2 != nil {
			err = e2
			return
		}
		if l2 == 0 {
			break
		}
	}
	return
}

// Close closes the underlying buffer
func (s *slice) Close() error {
	return s.buf.Close()
}

func (s *slice) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		s.pos = offset
	case io.SeekCurrent:
		s.pos += offset
	case io.SeekEnd:
		s.pos = s.Size() - offset
	}
	return s.pos, nil
}

func (s *slice) ReadBytes(delim byte) (line []byte, err error) {
	if s.len-s.pos <= 0 {
		return []byte{}, io.EOF
	}

	p := make([]byte, 100)
	off := s.pos
	for {
		var n int
		n, err = s.ReadAtOffset(off, p)
		if n > 0 {
			i := bytes.IndexByte(p[:n], delim)
			end := i + 1
			if i < 0 {
				line = append(line, p[:n]...)
				off += int64(n)
			} else {
				// found delim
				line = append(line, p[:end]...)
				off += int64(end)
				err = nil
				break
			}
		}
		if err != nil {
			break
		}
	}
	s.pos = off
	return line, err
}

func (s *slice) ReadString(delim byte) (line string, err error) {
	var bytes []byte
	bytes, err = s.ReadBytes(delim)
	return string(bytes), err
}

func (s *slice) Peek(n int) (p []byte, err error) {
	p = make([]byte, n)
	n, err = s.ReadAtOffset(s.pos, p)
	return p[:n], err
}

// Len returns the number of bytes of the unread portion of the slice;
func (s *slice) Len() int64 {
	return s.Size() - s.pos
}

func (s *slice) Size() int64 {
	if s.len == unlimited {
		return s.buf.Size() - s.off
	} else {
		return s.len - s.off
	}
}
