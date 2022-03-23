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

// A buffer which holds data in memory until a defined size and overflow extra data to a temporary file.

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
)

const (
	tmpFilePrefix = "tmp-diskbuffer-"
	unlimited     = math.MaxInt64
)

type Buffer interface {
	io.Reader
	io.Writer
	io.Closer
	io.ReaderFrom
	io.StringWriter
	io.WriterTo
	io.Seeker
	ReadBytes(delim byte) (line []byte, err error)
	ReadString(delim byte) (line string, err error)
	Peek(n int) (p []byte, err error)
	Size() int64
	Slice(offset, len int64) Slice
}

// A Buffer is a variable-sized buffer of bytes with Read and Write methods.
// The zero value for Buffer is an empty buffer ready to use.
type buffer struct {
	opts    options
	memBuf  *memBuffer
	fileBuf *fileBuffer
	off     int64 // read at &buf[off], write at &buf[len(buf)]
	max     int64
}

var ErrReadOnly = errors.New("diskbuffer.Buffer: mutating operation attempted on read only buffer")

// ErrTooLarge is passed to panic if memory cannot be allocated to store data in a buffer.
var ErrTooLarge = errors.New("diskbuffer.Buffer: too large")

// ErrMaxSizeExceeded is returned when the maximum allowed buffer size is reached when writing
type ErrMaxSizeExceeded int64

func (e ErrMaxSizeExceeded) Error() string {
	return fmt.Sprintf("diskbuffer.Buffer: maximum size %d exceeded", e)
}

var errNegativeRead = errors.New("diskbuffer.Buffer: reader returned negative count from Read")

// String returns the contents of the unread portion of the buffer
// as a string. If the Buffer is a nil pointer, it returns "<nil>".
//
// To build strings more efficiently, see the strings.Builder type.
func (b *buffer) String() string {
	if b == nil {
		// Special case, useful in debugging.
		return "<nil>"
	}
	return fmt.Sprintf("size: %d, cap: %d, off: %d (mem: %d/%d, disk: %d/%d)",
		b.Size(), b.Cap(), b.off, b.memBuf.size(), b.memBuf.cap(), b.fileBuf.size(), b.fileBuf.cap())
}

// empty reports whether the unread portion of the buffer is empty.
func (b *buffer) empty() bool { return b.Len() <= 0 }

// Len returns the number of bytes of the unread portion of the buffer;
// b.Len() == len(b.Bytes()).
func (b *buffer) Len() int64 {
	return b.Size() - b.off
}

func (b *buffer) Size() int64 {
	return b.memBuf.size() + b.fileBuf.size()
}

// Cap returns the capacity of the buffer's underlying byte slice, that is, the
// total space allocated for the buffer's data.
func (b *buffer) Cap() int64 { return b.max }

// Write appends the contents of p to the buffer, growing the buffer as
// needed. The return value n is the length of p; err is always nil. If the
// buffer becomes too large, Write will panic with ErrTooLarge.
func (b *buffer) Write(p []byte) (n int, err error) {
	if b.opts.readOnly {
		return 0, ErrReadOnly
	}
	return b.write(p)
}

func (b *buffer) write(p []byte) (int, error) {
	if b.opts.readOnly {
		return 0, ErrReadOnly
	}

	var wrote int
	var err error
	if b.memBuf.hasSpace() {
		n := b.memBuf.write(p)
		wrote = n
		if b.memBuf.hasSpace() {
			// All was written to memory buffer
			return wrote, nil
		}

		// we can't write to memory any more, switch to file
		if b.fileBuf, err = newFileBuffer(b.max-b.memBuf.cap(), b.opts.tmpDir); err != nil {
			return wrote, err
		}
		p = p[wrote:]
	}
	// There is more to write, add to file
	n, err := b.fileBuf.write(p)
	wrote += n
	return wrote, err
}

// WriteString appends the contents of s to the buffer, growing the buffer as
// needed. The return value n is the length of s; err is always nil. If the
// buffer becomes too large, WriteString will panic with ErrTooLarge.
func (b *buffer) WriteString(s string) (n int, err error) {
	if b.opts.readOnly {
		return 0, ErrReadOnly
	}
	return b.write([]byte(s))
}

// WriteByte appends the byte c to the buffer, growing the buffer as needed.
// The returned error is always nil, but is included to match bufio.Writer's
// WriteByte. If the buffer becomes too large, WriteByte will panic with
// ErrTooLarge.
func (b *buffer) WriteByte(c byte) error {
	if b.opts.readOnly {
		return ErrReadOnly
	}
	_, err := b.write([]byte{c})
	return err
}

// ReadFrom reads data from r until EOF and appends it to the buffer, growing
// the buffer as needed. The return value n is the number of bytes read. Any
// error except io.EOF encountered during the read is also returned. If the
// buffer becomes too large, ReadFrom will panic with ErrTooLarge.
func (b *buffer) ReadFrom(r io.Reader) (n int64, err error) {
	if b.opts.readOnly {
		return 0, ErrReadOnly
	}

	var wrote int64
	if b.memBuf.hasSpace() {
		wrote, err = b.memBuf.readFrom(r)
		if err == io.EOF {
			return wrote, nil
		}
		if err != nil {
			return wrote, err
		}
		if b.memBuf.hasSpace() {
			// All was written to memory buffer
			return wrote, nil
		}

		// we can't write to memory any more, switch to file
		var err error
		if b.fileBuf, err = newFileBuffer(b.max-b.memBuf.cap(), b.opts.tmpDir); err != nil {
			return wrote, err
		}
	}

	// There is more to write, add to file
	n, err = b.fileBuf.readFrom(r)
	wrote += n
	return wrote, err
}

// WriteTo writes data to w until the buffer is drained or an error occurs.
// The return value n is the number of bytes written. Any error
// encountered during the write is also returned.
func (b *buffer) WriteTo(w io.Writer) (n int64, err error) {
	m, err := w.Write(b.memBuf.bytes())
	n = int64(m)
	b.off += n
	if err != nil {
		return n, err
	}

	if b.fileBuf != nil {
		// Memory buffer exhausted, write to file
		m, err := b.fileBuf.writeTo(0, w)
		n += m
		b.off += m
		if err != nil {
			return n, err
		}
		// all bytes should have been written, by definition of
		// Write method in io.Writer
		//if n != b.bytesWritten-b.memBuf.size() {
		//	fmt.Printf("!!! s: %v, ml: %v, n:%v\n", b.bytesWritten, b.memBuf.size(), n)
		//
		//	return n, io.ErrShortWrite
		//}
	}
	return n, nil
}

// Read reads the next len(p) bytes from the buffer or until the buffer
// is drained. The return value n is the number of bytes read. If the
// buffer has no data to return, err is io.EOF (unless len(p) is zero);
// otherwise it is nil.
func (b *buffer) Read(p []byte) (n int, err error) {
	if b.empty() {
		if len(p) == 0 {
			return 0, nil
		}
		return 0, io.EOF
	}
	n, err = b.memBuf.read(b.off, p)
	b.off += int64(n)

	if err == io.EOF && len(p) > n && b.fileBuf != nil {
		// Memory buffer exhausted, read from file
		var m int
		m, err = b.fileBuf.read(b.off-b.memBuf.size(), p[n:])
		b.off += int64(m)
		n += m
	}
	return n, err
}

// Peek returns the next n bytes without advancing the reader.
// If Peek returns fewer than n bytes, it also returns an error explaining why the read is short.
// The error is ErrBufferFull if n is larger than b's buffer size.
//
// Calling Peek prevents a UnreadByte or UnreadRune call from succeeding until the next read operation.
func (b *buffer) Peek(n int) (p []byte, err error) {
	peekOffset := b.off
	p = make([]byte, n)

	if b.empty() {
		if n == 0 {
			return p, nil
		}
		return p, io.EOF
	}
	n, err = b.memBuf.read(peekOffset, p)
	peekOffset += int64(n)

	if err == io.EOF && len(p) > n && b.fileBuf != nil {
		// Memory buffer exhausted, read from file
		_, err = b.fileBuf.read(peekOffset-b.memBuf.size(), p[n:])
	}
	return p, err
}

// ReadAtOffset reads the next len(p) bytes from the buffer starting at off or until the buffer
// is drained. The return value n is the number of bytes read. If the
// buffer has no data to return, err is io.EOF (unless len(p) is zero);
// otherwise it is nil.
func (b *buffer) ReadAtOffset(off int64, p []byte) (n int, err error) {
	if b.Size()-off <= 0 {
		if len(p) == 0 {
			return 0, nil
		}
		return 0, io.EOF
	}
	n, err = b.memBuf.read(off, p)
	off += int64(n)

	if err == io.EOF && len(p) > n && b.fileBuf != nil {
		// Memory buffer exhausted, read from file
		var m int
		m, err = b.fileBuf.read(off-b.memBuf.size(), p[n:])
		n += m
	}
	return n, err
}

// ReadByte reads and returns the next byte from the buffer.
// If no byte is available, it returns error io.EOF.
func (b *buffer) ReadByte() (c byte, err error) {
	if b.empty() {
		return 0, io.EOF
	}
	if b.off < b.memBuf.len {
		c, err = b.memBuf.readByte(b.off)
		if err != nil {
			return 0, err
		}
	} else {
		c, err = b.fileBuf.readByte(b.off - b.memBuf.size())
		if err != nil {
			return 0, err
		}
	}

	b.off++
	return c, err
}

// ReadByte reads and returns the next byte from the buffer.
// If no byte is available, it returns error io.EOF.
func (b *buffer) ReadByteAt(off int64) (c byte, err error) {
	if b.empty() {
		return 0, io.EOF
	}
	if off < b.memBuf.len {
		c, err = b.memBuf.readByte(off)
		if err != nil {
			return 0, err
		}
	} else {
		c, err = b.fileBuf.readByte(off - b.memBuf.size())
		if err != nil {
			return 0, err
		}
	}
	return c, err
}

// ReadBytes reads until the first occurrence of delim in the input,
// returning a slice containing the data up to and including the delimiter.
// If ReadBytes encounters an error before finding a delimiter,
// it returns the data read before the error and the error itself (often io.EOF).
// ReadBytes returns err != nil if and only if the returned data does not end in
// delim.
func (b *buffer) ReadBytes(delim byte) (line []byte, err error) {
	if b.empty() {
		return []byte{}, io.EOF
	}
	found := false
	if b.off < b.memBuf.len {
		i := int64(bytes.IndexByte(b.memBuf.buf[b.off:], delim))
		end := b.off + i + 1
		if i < 0 {
			end = b.memBuf.len
			if b.memBuf.hasSpace() {
				err = io.EOF
			}
		} else {
			found = true
		}
		slice := b.memBuf.buf[b.off:end]
		// return a copy of slice. The buffer's backing array may
		// be overwritten by later calls.
		line = append(line, slice...)
		b.off = end
	}
	// if delim not found in memory, try disk
	if err == nil && !found {
		p := make([]byte, 100)
		off := b.off - b.memBuf.size()
		for {
			var n int
			n, err = b.fileBuf.read(off, p)
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
		b.off = b.memBuf.size() + off
	}
	return line, err
}

// ReadString reads until the first occurrence of delim in the input,
// returning a string containing the data up to and including the delimiter.
// If ReadString encounters an error before finding a delimiter,
// it returns the data read before the error and the error itself (often io.EOF).
// ReadString returns err != nil if and only if the returned data does not end
// in delim.
func (b *buffer) ReadString(delim byte) (line string, err error) {
	slice, err := b.ReadBytes(delim)
	return string(slice), err
}

func (b *buffer) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		b.off = offset
	case io.SeekCurrent:
		b.off += offset
	case io.SeekEnd:
		b.off = b.Size() - offset
	}
	return b.off, nil
}

func (b *buffer) Close() error {
	return b.fileBuf.close()
}

// New creates and initializes a new Buffer using sizeHint as the initialsize of the memory buffer
func New(opts ...Option) Buffer {
	b := &buffer{
		opts: defaultOptions(),
	}
	for _, opt := range opts {
		opt.apply(&b.opts)
	}

	if b.opts.maxTotalBytes > 0 && b.opts.maxMemBytes > b.opts.maxTotalBytes {
		b.opts.maxMemBytes = b.opts.maxTotalBytes
	}

	if b.opts.maxMemBytes > 0 {
		if b.opts.memBufferSizeHint > b.opts.maxMemBytes {
			b.opts.memBufferSizeHint = b.opts.maxMemBytes
		}
		b.memBuf = newMemBuffer(b.opts.maxMemBytes, b.opts.memBufferSizeHint)
	}

	if b.opts.maxTotalBytes == 0 {
		b.max = unlimited
		//} else if b.opts.maxTotalBytes >= b.opts.maxMemBytes {
		//	b.bytesLeftFile = b.opts.maxTotalBytes - b.opts.maxMemBytes
	} else {
		b.max = b.opts.maxTotalBytes
	}
	return b
}
