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
	"errors"
	"fmt"
	"io"
	"math"
)

const (
	tmpFilePrefix = "tmp-diskbuffer-"
	unlimited     = math.MaxInt64
	readChunkSize = 32 * 1024
)

var (
	ErrReadOnly      = errors.New("diskbuffer: mutating operation attempted on read only buffer")
	ErrClosed        = errors.New("diskbuffer: use after close")
	errInvalidOffset = errors.New("diskbuffer: invalid offset")
	errWhence        = errors.New("diskbuffer: invalid whence")
	errNegativeCount = errors.New("diskbuffer: negative count")
)

// ErrMaxSizeExceeded is returned when the maximum allowed buffer size is reached when writing
type ErrMaxSizeExceeded int64

func (e ErrMaxSizeExceeded) Error() string {
	return fmt.Sprintf("diskbuffer: maximum size %d exceeded", e)
}

type Buffer interface {
	io.Reader
	io.ReaderAt
	io.Writer
	io.ByteReader
	io.ByteWriter
	io.Closer
	io.ReaderFrom
	io.StringWriter
	io.WriterTo
	io.Seeker
	ReadBytes(delim byte) (line []byte, err error)
	Peek(n int) (p []byte, err error)
	Size() int64
}

// buffer implements the Buffer interface as a two-tier store: data is first
// written to an in-memory buffer, then spills to a temporary file when the
// memory limit is reached.
type buffer struct {
	tmpDir string

	pos   int64
	limit int64

	memBuf  *memBuffer
	fileBuf *fileBuffer

	readOnly bool
	closed   bool
}

// New creates and initializes a new Buffer using sizeHint as the initial size of the memory buffer
func New(opts ...Option) Buffer {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	b := &buffer{
		readOnly: o.readOnly,
		tmpDir:   o.tmpDir,
	}

	// Normalize total limit
	total := max(o.maxTotalBytes, 0)
	if total == 0 {
		total = unlimited
	}

	b.limit = total

	// Normalize memory limit
	memMax := int64(max(o.maxMemBytes, 0))
	if total != unlimited && memMax > total {
		memMax = total
	}

	// Normalize hint
	hint := max(o.memBufferSizeHint, 0)

	// Always create; memMax==0 just means it immediately spills to disk.
	b.memBuf = newMemBuffer(memMax, hint)

	return b
}

func (b *buffer) Close() error {
	if b.closed {
		return nil
	}
	b.closed = true
	b.memBuf = nil
	if b.fileBuf == nil {
		return nil
	}
	err := b.fileBuf.close()
	b.fileBuf = nil
	return err
}

func (b *buffer) ensureOpen() error {
	if b.closed {
		return ErrClosed
	}
	return nil
}

// Len returns the number of bytes of the unread portion of the buffer.
// b.Len() == len(b.Bytes()).
func (b *buffer) Len() int64 {
	n := b.Size() - b.pos
	if n < 0 {
		return 0
	}
	return n
}

func (b *buffer) Size() int64 {
	if b.closed {
		return 0
	}
	sz := b.memBuf.Size()
	if b.fileBuf != nil {
		sz += b.fileBuf.Size()
	}
	return sz
}

// Limit returns the maximum number of bytes the buffer can hold.
func (b *buffer) Limit() int64 {
	if b.closed {
		return 0
	}
	return b.limit
}

// Write appends p to the buffer.
// It may return ErrReadOnly if the buffer is read-only, or ErrMaxSizeExceeded if the configured total limit is reached.
// It may write fewer than len(p) bytes when returning ErrMaxSizeExceeded.
func (b *buffer) Write(p []byte) (n int, err error) {
	return b.write(p)
}

func (b *buffer) write(p []byte) (int, error) {
	if err := b.ensureOpen(); err != nil {
		return 0, err
	}
	if b.readOnly {
		return 0, ErrReadOnly
	}
	if len(p) == 0 {
		return 0, nil
	}

	wrote := 0

	if b.memBuf.hasSpace() {
		n := b.memBuf.write(p)
		wrote += n
		if wrote == len(p) {
			return wrote, nil
		}
		p = p[n:]
	}

	if b.fileBuf == nil {
		limit := b.remainingTotal()
		if limit == 0 {
			return wrote, ErrMaxSizeExceeded(b.limit)
		}
		fb, err := newFileBuffer(limit, b.tmpDir)
		if err != nil {
			return wrote, err
		}
		b.fileBuf = fb
	}

	n, err := b.fileBuf.write(p)
	wrote += n
	return wrote, err
}

// WriteString appends s to the buffer. See Write for error semantics.
func (b *buffer) WriteString(s string) (int, error) {
	return b.writeString(s)
}

func (b *buffer) writeString(s string) (int, error) {
	if err := b.ensureOpen(); err != nil {
		return 0, err
	}
	if b.readOnly {
		return 0, ErrReadOnly
	}
	if len(s) == 0 {
		return 0, nil
	}

	wrote := 0

	// Fill mem first
	if b.memBuf.hasSpace() {
		n := b.memBuf.writeString(s)
		wrote += n
		if wrote == len(s) {
			return wrote, nil
		}
		s = s[n:]
	}

	// Ensure file buffer exists
	if b.fileBuf == nil {
		limit := b.remainingTotal()
		if limit == 0 {
			return wrote, ErrMaxSizeExceeded(b.limit)
		}
		fb, err := newFileBuffer(limit, b.tmpDir)
		if err != nil {
			return wrote, err
		}
		b.fileBuf = fb
	}

	n, err := b.fileBuf.write([]byte(s))
	wrote += n
	return wrote, err
}

// WriteByte appends a single byte to the buffer. See Write for error semantics.
func (b *buffer) WriteByte(c byte) error {
	var one [1]byte
	one[0] = c
	_, err := b.write(one[:])
	return err
}

// ReadFrom reads from r until EOF and appends to the buffer.
// It returns ErrReadOnly if read-only, and ErrMaxSizeExceeded if the buffer reaches its configured limit.
// On ErrMaxSizeExceeded, n is the number of bytes successfully stored.
func (b *buffer) ReadFrom(r io.Reader) (int64, error) {
	if err := b.ensureOpen(); err != nil {
		return 0, err
	}
	if b.readOnly {
		return 0, ErrReadOnly
	}
	buf := make([]byte, readChunkSize)
	var total int64
	for {
		n, err := r.Read(buf)
		if n > 0 {
			m, werr := b.write(buf[:n])
			total += int64(m)
			if werr != nil {
				return total, werr
			}
			if m != n {
				return total, io.ErrShortWrite
			}
		}
		if err != nil {
			if err == io.EOF {
				return total, nil
			}
			return total, err
		}
	}
}

// WriteTo writes data to w until the buffer is drained or an error occurs.
// The return value n is the number of bytes written. Any error
// encountered during the write is also returned.
func (b *buffer) WriteTo(w io.Writer) (n int64, err error) {
	if err := b.ensureOpen(); err != nil {
		return 0, err
	}

	mem := b.memBuf.bytes()
	memSize := int64(len(mem))

	if b.pos < memSize {
		toWrite := memSize - b.pos
		m, e := w.Write(mem[int(b.pos):])
		n += int64(m)
		b.pos += int64(m)
		if e != nil {
			return n, e
		}
		if int64(m) < toWrite {
			return n, io.ErrShortWrite
		}
	}

	if b.fileBuf != nil && b.pos < b.Size() {
		diskOff := b.pos - memSize
		m, e := b.fileBuf.writeToAt(diskOff, w)
		n += m
		b.pos += m
		if e != nil {
			return n, e
		}
	}

	return n, nil
}

// Read reads up to len(p) bytes from the current read position
// and advances the position by the number of bytes read.
//
// When Read reaches the end of the buffer, it returns the number
// of bytes read and io.EOF. Subsequent reads return 0, io.EOF.
//
// Read implements standard io.Reader semantics.
func (b *buffer) Read(p []byte) (n int, err error) {
	n, err = b.ReadAt(p, b.pos)
	b.pos += int64(n)

	if err == io.EOF && n > 0 {
		return n, nil
	}

	return n, err
}

// ReadAt reads up to len(p) bytes starting at absolute offset off.
// It implements io.ReaderAt semantics.
//
// If fewer than len(p) bytes are available, ReadAt returns the
// number of bytes read and a non-nil error, typically io.EOF.
// ReadAt does not modify the buffer's read position.
func (b *buffer) ReadAt(p []byte, off int64) (n int, err error) {
	if err := b.ensureOpen(); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 {
		return 0, errInvalidOffset
	}

	size := b.Size()
	if off >= size {
		return 0, io.EOF
	}

	mem := b.memBuf.bytes()
	memSize := int64(len(mem))

	// Read from memory region if off is inside it.
	if off < memSize {
		start := int(off) // safe: off < len(mem) <= int range by construction
		n = copy(p, mem[start:])
		off += int64(n)

		if n == len(p) {
			return n, nil
		}
		// We consumed all memory; continue on disk.
		p = p[n:]
	}

	if b.fileBuf == nil {
		return n, io.EOF
	}

	diskOff := off - memSize
	m, e := b.fileBuf.readAt(diskOff, p)
	n += m

	// If disk read filled remainder, success.
	if m == len(p) {
		return n, nil
	}

	// Otherwise short read; preserve the most relevant error.
	if e != nil {
		return n, e
	}
	return n, io.EOF
}

// Peek returns up to n bytes starting at the current read position without advancing it.
// If fewer than n bytes are available, Peek returns the available bytes and io.EOF.
func (b *buffer) Peek(n int) ([]byte, error) {
	if err := b.ensureOpen(); err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, errNegativeCount
	}
	if n == 0 {
		return []byte{}, nil
	}
	if b.Len() == 0 {
		return []byte{}, io.EOF
	}
	if int64(n) > b.Len() {
		n = int(b.Len())
		p := make([]byte, n)
		_, _ = b.ReadAt(p, b.pos)
		return p, io.EOF
	}
	p := make([]byte, n)
	_, err := b.ReadAt(p, b.pos)
	return p, err
}

// ReadByte reads and returns the next byte from the buffer.
// If no byte is available, it returns error io.EOF.
func (b *buffer) ReadByte() (byte, error) {
	if err := b.ensureOpen(); err != nil {
		return 0, err
	}
	if b.Len() == 0 {
		return 0, io.EOF
	}

	c, err := b.ReadByteAt(b.pos)
	if err != nil {
		return 0, err
	}

	b.pos++
	return c, nil
}

// ReadByteAt reads and returns the byte at absolute offset off in the buffer.
// It does not advance the read position.
func (b *buffer) ReadByteAt(off int64) (byte, error) {
	if err := b.ensureOpen(); err != nil {
		return 0, err
	}
	if off < 0 {
		return 0, errInvalidOffset
	}

	size := b.Size()
	if off >= size {
		return 0, io.EOF
	}

	mem := b.memBuf.bytes()
	memSize := int64(len(mem))

	if off < memSize {
		// safe: off < memSize <= int range by construction
		return mem[int(off)], nil
	}

	if b.fileBuf == nil {
		// size says there should be data, but no file buffer exists -> invariant violation
		return 0, io.EOF
	}
	return b.fileBuf.readByteAt(off - memSize)
}

// ReadBytes reads until the first occurrence of delim in the input,
// returning a slice containing the data up to and including the delimiter.
// If ReadBytes encounters an error before finding a delimiter,
// it returns the data read before the error and the error itself (often io.EOF).
// ReadBytes returns err != nil if and only if the returned data does not end in
// delim.
func (b *buffer) ReadBytes(delim byte) ([]byte, error) {
	if err := b.ensureOpen(); err != nil {
		return nil, err
	}
	if b.Len() == 0 {
		return []byte{}, io.EOF
	}

	var line []byte

	mem := b.memBuf.bytes()
	memSize := int64(len(mem))

	if b.pos < memSize {
		// b.off fits in int because memSize <= mem limit <= int
		start := int(b.pos)

		if i := bytes.IndexByte(mem[start:], delim); i >= 0 {
			end := start + i + 1
			line = append(line, mem[start:end]...)
			b.pos = int64(end)
			return line, nil
		}

		// Delim not found in remaining memory: take all remaining mem.
		line = append(line, mem[start:]...)
		b.pos = memSize
	}

	if b.fileBuf == nil || b.pos >= b.Size() {
		return line, io.EOF
	}

	diskOff := b.pos - memSize
	buf := make([]byte, readChunkSize) // chunk size

	for {
		n, err := b.fileBuf.readAt(diskOff, buf)
		if n > 0 {
			if i := bytes.IndexByte(buf[:n], delim); i >= 0 {
				end := i + 1
				line = append(line, buf[:end]...)
				diskOff += int64(end)
				b.pos = memSize + diskOff
				return line, nil
			}
			line = append(line, buf[:n]...)
			diskOff += int64(n)
		}

		if err != nil {
			// io.EOF means we drained disk without finding delim.
			b.pos = memSize + diskOff
			return line, err
		}
	}
}

// Seek sets the read position. It follows standard io.Seeker semantics:
// seeking past end is allowed; subsequent reads at that position return 0, io.EOF.
func (b *buffer) Seek(offset int64, whence int) (int64, error) {
	if err := b.ensureOpen(); err != nil {
		return 0, err
	}

	var base int64
	switch whence {
	case io.SeekStart:
		base = 0
	case io.SeekCurrent:
		base = b.pos
	case io.SeekEnd:
		base = b.Size()
	default:
		return 0, errWhence
	}

	newOff := base + offset
	if newOff < 0 {
		return 0, errInvalidOffset
	}

	b.pos = newOff
	return b.pos, nil
}

func (b *buffer) remainingTotal() int64 {
	if b.limit == unlimited {
		return unlimited
	}
	rem := b.limit - b.Size()
	if rem < 0 {
		return 0
	}
	return rem
}
