package diskbuffer

import (
	"io"
)

// smallBufferSize is an initial allocation minimal capacity.
const smallBufferSize = 512

// A Buffer is a variable-sized buffer of bytes with Read and Write methods.
// The zero value for Buffer is an empty buffer ready to use.
type memBuffer struct {
	buf []byte // content
	len int64  // length of data.
	max int64  // max allowed size of buf
}

func newMemBuffer(maxSize int64, sizeHint int64) *memBuffer {
	if sizeHint < smallBufferSize {
		sizeHint = smallBufferSize
	}
	if sizeHint >= maxSize {
		sizeHint = maxSize - 1
	}
	return &memBuffer{
		buf: make([]byte, sizeHint),
		max: maxSize,
	}
}

// empty reports whether the buffer is empty.
func (b *memBuffer) empty() bool { return b.len == 0 }

// size returns the number of bytes in the buffer
func (b *memBuffer) size() int64 { return b.len }

// cap returns the number of bytes that can be stored in the buffer
func (b *memBuffer) cap() int64 { return b.max }

// free returns the number of bytes remaining before the buffer is full
func (b *memBuffer) free() int64 { return b.max - b.len }

// hasSpace returns true if buffer has space left
func (b *memBuffer) hasSpace() bool { return b.free() > 0 }

// bytes returns a slice of length b.len() holding the buffered data.
// The slice is valid for use only until the next buffer modification (that is,
// only until the next call to a method like Read, Write, Reset, or Truncate).
// The slice aliases the buffer content at least until the next buffer modification,
// so immediate changes to the slice will affect the result of future reads.
func (b *memBuffer) bytes() []byte { return b.buf[:b.len] }

// grow grows the buffer to give space for n more bytes if capacity allows.
// It returns the index where bytes should be written and true if at least one byte can be written to buffer.
func (b *memBuffer) grow(n int64) (int64, bool) {
	c := int64(cap(b.buf))
	if (c - b.len) >= n {
		return b.len, true
	}
	if c >= b.max {
		return b.len, false
	}

	newSize := int64(2*len(b.buf)) + n
	if newSize > b.max {
		newSize = b.max
	}
	buf := make([]byte, newSize)
	copy(buf, b.buf)
	b.buf = buf

	return b.len, true
}

// write appends the contents of p to the buffer, growing the buffer as
// needed. The return value n is the length written.
func (b *memBuffer) write(p []byte) (n int) {
	if m, ok := b.grow(int64(len(p))); ok {
		n = copy(b.buf[m:], p)
		b.len += int64(n)
	}
	return n
}

// MinRead is the minimum slice size passed to a Read call by
// Buffer.ReadFrom. As long as the Buffer has at least MinRead bytes beyond
// what is required to hold the contents of r, ReadFrom will not grow the
// underlying buffer.
const MinRead = 512

// readFrom reads data from r until EOF and appends it to the buffer, growing
// the buffer as needed. The return value n is the number of bytes read. Any
// error encountered during the read is also returned.
func (b *memBuffer) readFrom(r io.Reader) (n int64, err error) {
	for {
		if i, ok := b.grow(MinRead); ok {
			m, e := r.Read(b.buf[i:])
			if m < 0 {
				panic(errNegativeRead)
			}
			w := int64(m)
			b.len += w
			n += w
			if e != nil {
				return n, e
			}
		} else {
			return n, nil
		}
	}
}

// writeByte appends the byte c to the buffer, growing the buffer as needed.
// If buffer capacity is exceeded, io.EOF is returned.
func (b *memBuffer) WriteByte(c byte) error {
	if m, ok := b.grow(1); ok {
		b.buf[m] = c
		b.len++
		return nil
	} else {
		return io.EOF
	}
}

// read reads len(p) bytes starting at off, from the buffer or until the buffer
// is drained. The return value n is the number of bytes read. If the
// buffer has no data to return, err is io.EOF (unless len(p) is zero);
// otherwise it is nil.
func (b *memBuffer) read(off int64, p []byte) (n int, err error) {
	if b.empty() || off >= b.len {
		if len(p) == 0 {
			return 0, nil
		}
		return 0, io.EOF
	}
	n = copy(p, b.buf[off:b.len])
	if len(p) > n {
		return n, io.EOF
	}
	return n, nil
}

// slice returns a slice containing n bytes from offset off.
// If there are fewer than n bytes in the buffer, slice returns the entire buffer.
// The slice is only valid until the next call to a read or write method.
func (b *memBuffer) slice(off int64, n int) []byte {
	m := int(b.len - off)
	if n > m {
		n = m
	}
	data := b.buf[off : int(off)+n]
	return data
}

// readByte reads and returns the byte at offset off from the buffer.
// If no byte is available, it returns error io.EOF.
func (b *memBuffer) readByte(off int64) (byte, error) {
	if b.empty() {
		return 0, io.EOF
	}
	if off >= b.len {
		return 0, io.EOF
	}
	c := b.buf[off]
	return c, nil
}
