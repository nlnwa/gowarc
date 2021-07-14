package diskbuffer

import (
	"io"
	"io/ioutil"
	"os"
)

// A fileBuffer is a variable-sized buffer of bytes with Read and Write methods.
// The zero value for Buffer is an empty buffer ready to use.
type fileBuffer struct {
	diskFile *os.File // content
	len      int64    // length of data.
	max      int64    // max allowed size of buf
}

func newFileBuffer(maxSize int64, tmpDir string) (*fileBuffer, error) {
	var err error
	if maxSize <= 0 {
		maxSize = 0
	}
	b := &fileBuffer{
		max: maxSize,
	}
	if b.diskFile, err = ioutil.TempFile(tmpDir, tmpFilePrefix); err != nil {
		return nil, err
	}

	return b, nil
}

func (b *fileBuffer) close() error {
	if b == nil || b.diskFile == nil {
		return nil
	}

	b.len = 0
	if err := b.diskFile.Close(); err != nil {
		return err
	}
	if err := os.Remove(b.diskFile.Name()); err != nil {
		return err
	}
	return nil
}

// empty reports whether the buffer is empty.
func (b *fileBuffer) empty() bool {
	if b == nil {
		return true
	}
	return b.len == 0
}

// size returns the number of bytes in the buffer
func (b *fileBuffer) size() int64 {
	if b == nil {
		return 0
	}
	return b.len
}

// cap returns the number of bytes that can be stored in the buffer
func (b *fileBuffer) cap() int64 {
	if b == nil {
		return 0
	}
	return b.max
}

// free returns the number of bytes remaining before the buffer is full
func (b *fileBuffer) free() int64 {
	if b == nil {
		return 0
	}
	return b.max - b.len
}

// hasSpace returns true if buffer has space left
func (b *fileBuffer) hasSpace() bool {
	if b == nil {
		return false
	}
	return b.free() > 0
}

// write appends the contents of p to the file. The return value n is the length written.
func (b *fileBuffer) write(p []byte) (n int, err error) {
	if b.hasSpace() {
		var e error
		if int64(len(p)) > b.free() {
			e = ErrMaxSizeExceeded(b.max)
			p = p[:b.free()]
		}
		n, err = b.diskFile.WriteAt(p, b.len)
		m := int64(n)
		b.len += m
		if err == nil {
			err = e
		}
		return n, err
	}
	return n, ErrMaxSizeExceeded(b.max)
}

// readFrom reads data from r until EOF and appends it to the buffer, growing
// the buffer as needed. The return value n is the number of bytes read. Any
// error encountered during the read is also returned.
func (b *fileBuffer) readFrom(r io.Reader) (n int64, err error) {
	if b.hasSpace() {
		_, err = b.diskFile.Seek(0, io.SeekEnd)
		if err != nil {
			return 0, err
		}
		n, err = io.CopyN(b.diskFile, r, b.free())
		b.len += n
		if err == io.EOF {
			return n, nil
		}
		return n, err
	}
	return n, ErrMaxSizeExceeded(b.max)
}

func (b *fileBuffer) writeTo(off int64, w io.Writer) (n int64, err error) {
	_, err = b.diskFile.Seek(off, io.SeekStart)
	if err != nil {
		return
	}
	n, err = io.Copy(w, b.diskFile)
	if err != nil {
		return n, err
	}
	return n, nil
}

// read reads len(p) bytes starting at off, from the buffer or until the buffer
// is drained. The return value n is the number of bytes read. If the
// buffer has no data to return, err is io.EOF (unless len(p) is zero);
// otherwise it is nil.
func (b *fileBuffer) read(off int64, p []byte) (n int, err error) {
	if b.empty() || off >= b.len {
		if len(p) == 0 {
			return 0, nil
		}
		return 0, io.EOF
	}
	n, err = b.diskFile.ReadAt(p, off)
	return n, err
}

// readByte reads and returns the byte at offset off from the buffer.
// If no byte is available, it returns error io.EOF.
func (b *fileBuffer) readByte(off int64) (byte, error) {
	p := make([]byte, 1)
	_, err := b.read(off, p)
	return p[0], err
}
