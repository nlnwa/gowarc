package diskbuffer

import (
	"errors"
	"io"
	"io/fs"
	"os"
)

// fileBuffer is the file-backed portion of a buffer.
// Data that overflows the in-memory limit is written to a temporary file.
type fileBuffer struct {
	file  *os.File // temporary backing file
	size  int64    // current number of bytes stored
	limit int64    // maximum allowed bytes (math.MaxInt64 == unlimited)
}

func newFileBuffer(maxSize int64, tmpDir string) (*fileBuffer, error) {
	if maxSize <= 0 {
		return nil, errors.New("diskbuffer: file buffer limit must be positive")
	}

	file, err := os.CreateTemp(tmpDir, tmpFilePrefix)
	if err != nil {
		return nil, err
	}

	return &fileBuffer{
		limit: maxSize,
		file:  file,
	}, nil
}

func (b *fileBuffer) close() error {
	if b.file == nil {
		return nil
	}

	f := b.file
	name := f.Name()

	b.file = nil
	b.size = 0

	closeErr := f.Close()
	if errors.Is(closeErr, fs.ErrClosed) {
		closeErr = nil
	}

	removeErr := os.Remove(name)
	if errors.Is(removeErr, fs.ErrNotExist) {
		removeErr = nil
	}

	if removeErr != nil && closeErr != nil {
		return errors.Join(closeErr, removeErr)
	}
	if closeErr != nil {
		return closeErr
	}
	return removeErr
}

// Size returns the number of bytes stored in the buffer.
func (b *fileBuffer) Size() int64 {
	return b.size
}

// Limit returns the maximum number of bytes the buffer can hold.
func (b *fileBuffer) Limit() int64 {
	return b.limit
}

// write appends p to the file. It returns the number of bytes written.
// If the write would exceed the limit, it writes as much as possible
// and returns ErrMaxSizeExceeded.
func (b *fileBuffer) write(p []byte) (n int, err error) {
	free := b.limit - b.size
	if free <= 0 {
		return 0, ErrMaxSizeExceeded(b.limit)
	}

	var e error
	if int64(len(p)) > free {
		p = p[:int(free)]
		e = ErrMaxSizeExceeded(b.limit)
	}

	n, err = b.file.Write(p)
	b.size += int64(n)
	if err == nil {
		err = e
	}

	return n, err
}

func (b *fileBuffer) writeToAt(off int64, w io.Writer) (n int64, err error) {
	if off < 0 || off > b.size {
		return 0, io.EOF
	}
	sr := io.NewSectionReader(b.file, off, b.size-off)
	return io.Copy(w, sr)
}

// readAt reads up to len(p) bytes starting at off from the backing file.
// Returns io.EOF when the buffer has no data or off is past the end.
func (b *fileBuffer) readAt(off int64, p []byte) (n int, err error) {
	if b.size == 0 || off >= b.size {
		if len(p) == 0 {
			return
		}
		return 0, io.EOF
	}
	return b.file.ReadAt(p, off)
}

// readByte reads and returns the byte at offset off from the buffer.
// If no byte is available, it returns error io.EOF.
func (b *fileBuffer) readByteAt(off int64) (byte, error) {
	var p [1]byte
	_, err := b.readAt(off, p[:])
	return p[0], err
}
