package diskbuffer

// smallBufferSize is an initial allocation minimal capacity.
const smallBufferSize = 512

// memBuffer is the in-memory portion of a buffer.
// Data is stored in a byte slice up to a configured limit,
// beyond which writes spill to the file-backed portion.
type memBuffer struct {
	buf   []byte // contents
	limit int64  // maximum allowed size
}

func newMemBuffer(limit int64, initialCap int) *memBuffer {
	if limit < 0 {
		limit = 0
	}
	if initialCap < 0 {
		initialCap = 0
	}
	if initialCap < smallBufferSize {
		initialCap = smallBufferSize
	}
	if int64(initialCap) > limit {
		initialCap = int(limit)
	}
	return &memBuffer{
		buf:   make([]byte, 0, initialCap),
		limit: limit,
	}
}

// Size returns the number of bytes stored in the buffer.
func (m *memBuffer) Size() int64 { return int64(len(m.buf)) }

// Limit returns the maximum number of bytes the buffer can hold.
func (m *memBuffer) Limit() int64 { return m.limit }

// hasSpace reports whether the buffer can accept more data.
func (m *memBuffer) hasSpace() bool { return int64(len(m.buf)) < m.limit }

func (m *memBuffer) bytes() []byte { return m.buf }

// write appends p to the buffer, truncating to the remaining capacity.
// It returns the number of bytes written.
func (m *memBuffer) write(p []byte) int {
	if len(p) == 0 {
		return 0
	}
	free := m.limit - int64(len(m.buf))
	if free <= 0 {
		return 0
	}
	if int64(len(p)) > free {
		p = p[:int(free)]
	}
	m.buf = append(m.buf, p...)
	return len(p)
}

// writeString appends s to the buffer, truncating to the remaining capacity.
// Unlike write, this avoids the []byte(s) allocation by using append(buf, s...).
func (m *memBuffer) writeString(s string) int {
	free := m.limit - int64(len(m.buf))
	if free <= 0 {
		return 0
	}
	if int64(len(s)) > free {
		s = s[:int(free)]
	}
	m.buf = append(m.buf, s...)
	return len(s)
}
