package diskbuffer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemBuffer_SizeAndLimit(t *testing.T) {
	m := newMemBuffer(100, 16)
	assert.Equal(t, int64(0), m.Size())
	assert.Equal(t, int64(100), m.Limit())
	assert.True(t, m.hasSpace())

	m.write([]byte("hello"))
	assert.Equal(t, int64(5), m.Size())
	assert.Equal(t, "hello", string(m.bytes()))
}

func TestMemBuffer_Full(t *testing.T) {
	m := newMemBuffer(5, 5)
	m.write([]byte("12345"))
	assert.False(t, m.hasSpace())

	// Writing more returns 0
	n := m.write([]byte("x"))
	assert.Equal(t, 0, n)
}

func TestMemBuffer_WriteString(t *testing.T) {
	m := newMemBuffer(10, 4)
	n := m.writeString("hello")
	assert.Equal(t, 5, n)
	assert.Equal(t, "hello", string(m.bytes()))

	// Write more than free space — should be truncated
	n = m.writeString("world!")
	assert.Equal(t, 5, n) // only 5 bytes free
	assert.Equal(t, "helloworld", string(m.bytes()))
}

func TestMemBuffer_NewWithEdgeCases(t *testing.T) {
	// Negative limit → 0
	m := newMemBuffer(-1, 0)
	assert.Equal(t, int64(0), m.Limit())

	// Negative initial cap
	m = newMemBuffer(100, -1)
	assert.Equal(t, int64(100), m.Limit())

	// Hint > limit → capped to limit
	m = newMemBuffer(10, 20)
	assert.Equal(t, int64(10), m.Limit())
	assert.LessOrEqual(t, cap(m.bytes()), 10)
}
