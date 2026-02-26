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

package countingreader

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestNew(t *testing.T) {
	input := strings.NewReader("test data")
	cr := New(input)

	if cr == nil {
		t.Fatal("New() returned nil")
	}

	if cr.N() != 0 {
		t.Errorf("New reader should have 0 bytes read, got %d", cr.N())
	}
}

func TestNewLimited(t *testing.T) {
	input := strings.NewReader("test data")
	cr := NewLimited(input, 4)

	if cr == nil {
		t.Fatal("NewLimited() returned nil")
	}

	if cr.N() != 0 {
		t.Errorf("NewLimited reader should have 0 bytes read, got %d", cr.N())
	}
}

func TestReader_Read(t *testing.T) {
	input := "Hello, World!"
	cr := New(strings.NewReader(input))

	buf := make([]byte, 5)
	n, err := cr.Read(buf)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if n != 5 {
		t.Errorf("Expected to read 5 bytes, got %d", n)
	}

	if string(buf) != "Hello" {
		t.Errorf("Expected to read 'Hello', got %q", string(buf))
	}

	if cr.N() != 5 {
		t.Errorf("Expected counter to be 5, got %d", cr.N())
	}
}

func TestReader_ReadAll(t *testing.T) {
	input := "Hello, World!"
	cr := New(strings.NewReader(input))

	data, err := io.ReadAll(cr)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if string(data) != input {
		t.Errorf("Expected to read %q, got %q", input, string(data))
	}

	if cr.N() != int64(len(input)) {
		t.Errorf("Expected counter to be %d, got %d", len(input), cr.N())
	}
}

func TestReader_MultipleReads(t *testing.T) {
	input := "Hello, World!"
	cr := New(strings.NewReader(input))

	buf1 := make([]byte, 5)
	n1, _ := cr.Read(buf1)

	if cr.N() != int64(n1) {
		t.Errorf("After first read, expected counter %d, got %d", n1, cr.N())
	}

	buf2 := make([]byte, 8)
	n2, _ := cr.Read(buf2)

	total := int64(n1 + n2)
	if cr.N() != total {
		t.Errorf("After second read, expected counter %d, got %d", total, cr.N())
	}

	if string(buf1)+string(buf2[:n2]) != input {
		t.Errorf("Data mismatch")
	}
}

func TestReader_N(t *testing.T) {
	cr := New(strings.NewReader("test"))

	// Initially should be 0
	if cr.N() != 0 {
		t.Errorf("Initial N() should be 0, got %d", cr.N())
	}

	// After reading 2 bytes
	buf := make([]byte, 2)
	_, err := cr.Read(buf)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if cr.N() != 2 {
		t.Errorf("After reading 2 bytes, N() should be 2, got %d", cr.N())
	}

	// After reading 2 more bytes
	_, err = cr.Read(buf)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if cr.N() != 4 {
		t.Errorf("After reading 4 bytes total, N() should be 4, got %d", cr.N())
	}
}

func TestNewLimited_RespectsLimit(t *testing.T) {
	input := "Hello, World!"
	limit := int64(5)
	cr := NewLimited(strings.NewReader(input), limit)

	data, err := io.ReadAll(cr)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if len(data) != int(limit) {
		t.Errorf("Expected to read %d bytes, got %d", limit, len(data))
	}

	if string(data) != "Hello" {
		t.Errorf("Expected to read 'Hello', got %q", string(data))
	}

	if cr.N() != limit {
		t.Errorf("Expected counter to be %d, got %d", limit, cr.N())
	}
}

func TestNewLimited_EOFAtLimit(t *testing.T) {
	input := "Hello, World!"
	limit := int64(5)
	cr := NewLimited(strings.NewReader(input), limit)

	// Read exactly to the limit
	buf := make([]byte, 5)
	n, err := cr.Read(buf)

	if err != nil {
		t.Errorf("First read should not error, got: %v", err)
	}

	if n != 5 {
		t.Errorf("First read should return 5 bytes, got %d", n)
	}

	// Next read should return EOF
	buf2 := make([]byte, 5)
	n2, err2 := cr.Read(buf2)

	if err2 != io.EOF {
		t.Errorf("Expected EOF after limit, got: %v", err2)
	}

	if n2 != 0 {
		t.Errorf("Expected 0 bytes after limit, got %d", n2)
	}
}

func TestNewLimited_PartialReadAtLimit(t *testing.T) {
	input := "Hello, World!"
	limit := int64(7)
	cr := NewLimited(strings.NewReader(input), limit)

	// Try to read 10 bytes but should only get 7
	buf := make([]byte, 10)
	n, err := cr.Read(buf)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if n != int(limit) {
		t.Errorf("Expected to read %d bytes, got %d", limit, n)
	}

	if string(buf[:n]) != "Hello, " {
		t.Errorf("Expected to read 'Hello, ', got %q", string(buf[:n]))
	}
}

func TestNewLimited_ZeroLimit(t *testing.T) {
	input := "Hello, World!"
	cr := NewLimited(strings.NewReader(input), 0)

	buf := make([]byte, 5)
	n, err := cr.Read(buf)

	if err != io.EOF {
		t.Errorf("Expected EOF with 0 limit, got: %v", err)
	}

	if n != 0 {
		t.Errorf("Expected 0 bytes with 0 limit, got %d", n)
	}
}

func TestNewLimited_NegativeLimit(t *testing.T) {
	// Negative limit should behave like unlimited
	input := "Hello, World!"
	cr := NewLimited(strings.NewReader(input), -1)

	data, err := io.ReadAll(cr)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if string(data) != input {
		t.Errorf("Expected to read all data with -1 limit, got %q", string(data))
	}
}

func TestReader_EmptyReader(t *testing.T) {
	cr := New(strings.NewReader(""))

	buf := make([]byte, 10)
	n, err := cr.Read(buf)

	if err != io.EOF {
		t.Errorf("Expected EOF for empty reader, got: %v", err)
	}

	if n != 0 {
		t.Errorf("Expected 0 bytes for empty reader, got %d", n)
	}

	if cr.N() != 0 {
		t.Errorf("Expected counter to be 0 for empty reader, got %d", cr.N())
	}
}

func TestReader_CopyOperations(t *testing.T) {
	input := bytes.Repeat([]byte("a"), 1000)
	cr := New(bytes.NewReader(input))

	var buf bytes.Buffer
	n, err := io.Copy(&buf, cr)

	if err != nil {
		t.Errorf("Unexpected error during copy: %v", err)
	}

	if n != 1000 {
		t.Errorf("Expected to copy 1000 bytes, got %d", n)
	}

	if cr.N() != 1000 {
		t.Errorf("Expected counter to be 1000, got %d", cr.N())
	}

	if !bytes.Equal(buf.Bytes(), input) {
		t.Error("Copied data doesn't match input")
	}
}

func TestNewLimited_CopyWithLimit(t *testing.T) {
	input := bytes.Repeat([]byte("a"), 1000)
	limit := int64(100)
	cr := NewLimited(bytes.NewReader(input), limit)

	var buf bytes.Buffer
	n, err := io.Copy(&buf, cr)

	if err != nil {
		t.Errorf("Unexpected error during copy: %v", err)
	}

	if n != limit {
		t.Errorf("Expected to copy %d bytes, got %d", limit, n)
	}

	if cr.N() != limit {
		t.Errorf("Expected counter to be %d, got %d", limit, cr.N())
	}

	if int64(buf.Len()) != limit {
		t.Errorf("Expected buffer to have %d bytes, got %d", limit, buf.Len())
	}
}

func TestReader_SmallReads(t *testing.T) {
	input := "Hello, World!"
	cr := New(strings.NewReader(input))

	// Read one byte at a time
	for i := 0; i < len(input); i++ {
		buf := make([]byte, 1)
		n, err := cr.Read(buf)

		if err != nil {
			t.Fatalf("Error at byte %d: %v", i, err)
		}

		if n != 1 {
			t.Errorf("Expected to read 1 byte at position %d, got %d", i, n)
		}

		if cr.N() != int64(i+1) {
			t.Errorf("At position %d, expected counter %d, got %d", i, i+1, cr.N())
		}
	}
}

// errorReader is a reader that returns a specific error
type errorReader struct {
	err error
}

func (e *errorReader) Read(p []byte) (n int, err error) {
	return 0, e.err
}

func TestReader_ErrorPropagation(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		wantError bool
	}{
		{
			name:      "EOF error",
			err:       io.EOF,
			wantError: true,
		},
		{
			name:      "unexpected EOF",
			err:       io.ErrUnexpectedEOF,
			wantError: true,
		},
		{
			name:      "custom error",
			err:       errors.New("custom error"),
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			er := &errorReader{err: tt.err}
			cr := New(er)

			buf := make([]byte, 10)
			_, err := cr.Read(buf)

			if tt.wantError && err == nil {
				t.Error("Read() expected error but got none")
			}
			if !tt.wantError && err != nil {
				t.Errorf("Read() unexpected error: %v", err)
			}
			if tt.wantError && err != tt.err {
				t.Errorf("Read() error = %v, want %v", err, tt.err)
			}
		})
	}
}

func TestReader_ZeroByteRead(t *testing.T) {
	input := "test data"
	cr := New(strings.NewReader(input))

	// Read zero bytes
	buf := make([]byte, 0)
	n, err := cr.Read(buf)

	if err != nil {
		t.Errorf("Read(0 bytes) unexpected error: %v", err)
	}

	if n != 0 {
		t.Errorf("Read(0 bytes) returned %d, want 0", n)
	}

	// Counter should still be 0
	if cr.N() != 0 {
		t.Errorf("After Read(0 bytes), N() = %d, want 0", cr.N())
	}
}

func TestReader_NilBuffer(t *testing.T) {
	input := "test data"
	cr := New(strings.NewReader(input))

	// Read with nil buffer
	n, err := cr.Read(nil)

	if err != nil && err != io.EOF {
		t.Errorf("Read(nil) unexpected error: %v", err)
	}

	if n != 0 {
		t.Errorf("Read(nil) returned %d, want 0", n)
	}
}

func TestReader_LargeBuffer(t *testing.T) {
	input := "small"
	cr := New(strings.NewReader(input))

	// Buffer much larger than input
	buf := make([]byte, 1000)
	n, err := cr.Read(buf)

	if err != nil && err != io.EOF {
		t.Errorf("Read() unexpected error: %v", err)
	}

	if n != len(input) {
		t.Errorf("Read() returned %d, want %d", n, len(input))
	}

	if cr.N() != int64(len(input)) {
		t.Errorf("N() = %d, want %d", cr.N(), len(input))
	}
}

func TestReader_SmallBuffer(t *testing.T) {
	input := "large data to read"
	cr := New(strings.NewReader(input))

	// Read in very small chunks
	buf := make([]byte, 1)
	totalRead := 0

	for {
		n, err := cr.Read(buf)
		totalRead += n

		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read() unexpected error: %v", err)
		}
		if n > 1 {
			t.Errorf("Read(1 byte buffer) returned %d, want <= 1", n)
		}
	}

	if totalRead != len(input) {
		t.Errorf("Total read = %d, want %d", totalRead, len(input))
	}

	if cr.N() != int64(len(input)) {
		t.Errorf("N() = %d, want %d", cr.N(), len(input))
	}
}

func TestReader_ConsecutiveReadsAfterEOF(t *testing.T) {
	input := "short"
	cr := New(strings.NewReader(input))

	// Read all data
	buf := make([]byte, 100)
	_, _ = cr.Read(buf)

	// Try reading again after EOF
	n, err := cr.Read(buf)

	if err != io.EOF {
		t.Errorf("Second read should return EOF, got: %v", err)
	}
	if n != 0 {
		t.Errorf("Second read should return 0 bytes, got: %d", n)
	}

	// Counter should not change
	if cr.N() != int64(len(input)) {
		t.Errorf("N() after EOF = %d, want %d", cr.N(), len(input))
	}
}

func TestNewLimited_ZeroLimitEdgeCase(t *testing.T) {
	input := "test data"
	cr := NewLimited(strings.NewReader(input), 0)

	buf := make([]byte, 10)
	n, err := cr.Read(buf)

	if err != io.EOF {
		t.Errorf("Read() with zero limit should return EOF, got: %v", err)
	}
	if n != 0 {
		t.Errorf("Read() with zero limit returned %d bytes, want 0", n)
	}
}

func TestNewLimited_NegativeLimitEdgeCase(t *testing.T) {
	// Per existing test TestNewLimited_NegativeLimit:
	// "Negative limit should behave like unlimited"
	input := "test data"
	cr := NewLimited(strings.NewReader(input), -1)

	data, err := io.ReadAll(cr)

	if err != nil {
		t.Errorf("ReadAll() with negative limit unexpected error: %v", err)
	}

	// Should read all data (unlimited)
	if string(data) != input {
		t.Errorf("Read() with negative limit got %q, want %q", string(data), input)
	}
}

func TestNewLimited_LimitLargerThanContent(t *testing.T) {
	input := "small"
	limit := int64(1000)
	cr := NewLimited(strings.NewReader(input), limit)

	data, err := io.ReadAll(cr)

	if err != nil {
		t.Errorf("ReadAll() unexpected error: %v", err)
	}

	// Should read only the available content
	if len(data) != len(input) {
		t.Errorf("Read %d bytes, want %d", len(data), len(input))
	}

	if cr.N() != int64(len(input)) {
		t.Errorf("N() = %d, want %d", cr.N(), len(input))
	}
}

func TestNewLimited_ExactLimit(t *testing.T) {
	input := "exactly"
	limit := int64(len(input))
	cr := NewLimited(strings.NewReader(input), limit)

	data, err := io.ReadAll(cr)

	if err != nil {
		t.Errorf("ReadAll() unexpected error: %v", err)
	}

	if len(data) != len(input) {
		t.Errorf("Read %d bytes, want %d", len(data), len(input))
	}

	if string(data) != input {
		t.Errorf("Read %q, want %q", string(data), input)
	}

	if cr.N() != limit {
		t.Errorf("N() = %d, want %d", cr.N(), limit)
	}
}

func TestNewLimited_BufferLargerThanLimit(t *testing.T) {
	input := "large data content here"
	limit := int64(5)
	cr := NewLimited(strings.NewReader(input), limit)

	// Try to read more than the limit
	buf := make([]byte, 100)
	n, _ := cr.Read(buf)

	if n != int(limit) {
		t.Errorf("Read() returned %d bytes, want %d", n, limit)
	}

	// Next read should return EOF
	n2, err2 := cr.Read(buf)
	if err2 != io.EOF {
		t.Errorf("Second read should return EOF, got: %v", err2)
	}
	if n2 != 0 {
		t.Errorf("Second read should return 0 bytes, got: %d", n2)
	}
}

func TestNewLimited_MultipleReadsUpToLimit(t *testing.T) {
	input := "0123456789"
	limit := int64(7)
	cr := NewLimited(strings.NewReader(input), limit)

	// Read in chunks
	buf1 := make([]byte, 3)
	n1, err1 := cr.Read(buf1)

	if err1 != nil {
		t.Errorf("First read error: %v", err1)
	}
	if n1 != 3 {
		t.Errorf("First read returned %d, want 3", n1)
	}

	buf2 := make([]byte, 3)
	n2, err2 := cr.Read(buf2)

	if err2 != nil {
		t.Errorf("Second read error: %v", err2)
	}
	if n2 != 3 {
		t.Errorf("Second read returned %d, want 3", n2)
	}

	buf3 := make([]byte, 3)
	n3, _ := cr.Read(buf3)

	// Should only read 1 byte (to reach limit of 7)
	if n3 != 1 {
		t.Errorf("Third read returned %d, want 1", n3)
	}

	// Next read should be EOF
	buf4 := make([]byte, 3)
	n4, _ := cr.Read(buf4)

	if n4 != 0 {
		t.Errorf("Fourth read should return 0 bytes after EOF, got: %d", n4)
	}
	if n4 != 0 {
		t.Errorf("Fourth read should return 0 bytes, got: %d", n4)
	}

	if cr.N() != limit {
		t.Errorf("Final N() = %d, want %d", cr.N(), limit)
	}
}

func TestReader_BinaryData(t *testing.T) {
	// Test with binary data including null bytes
	input := []byte{0, 1, 2, 3, 255, 254, 253}
	cr := New(strings.NewReader(string(input)))

	data, err := io.ReadAll(cr)
	if err != nil {
		t.Errorf("ReadAll() error: %v", err)
	}

	if len(data) != len(input) {
		t.Errorf("Read %d bytes, want %d", len(data), len(input))
	}

	for i, b := range data {
		if b != input[i] {
			t.Errorf("Byte %d = %d, want %d", i, b, input[i])
		}
	}

	if cr.N() != int64(len(input)) {
		t.Errorf("N() = %d, want %d", cr.N(), len(input))
	}
}

func TestReader_EmptyInput(t *testing.T) {
	cr := New(strings.NewReader(""))

	buf := make([]byte, 10)
	n, err := cr.Read(buf)

	if err != io.EOF {
		t.Errorf("Read() on empty input should return EOF, got: %v", err)
	}
	if n != 0 {
		t.Errorf("Read() on empty input returned %d bytes, want 0", n)
	}
	if cr.N() != 0 {
		t.Errorf("N() on empty input = %d, want 0", cr.N())
	}
}

func TestNewLimited_EmptyInput(t *testing.T) {
	cr := NewLimited(strings.NewReader(""), 10)

	buf := make([]byte, 10)
	n, err := cr.Read(buf)

	if err != io.EOF {
		t.Errorf("Read() on empty input should return EOF, got: %v", err)
	}
	if n != 0 {
		t.Errorf("Read() on empty input returned %d bytes, want 0", n)
	}
	if cr.N() != 0 {
		t.Errorf("N() on empty input = %d, want 0", cr.N())
	}
}

func TestReader_VeryLargeCount(t *testing.T) {
	// Test that counter doesn't overflow with large reads
	largeSize := int64(1000000)
	input := strings.Repeat("a", int(largeSize))
	cr := New(strings.NewReader(input))

	_, err := io.ReadAll(cr)
	if err != nil {
		t.Errorf("ReadAll() error: %v", err)
	}

	if cr.N() != largeSize {
		t.Errorf("N() = %d, want %d", cr.N(), largeSize)
	}
}

// partialReader always returns partial reads (half of buffer)
type partialReader struct {
	data []byte
	pos  int
}

func (p *partialReader) Read(buf []byte) (n int, err error) {
	if p.pos >= len(p.data) {
		return 0, io.EOF
	}

	// Read at most half the buffer size
	toRead := len(buf) / 2
	if toRead == 0 {
		toRead = 1
	}
	if toRead > len(p.data)-p.pos {
		toRead = len(p.data) - p.pos
	}

	n = copy(buf, p.data[p.pos:p.pos+toRead])
	p.pos += n
	return n, nil
}

func TestReader_PartialReads(t *testing.T) {
	input := "this is test data"
	pr := &partialReader{data: []byte(input)}
	cr := New(pr)

	data, err := io.ReadAll(cr)
	if err != nil {
		t.Errorf("ReadAll() error: %v", err)
	}

	if string(data) != input {
		t.Errorf("Read %q, want %q", string(data), input)
	}

	if cr.N() != int64(len(input)) {
		t.Errorf("N() = %d, want %d", cr.N(), len(input))
	}
}

func TestNewLimited_PartialReads(t *testing.T) {
	input := "this is test data"
	limit := int64(10)
	pr := &partialReader{data: []byte(input)}
	cr := NewLimited(pr, limit)

	data, err := io.ReadAll(cr)
	if err != nil {
		t.Errorf("ReadAll() error: %v", err)
	}

	if len(data) != int(limit) {
		t.Errorf("Read %d bytes, want %d", len(data), limit)
	}

	if cr.N() != limit {
		t.Errorf("N() = %d, want %d", cr.N(), limit)
	}
}
