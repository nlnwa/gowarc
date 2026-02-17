package gowarc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithVersion(t *testing.T) {
	tests := []struct {
		name    string
		version *WarcVersion
		want    string
	}{
		{"v1.0", V1_0, "WARC/1.0"},
		{"v1.1", V1_1, "WARC/1.1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rb := NewRecordBuilder(Warcinfo, WithVersion(tt.version),
				WithSpecViolationPolicy(ErrIgnore), WithSyntaxErrorPolicy(ErrIgnore))
			rb.AddWarcHeader(WarcRecordID, "<urn:uuid:00000000-0000-0000-0000-000000000000>")
			rb.AddWarcHeader(WarcDate, "2024-01-01T00:00:00Z")
			rb.AddWarcHeader(ContentType, ApplicationWarcFields)
			record, _, err := rb.Build()
			assert.NoError(t, err)
			defer record.Close()

			assert.Equal(t, tt.want, record.Version().String())
		})
	}
}

func TestWithRecordIdFunc(t *testing.T) {
	tests := []struct {
		name   string
		idFunc func() (string, error)
		wantID string
	}{
		{
			"custom static id",
			func() (string, error) { return "urn:uuid:custom-id-1234", nil },
			"urn:uuid:custom-id-1234",
		},
		{
			"custom uri",
			func() (string, error) { return "http://example.com/rec/1", nil },
			"http://example.com/rec/1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rb := NewRecordBuilder(Warcinfo,
				WithRecordIdFunc(tt.idFunc),
				WithSpecViolationPolicy(ErrIgnore), WithSyntaxErrorPolicy(ErrIgnore))
			rb.AddWarcHeader(WarcDate, "2024-01-01T00:00:00Z")
			rb.AddWarcHeader(ContentType, ApplicationWarcFields)
			record, _, err := rb.Build()
			assert.NoError(t, err)
			defer record.Close()

			assert.Equal(t, tt.wantID, record.RecordId())
		})
	}
}

func TestWithDefaultDigestAlgorithm(t *testing.T) {
	tests := []struct {
		name      string
		algorithm string
		prefix    string
	}{
		{"sha1", "sha1", "sha1:"},
		{"sha256", "sha256", "sha256:"},
		{"md5", "md5", "md5:"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rb := NewRecordBuilder(Warcinfo,
				WithDefaultDigestAlgorithm(tt.algorithm),
				WithAddMissingDigest(true),
				WithSpecViolationPolicy(ErrIgnore), WithSyntaxErrorPolicy(ErrIgnore))
			rb.AddWarcHeader(WarcRecordID, "<urn:uuid:00000000-0000-0000-0000-000000000000>")
			rb.AddWarcHeader(WarcDate, "2024-01-01T00:00:00Z")
			rb.AddWarcHeader(ContentType, ApplicationWarcFields)
			_, _ = rb.WriteString("test data")
			record, _, err := rb.Build()
			assert.NoError(t, err)
			defer record.Close()

			digest := record.WarcHeader().Get(WarcBlockDigest)
			assert.Contains(t, digest, tt.prefix) // e.g. "sha256:..."
		})
	}
}

func TestWithSkipParseBlock(t *testing.T) {
	rb := NewRecordBuilder(Response,
		WithSkipParseBlock(),
		WithSpecViolationPolicy(ErrIgnore), WithSyntaxErrorPolicy(ErrIgnore))
	rb.AddWarcHeader(WarcRecordID, "<urn:uuid:00000000-0000-0000-0000-000000000000>")
	rb.AddWarcHeader(WarcDate, "2024-01-01T00:00:00Z")
	rb.AddWarcHeader(ContentType, "application/http;msgtype=response")
	_, _ = rb.WriteString("HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nHello")
	record, _, err := rb.Build()
	assert.NoError(t, err)
	defer record.Close()

	// With SkipParseBlock, block should NOT be an HttpResponseBlock
	_, ok := record.Block().(HttpResponseBlock)
	assert.False(t, ok, "expected generic block when SkipParseBlock is set")
}

func TestWithNoValidation(t *testing.T) {
	// WithNoValidation should allow records with missing required fields
	rb := NewRecordBuilder(Warcinfo, WithNoValidation())
	// Deliberately don't add any required headers
	_, _ = rb.WriteString("some content")
	record, _, err := rb.Build()
	assert.NoError(t, err)
	defer record.Close()

	// With SkipParseBlock (implied by NoValidation), the block should be generic
	_, ok := record.Block().(WarcFieldsBlock)
	assert.False(t, ok, "expected generic block with NoValidation (skipParseBlock=true)")
}

func TestWithBufferTmpDir(t *testing.T) {
	// Just verify the option can be applied without panic
	rb := NewRecordBuilder(Warcinfo,
		WithBufferTmpDir(t.TempDir()),
		WithSpecViolationPolicy(ErrIgnore), WithSyntaxErrorPolicy(ErrIgnore))
	rb.AddWarcHeader(WarcRecordID, "<urn:uuid:00000000-0000-0000-0000-000000000000>")
	rb.AddWarcHeader(WarcDate, "2024-01-01T00:00:00Z")
	rb.AddWarcHeader(ContentType, ApplicationWarcFields)
	_, _ = rb.WriteString("test buffer dir")
	record, _, err := rb.Build()
	assert.NoError(t, err)
	defer record.Close()
}

func TestWithBufferMaxMemBytes(t *testing.T) {
	// Verify the option can be applied and content is written/read correctly
	rb := NewRecordBuilder(Warcinfo,
		WithBufferMaxMemBytes(64),
		WithSpecViolationPolicy(ErrIgnore), WithSyntaxErrorPolicy(ErrIgnore))
	rb.AddWarcHeader(WarcRecordID, "<urn:uuid:00000000-0000-0000-0000-000000000000>")
	rb.AddWarcHeader(WarcDate, "2024-01-01T00:00:00Z")
	rb.AddWarcHeader(ContentType, ApplicationWarcFields)
	_, _ = rb.WriteString("small content")
	record, _, err := rb.Build()
	assert.NoError(t, err)
	defer record.Close()
}

func TestWithUrlParserOptions(t *testing.T) {
	// Just verify the option can be applied without error
	opts := newOptions(WithUrlParserOptions())
	// No panic or error means success; urlParserOptions starts as nil and appending nothing keeps it nil
	_ = opts
}
