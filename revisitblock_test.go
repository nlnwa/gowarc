package gowarc

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_revisitBlock_Accessors(t *testing.T) {
	tests := []struct {
		name              string
		content           string
		payloadDigest     string
		wantHeaderBytes   string
		wantPayloadDigest string
	}{
		{
			name:              "response revisit with headers",
			content:           "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\n",
			payloadDigest:     "sha1:da39a3ee5e6b4b0d3255bfef95601890afd80709",
			wantHeaderBytes:   "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\n",
			wantPayloadDigest: "sha1:da39a3ee5e6b4b0d3255bfef95601890afd80709",
		},
		{
			name:              "empty revisit",
			content:           "",
			payloadDigest:     "",
			wantHeaderBytes:   "",
			wantPayloadDigest: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := newDigest("sha1", Base16)
			require.NoError(t, err)

			block, err := parseRevisitBlock(&warcRecordOptions{defaultDigestAlgorithm: "sha1", defaultDigestEncoding: Base16},
				strings.NewReader(tt.content), d, tt.payloadDigest)
			require.NoError(t, err)

			// ProtocolHeaderBytes
			assert.Equal(t, tt.wantHeaderBytes, string(block.ProtocolHeaderBytes()))

			// PayloadBytes returns empty reader
			pr, err := block.PayloadBytes()
			require.NoError(t, err)
			payload, err := io.ReadAll(pr)
			require.NoError(t, err)
			assert.Empty(t, payload)

			// PayloadDigest
			assert.Equal(t, tt.wantPayloadDigest, block.PayloadDigest())

			// BlockDigest should be non-empty for non-empty content
			if tt.content != "" {
				assert.NotEmpty(t, block.BlockDigest())
			}

			// IsCached always true for revisit blocks
			assert.True(t, block.IsCached())
		})
	}
}

func Test_revisitBlock_Write(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{"with http headers", "HTTP/1.1 200 OK\r\nServer: Apache\r\n\r\n"},
		{"empty", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := newDigest("sha1", Base16)
			require.NoError(t, err)

			block, err := parseRevisitBlock(&warcRecordOptions{defaultDigestAlgorithm: "sha1", defaultDigestEncoding: Base16},
				strings.NewReader(tt.content), d, "")
			require.NoError(t, err)

			var buf bytes.Buffer
			n, err := block.Write(&buf)
			require.NoError(t, err)
			assert.Equal(t, int64(len(tt.content)), n)
			assert.Equal(t, tt.content, buf.String())
		})
	}
}

func Test_revisitBlock_Write_Error(t *testing.T) {
	d, err := newDigest("sha1", Base16)
	require.NoError(t, err)
	block, err := parseRevisitBlock(&warcRecordOptions{defaultDigestAlgorithm: "sha1", defaultDigestEncoding: Base16},
		strings.NewReader("HTTP/1.1 200 OK\r\n\r\n"), d, "")
	require.NoError(t, err)

	w := &failWriter{err: io.ErrClosedPipe}
	n, err := block.Write(w)
	assert.Error(t, err)
	assert.Equal(t, int64(0), n)
}

func Test_newRevisitBlock_FromResponseBlock(t *testing.T) {
	builder := NewRecordBuilder(Response, WithSpecViolationPolicy(ErrIgnore), WithSyntaxErrorPolicy(ErrIgnore))
	_, err := builder.WriteString("HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ntest")
	require.NoError(t, err)
	builder.AddWarcHeader(WarcRecordID, "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>")
	builder.AddWarcHeader(WarcDate, "2006-01-02T15:04:05Z")
	builder.AddWarcHeader(ContentType, "application/http;msgtype=response")

	rec, _, err := builder.Build()
	require.NoError(t, err)
	defer func() { assert.NoError(t, rec.Close()) }()
	require.NoError(t, rec.Block().Cache())

	opts := defaultWarcRecordOptions()
	block, err := newRevisitBlock(&opts, rec.Block())
	require.NoError(t, err)
	assert.NotNil(t, block)
	assert.NotEmpty(t, block.BlockDigest())
	assert.NotEmpty(t, block.PayloadDigest())
}

func Test_newRevisitBlock_FromRequestBlock(t *testing.T) {
	builder := NewRecordBuilder(Request, WithSpecViolationPolicy(ErrIgnore), WithSyntaxErrorPolicy(ErrIgnore))
	_, err := builder.WriteString("GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")
	require.NoError(t, err)
	builder.AddWarcHeader(WarcRecordID, "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>")
	builder.AddWarcHeader(WarcDate, "2006-01-02T15:04:05Z")
	builder.AddWarcHeader(ContentType, "application/http;msgtype=request")

	rec, _, err := builder.Build()
	require.NoError(t, err)
	defer func() { assert.NoError(t, rec.Close()) }()
	require.NoError(t, rec.Block().Cache())

	opts := defaultWarcRecordOptions()
	block, err := newRevisitBlock(&opts, rec.Block())
	require.NoError(t, err)
	assert.NotNil(t, block)
	assert.NotEmpty(t, block.ProtocolHeaderBytes())
}

func Test_newRevisitBlock_FromGenericBlock(t *testing.T) {
	opts := defaultWarcRecordOptions()
	gb := &genericBlock{
		rawBytes: strings.NewReader("some content"),
		opts:     &opts,
	}

	block, err := newRevisitBlock(&opts, gb)
	require.NoError(t, err)
	assert.NotNil(t, block)
	assert.Empty(t, block.headerBytes)
}

func Test_newRevisitBlock_UnsupportedType(t *testing.T) {
	opts := defaultWarcRecordOptions()
	wfb := &warcFieldsBlock{}

	_, err := newRevisitBlock(&opts, wfb)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")
}

func Test_parseRevisitBlock_ReadError(t *testing.T) {
	opts := defaultWarcRecordOptions()
	d, err := newDigest("sha1", Base16)
	require.NoError(t, err)

	errReader := ReplaceErrReader(strings.NewReader("header bytes"), io.ErrUnexpectedEOF)
	_, err = parseRevisitBlock(&opts, errReader, d, "sha1:abc")
	assert.Error(t, err)
}
