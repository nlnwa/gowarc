package gowarc

import (
	"bytes"
	"strings"
	"testing"

	"github.com/klauspost/compress/gzip"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// minimalRecord returns a valid WARC record string with the given type and record ID suffix.
// The record has zero-length content.
func minimalRecord(recordType, idSuffix string) string {
	return "WARC/1.1\r\n" +
		"WARC-Type: " + recordType + "\r\n" +
		"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
		"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac12" + idSuffix + ">\r\n" +
		"Content-Type: application/warc-fields\r\n" +
		"Content-Length: 0\r\n" +
		"\r\n" +
		"\r\n\r\n"
}

func gzipString(s string) []byte {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, _ = gz.Write([]byte(s))
	_ = gz.Close()
	return buf.Bytes()
}

func TestWarcFileReader_Records(t *testing.T) {
	rec1 := minimalRecord("warcinfo", "0001")
	rec2 := minimalRecord("warcinfo", "0002")
	rec3 := minimalRecord("warcinfo", "0003")

	type wantRecord struct {
		recordType    RecordType
		idSuffix      string // substring of the record ID to match
		sizeGt0       bool
		hasValidation bool // expect non-empty Validation slice
	}

	tests := []struct {
		name string
		opts []WarcRecordOption
		data []byte // raw stream data

		wantRecords []wantRecord
		wantErr     string // non-empty: the iterator should yield an error containing this
		wantErrIs   error  // non-nil: the iterator error must match errors.Is
		wantNoErr   bool   // explicitly expect NO error (distinguishes from don't-care)
	}{
		{
			name:      "empty stream yields no records",
			data:      []byte{},
			wantNoErr: true,
		},
		{
			name: "single record",
			data: []byte(rec1),
			wantRecords: []wantRecord{
				{recordType: Warcinfo, idSuffix: "0001", sizeGt0: true},
			},
		},
		{
			name: "two records with correct offsets and sizes",
			data: []byte(rec1 + rec2),
			wantRecords: []wantRecord{
				{recordType: Warcinfo, idSuffix: "0001", sizeGt0: true},
				{recordType: Warcinfo, idSuffix: "0002", sizeGt0: true},
			},
		},
		{
			name: "three records",
			data: []byte(rec1 + rec2 + rec3),
			wantRecords: []wantRecord{
				{recordType: Warcinfo, idSuffix: "0001", sizeGt0: true},
				{recordType: Warcinfo, idSuffix: "0002", sizeGt0: true},
				{recordType: Warcinfo, idSuffix: "0003", sizeGt0: true},
			},
		},
		{
			name: "gzip compressed single record",
			data: gzipString(rec1),
			wantRecords: []wantRecord{
				{recordType: Warcinfo, idSuffix: "0001", sizeGt0: true},
			},
		},
		{
			name: "gzip compressed multi-stream (two records)",
			data: append(gzipString(rec1), gzipString(rec2)...),
			wantRecords: []wantRecord{
				{recordType: Warcinfo, idSuffix: "0001", sizeGt0: true},
				{recordType: Warcinfo, idSuffix: "0002", sizeGt0: true},
			},
		},
		{
			name: "garbage before record with ErrWarn produces validation",
			opts: []WarcRecordOption{WithSyntaxErrorPolicy(ErrWarn), WithSpecViolationPolicy(ErrWarn)},
			data: append([]byte("XX"), []byte(rec1)...),
			wantRecords: []wantRecord{
				{recordType: Warcinfo, idSuffix: "0001", sizeGt0: true, hasValidation: true},
			},
		},
		{
			name:    "garbage before record with ErrFail yields error",
			opts:    []WarcRecordOption{WithSyntaxErrorPolicy(ErrFail), WithSpecViolationPolicy(ErrFail)},
			data:    append([]byte("GARBAGE"), []byte(rec1)...),
			wantErr: "expected start of record",
		},
		{
			name:      "corrupt data with ErrWarn yields ErrNoRecord",
			data:      []byte("NOT A WARC RECORD AT ALL"),
			wantErrIs: ErrNoRecord,
		},
		{
			name:    "corrupt data with ErrFail yields error",
			opts:    []WarcRecordOption{WithSyntaxErrorPolicy(ErrFail)},
			data:    []byte("NOT A WARC RECORD AT ALL"),
			wantErr: "expected start of record",
		},
		{
			name:    "truncated record yields error with strict policy",
			opts:    []WarcRecordOption{WithSpecViolationPolicy(ErrFail), WithSyntaxErrorPolicy(ErrFail)},
			data:    []byte("WARC/1.1\r\n"),
			wantErr: "missing required field",
		},
		{
			name: "truncated record yields record with validation warnings",
			data: []byte("WARC/1.1\r\n"),
			wantRecords: []wantRecord{
				{recordType: 0, idSuffix: "", sizeGt0: true, hasValidation: true},
			},
		},
		{
			name: "record with validation warnings (bad end-of-record marker)",
			opts: []WarcRecordOption{WithSpecViolationPolicy(ErrWarn), WithSyntaxErrorPolicy(ErrWarn)},
			data: []byte(
				"WARC/1.1\r\n" +
					"WARC-Type: warcinfo\r\n" +
					"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
					"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120001>\r\n" +
					"Content-Type: application/warc-fields\r\n" +
					"Content-Length: 0\r\n" +
					"\r\n" +
					"\n\n"), // bad end-of-record marker — \n\n instead of \r\n\r\n
			wantRecords: []wantRecord{
				{recordType: Warcinfo, idSuffix: "0001", sizeGt0: true, hasValidation: true},
			},
		},
		{
			name: "garbage after valid records yields records then ErrNoRecord",
			data: append([]byte(rec1+rec2), []byte("TRAILING GARBAGE")...),
			wantRecords: []wantRecord{
				{recordType: Warcinfo, idSuffix: "0001", sizeGt0: true},
				{recordType: Warcinfo, idSuffix: "0002", sizeGt0: true},
			},
			wantErrIs: ErrNoRecord,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader, err := NewWarcFileReaderFromStream(bytes.NewReader(tt.data), 0, tt.opts...)
			require.NoError(t, err)
			defer func() { assert.NoError(t, reader.Close()) }()

			var got []Record
			var iterErr error

			for rec, err := range reader.Records() {
				if err != nil {
					iterErr = err
					break
				}
				got = append(got, rec)
			}

			if tt.wantErr != "" {
				require.Error(t, iterErr)
				assert.Contains(t, iterErr.Error(), tt.wantErr)
				return
			}

			if tt.wantErrIs != nil {
				require.Error(t, iterErr, "expected an error from iterator")
				assert.ErrorIs(t, iterErr, tt.wantErrIs)
			}

			if tt.wantNoErr {
				assert.NoError(t, iterErr, "expected no error from iterator")
			}

			require.Len(t, got, len(tt.wantRecords), "record count")

			// Validate offsets are monotonically increasing and non-overlapping
			var prevEnd int64
			for i, rec := range got {
				want := tt.wantRecords[i]

				assert.Equal(t, want.recordType, rec.WarcRecord.Type(), "record %d type", i)
				if want.idSuffix != "" {
					assert.Contains(t, rec.WarcRecord.WarcHeader().Get(WarcRecordID), want.idSuffix, "record %d ID", i)
				}

				if want.sizeGt0 {
					assert.Greater(t, rec.Size, int64(0), "record %d size", i)
				}

				if want.hasValidation {
					assert.NotEmpty(t, rec.Validation, "record %d should have validation warnings", i)
				} else {
					assert.Empty(t, rec.Validation, "record %d should have no validation warnings", i)
				}

				// Offset should be >= previous record's end
				assert.GreaterOrEqual(t, rec.Offset, prevEnd, "record %d offset >= prev end", i)
				prevEnd = rec.Offset + rec.Size

				assert.NoError(t, rec.Close())
			}

			// Consecutive records: sum of offset+size should be monotonic
			if len(got) >= 2 {
				for i := 1; i < len(got); i++ {
					prevRecEnd := got[i-1].Offset + got[i-1].Size
					assert.LessOrEqual(t, prevRecEnd, got[i].Offset,
						"record %d should start at or after record %d ends", i, i-1)
				}
			}
		})
	}
}

func TestWarcFileReader_Records_BreakEarly(t *testing.T) {
	// Verify that breaking out of the iterator mid-stream works correctly
	rec1 := minimalRecord("warcinfo", "0001")
	rec2 := minimalRecord("warcinfo", "0002")
	rec3 := minimalRecord("warcinfo", "0003")

	reader, err := NewWarcFileReaderFromStream(
		bytes.NewReader([]byte(rec1+rec2+rec3)), 0)
	require.NoError(t, err)
	defer func() { assert.NoError(t, reader.Close()) }()

	var count int
	for rec, err := range reader.Records() {
		require.NoError(t, err)
		func() { assert.NoError(t, rec.Close()) }()
		count++
		if count == 2 {
			break
		}
	}
	assert.Equal(t, 2, count, "should have seen exactly 2 records before break")
}

func TestWarcFileReader_Records_ValidationWarnings(t *testing.T) {
	// Feed a stream with garbage prefix (ErrWarn) — Records() should yield
	// the record with validation populated.
	input := "XX" +
		"WARC/1.1\r\n" +
		"WARC-Type: warcinfo\r\n" +
		"WARC-Date: 2017-03-06T04:03:53Z\r\n" +
		"WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120001>\r\n" +
		"Content-Type: application/warc-fields\r\n" +
		"Content-Length: 0\r\n" +
		"\r\n" +
		"\r\n\r\n"

	reader, err := NewWarcFileReaderFromStream(
		strings.NewReader(input), 0,
		WithSyntaxErrorPolicy(ErrWarn), WithSpecViolationPolicy(ErrWarn))
	require.NoError(t, err)
	defer func() { assert.NoError(t, reader.Close()) }()

	var records []Record
	for rec, err := range reader.Records() {
		require.NoError(t, err)
		records = append(records, rec)
	}

	require.Len(t, records, 1)
	assert.NotEmpty(t, records[0].Validation, "should have validation warnings")
	// Offset should account for the skipped garbage
	assert.Equal(t, int64(2), records[0].Offset)
	func() { assert.NoError(t, records[0].Close()) }()
}

func TestWarcFileReader_Records_SizeCoversFullRecord(t *testing.T) {
	// Write two records via the writer, read them back via Records(),
	// verify that Offset + Size of record i == Offset of record i+1.
	freezeClockAndHost(t)

	dir := t.TempDir()
	ng := &PatternNameGenerator{
		Directory: dir,
		Prefix:    "iter-",
		Pattern:   "%{prefix}s%{ts}s.warc",
		Extension: "warc",
	}
	w := NewWarcFileWriter(
		WithCompression(false),
		WithFileNameGenerator(ng),
		WithMaxFileSize(0),
		WithMaxConcurrentWriters(1),
	)
	res := w.Write(createTestRecord(), createTestRecord())
	require.Len(t, res, 2)
	require.NoError(t, res[0].Err)
	require.NoError(t, res[1].Err)
	require.NoError(t, w.Close())

	files := listFiles(t, dir, `\.warc$`)
	require.Len(t, files, 1)

	reader, err := NewWarcFileReader(dir+"/"+files[0], 0)
	require.NoError(t, err)
	defer func() { assert.NoError(t, reader.Close()) }()

	var records []Record
	for rec, err := range reader.Records() {
		require.NoError(t, err)
		records = append(records, rec)
		defer func() { assert.NoError(t, rec.Close()) }()
	}

	require.Len(t, records, 2)
	assert.Equal(t, int64(0), records[0].Offset)
	assert.Greater(t, records[0].Size, int64(0))
	// Second record starts exactly where the first ends
	assert.Equal(t, records[0].Offset+records[0].Size, records[1].Offset,
		"second record offset should equal first record offset + size")
}

func TestRecord_Close_NilWarcRecord(t *testing.T) {
	// Record.Close() should be safe to call when WarcRecord is nil
	r := Record{}
	assert.NoError(t, r.Close())
}
