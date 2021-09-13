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

package gowarc

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"regexp"
	"sync"
	"testing"
	"time"
)

const (
	uncompressedRecordSize               int64 = 529
	compressedRecordSize                 int64 = 392
	uncompressedRecordWithWarcInfoIdSize int64 = 596
	compressedRecordWithWarcInfoIdSize   int64 = 428
)

func TestWarcFileWriter_Write_uncompressed(t *testing.T) {
	now = func() time.Time {
		return time.Date(2001, 9, 12, 5, 30, 20, 0, time.UTC)
	}
	assert := assert.New(t)

	testdir := "tmp-test"
	nameGenerator := &PatternNameGenerator{Prefix: "foo-", Directory: testdir}

	assert.NoError(os.Mkdir(testdir, 0755))
	w := NewWarcFileWriter(
		WithCompression(false),
		WithFileNameGenerator(nameGenerator),
		WithMaxFileSize(0),
		WithMaxConcurrentWriters(1))
	defer func() { assert.NoError(os.RemoveAll(testdir)) }()

	// Write two records sequentially
	res := w.Write(createTestRecord())
	assert.NoError(res[0].err)
	assert.Equalf(uncompressedRecordSize, res[0].bytesWritten, "Expected size from writer %d, but was %d", uncompressedRecordSize, res[0].bytesWritten)
	assert.Equalf(int64(0), res[0].fileOffset, "Expected offset from writer %d, but was %d", int64(0), res[0].fileOffset)
	assert.Regexp("^foo-\\d{14}-0001-\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}.warc$", res[0].fileName)

	res = w.Write(createTestRecord())
	assert.NoError(res[0].err)
	assert.Equalf(uncompressedRecordSize, res[0].bytesWritten, "Expected size from writer %d, but was %d", uncompressedRecordSize, res[0].bytesWritten)
	assert.Equalf(uncompressedRecordSize, res[0].fileOffset, "Expected offset from writer %d, but was %d", uncompressedRecordSize, res[0].fileOffset)
	assert.Regexp("^foo-\\d{14}-0001-\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}.warc$", res[0].fileName)

	// Close writer
	assert.NoError(w.Close())
	assert.NoError(w.Shutdown())
}

func TestWarcFileWriter_Write_compressed(t *testing.T) {
	now = func() time.Time {
		return time.Date(2001, 9, 12, 5, 30, 20, 0, time.UTC)
	}
	assert := assert.New(t)

	testdir := "tmp-test"
	nameGenerator := &PatternNameGenerator{Prefix: "foo-", Directory: testdir}

	assert.NoError(os.Mkdir(testdir, 0755))
	w := NewWarcFileWriter(
		WithCompression(true),
		WithFileNameGenerator(nameGenerator),
		WithMaxFileSize(0),
		WithMaxConcurrentWriters(1))
	defer func() { assert.NoError(os.RemoveAll(testdir)) }()

	// Write two records sequentially
	res := w.Write(createTestRecord())
	assert.NoError(res[0].err)
	assert.Equalf(uncompressedRecordSize, res[0].bytesWritten, "Expected size from writer %d, but was %d", uncompressedRecordSize, res[0].bytesWritten)
	assert.Equalf(int64(0), res[0].fileOffset, "Expected offset from writer %d, but was %d", int64(0), res[0].fileOffset)
	assert.Regexp("^foo-\\d{14}-0001-\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}.warc.gz$", res[0].fileName)

	res = w.Write(createTestRecord())
	assert.NoError(res[0].err)
	assert.Equalf(uncompressedRecordSize, res[0].bytesWritten, "Expected size from writer %d, but was %d", uncompressedRecordSize, res[0].bytesWritten)
	assert.Equalf(compressedRecordSize, res[0].fileOffset, "Expected offset from writer %d, but was %d", compressedRecordSize, res[0].fileOffset)
	assert.Regexp("^foo-\\d{14}-0001-\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}.warc.gz$", res[0].fileName)

	// Close writer
	assert.NoError(w.Close())
	assert.NoError(w.Shutdown())
}

func TestWarcFileWriter_Write_warcinfo_uncompressed(t *testing.T) {
	now = func() time.Time {
		return time.Date(2001, 9, 12, 5, 30, 20, 0, time.UTC)
	}
	assert := assert.New(t)

	testdir := "tmp-test"
	nameGenerator := &PatternNameGenerator{Prefix: "foo-", Directory: testdir, Pattern: "%{prefix}s%{ts}s-%04{serial}d-10.10.10.10.warc"}

	assert.NoError(os.Mkdir(testdir, 0755))
	w := NewWarcFileWriter(
		WithCompression(false),
		WithFileNameGenerator(nameGenerator),
		WithMaxFileSize(0),
		WithMaxConcurrentWriters(1),
		WithWarcInfoFunc(func(recordBuilder WarcRecordBuilder) error {
			recordBuilder.AddWarcHeader(WarcRecordID, "<urn:uuid:4f271dba-fdfa-4915-ab7e-3e4e1fc0791b>")
			return nil
		}))
	defer func() { assert.NoError(os.RemoveAll(testdir)) }()

	// Write two records sequentially
	uncompressedWarcinfoSize := int64(316)
	res := w.Write(createTestRecord())
	assert.NoError(res[0].err)
	assert.Equalf(uncompressedRecordWithWarcInfoIdSize, res[0].bytesWritten, "Expected size from writer %d, but was %d", uncompressedRecordWithWarcInfoIdSize, res[0].bytesWritten)
	assert.Equalf(uncompressedWarcinfoSize, res[0].fileOffset, "Expected offset from writer %d, but was %d", uncompressedWarcinfoSize, res[0].fileOffset)
	assert.Equal("foo-20010912053020-0001-10.10.10.10.warc", res[0].fileName)

	res = w.Write(createTestRecord())
	assert.NoError(res[0].err)
	assert.Equalf(uncompressedRecordWithWarcInfoIdSize, res[0].bytesWritten, "Expected size from writer %d, but was %d", uncompressedRecordWithWarcInfoIdSize, res[0].bytesWritten)
	assert.Equalf(uncompressedWarcinfoSize+uncompressedRecordWithWarcInfoIdSize, res[0].fileOffset, "Expected offset from writer %d, but was %d", uncompressedWarcinfoSize+uncompressedRecordWithWarcInfoIdSize, res[0].fileOffset)
	assert.Equal("foo-20010912053020-0001-10.10.10.10.warc", res[0].fileName)

	// Close writer
	assert.NoError(w.Close())
	assert.NoError(w.Shutdown())
}

func TestWarcFileWriter_Write_warcinfo_compressed(t *testing.T) {
	now = func() time.Time {
		return time.Date(2001, 9, 12, 5, 30, 20, 0, time.UTC)
	}
	assert := assert.New(t)

	testdir := "tmp-test"
	nameGenerator := &PatternNameGenerator{Prefix: "foo-", Directory: testdir, Pattern: "%{prefix}s%{ts}s-%04{serial}d-10.10.10.10.warc"}

	assert.NoError(os.Mkdir(testdir, 0755))
	w := NewWarcFileWriter(
		WithCompression(true),
		WithFileNameGenerator(nameGenerator),
		WithMaxFileSize(0),
		WithMaxConcurrentWriters(1),
		WithWarcInfoFunc(func(recordBuilder WarcRecordBuilder) error {
			recordBuilder.AddWarcHeader(WarcRecordID, "<urn:uuid:4f271dba-fdfa-4915-ab7e-3e4e1fc0791b>")
			return nil
		}))
	defer func() { assert.NoError(os.RemoveAll(testdir)) }()

	// Write two records sequentially
	compressedWarcinfoSize := int64(257)
	res := w.Write(createTestRecord())
	assert.NoError(res[0].err)
	assert.Equalf(uncompressedRecordWithWarcInfoIdSize, res[0].bytesWritten, "Expected size from writer %d, but was %d", uncompressedRecordWithWarcInfoIdSize, res[0].bytesWritten)
	assert.Equalf(compressedWarcinfoSize, res[0].fileOffset, "Expected offset from writer %d, but was %d", compressedWarcinfoSize, res[0].fileOffset)
	assert.Equal("foo-20010912053020-0001-10.10.10.10.warc.gz", res[0].fileName)

	res = w.Write(createTestRecord())
	assert.NoError(res[0].err)
	assert.Equalf(uncompressedRecordWithWarcInfoIdSize, res[0].bytesWritten, "Expected size from writer %d, but was %d", uncompressedRecordWithWarcInfoIdSize, res[0].bytesWritten)
	assert.Equalf(compressedWarcinfoSize+compressedRecordWithWarcInfoIdSize, res[0].fileOffset, "Expected offset from writer %d, but was %d", compressedWarcinfoSize+compressedRecordWithWarcInfoIdSize, res[0].fileOffset)
	assert.Equal("foo-20010912053020-0001-10.10.10.10.warc.gz", res[0].fileName)

	// Close writer
	assert.NoError(w.Close())
	assert.NoError(w.Shutdown())
}

func TestWarcFileWriter_Write_multi(t *testing.T) {
	now = func() time.Time {
		return time.Date(2001, 9, 12, 5, 30, 20, 0, time.UTC)
	}
	assert := assert.New(t)

	testdir := "tmp-test"
	nameGenerator := &PatternNameGenerator{Prefix: "foo-", Directory: testdir, Pattern: "%{prefix}s%{ts}s-%04{serial}d-10.10.10.10.warc"}

	assert.NoError(os.Mkdir(testdir, 0755))
	w := NewWarcFileWriter(
		WithCompression(false),
		WithFileNameGenerator(nameGenerator),
		WithMaxFileSize(0),
		WithMaxConcurrentWriters(1),
	)
	defer func() { assert.NoError(os.RemoveAll(testdir)) }()

	// Write two records in same request
	compressedWarcinfoSize := int64(257)
	rec1 := createTestRecord()
	rec2 := createTestRecord()
	res := w.Write(rec1, rec2)
	assert.Equal(2, len(res))
	assert.NoError(res[0].err)
	assert.Equalf(uncompressedRecordSize, res[0].bytesWritten, "Expected size from writer %d, but was %d", uncompressedRecordWithWarcInfoIdSize, res[0].bytesWritten)
	assert.Equalf(int64(0), res[0].fileOffset, "Expected offset from writer %d, but was %d", int64(0), res[0].fileOffset)
	assert.Equal("foo-20010912053020-0001-10.10.10.10.warc", res[0].fileName)

	assert.NoError(res[1].err)
	assert.Equalf(uncompressedRecordSize, res[1].bytesWritten, "Expected size from writer %d, but was %d", uncompressedRecordWithWarcInfoIdSize, res[1].bytesWritten)
	assert.Equalf(uncompressedRecordSize, res[1].fileOffset, "Expected offset from writer %d, but was %d", compressedWarcinfoSize+compressedRecordWithWarcInfoIdSize, res[1].fileOffset)
	assert.Equal("foo-20010912053020-0001-10.10.10.10.warc", res[1].fileName)

	// Close writer
	assert.NoError(w.Close())
	assert.NoError(w.Shutdown())
}

func TestWarcFileWriter_Write_multi_with_crossreference(t *testing.T) {
	now = func() time.Time {
		return time.Date(2001, 9, 12, 5, 30, 20, 0, time.UTC)
	}
	assert := assert.New(t)

	testdir := "tmp-test"
	nameGenerator := &PatternNameGenerator{Prefix: "foo-", Directory: testdir, Pattern: "%{prefix}s%{ts}s-%04{serial}d-10.10.10.10.warc"}

	assert.NoError(os.Mkdir(testdir, 0755))
	w := NewWarcFileWriter(
		WithCompression(false),
		WithFileNameGenerator(nameGenerator),
		WithMaxFileSize(0),
		WithMaxConcurrentWriters(1),
		WithAddWarcConcurrentToHeader(true),
	)
	defer func() { assert.NoError(os.RemoveAll(testdir)) }()

	// Write three records in same request
	compressedWarcinfoSize := int64(257)
	rec1 := createTestRecord()
	rec1.WarcHeader().Set(WarcRecordID, "<urn:uuid:aaaaaaaa-0221-11e7-adb1-0242ac120008>")
	rec2 := createTestRecord()
	rec2.WarcHeader().Set(WarcRecordID, "<urn:uuid:bbbbbbbb-0221-11e7-adb1-0242ac120008>")
	rec3 := createTestRecord()
	rec3.WarcHeader().Set(WarcRecordID, "<urn:uuid:cccccccc-0221-11e7-adb1-0242ac120008>")
	res := w.Write(rec1, rec2, rec3)
	recSize := uncompressedRecordSize + int64(2*len("WARC-Concurrent-To: <urn:uuid:cccccccc-0221-11e7-adb1-0242ac120008>\r\n"))

	assert.Equal(3, len(res))
	assert.NoError(res[0].err)
	assert.Equalf(recSize, res[0].bytesWritten, "Expected size from writer %d, but was %d", uncompressedRecordWithWarcInfoIdSize, res[0].bytesWritten)
	assert.Equalf(int64(0), res[0].fileOffset, "Expected offset from writer %d, but was %d", int64(0), res[0].fileOffset)
	assert.Equal("foo-20010912053020-0001-10.10.10.10.warc", res[0].fileName)
	assert.NotContains(rec1.WarcHeader().GetAll(WarcConcurrentTo), rec1.WarcHeader().Get(WarcRecordID))
	assert.Contains(rec1.WarcHeader().GetAll(WarcConcurrentTo), rec2.WarcHeader().Get(WarcRecordID))
	assert.Contains(rec1.WarcHeader().GetAll(WarcConcurrentTo), rec3.WarcHeader().Get(WarcRecordID))

	assert.NoError(res[1].err)
	assert.Equalf(recSize, res[1].bytesWritten, "Expected size from writer %d, but was %d", uncompressedRecordWithWarcInfoIdSize, res[1].bytesWritten)
	assert.Equalf(recSize, res[1].fileOffset, "Expected offset from writer %d, but was %d", compressedWarcinfoSize+compressedRecordWithWarcInfoIdSize, res[1].fileOffset)
	assert.Equal("foo-20010912053020-0001-10.10.10.10.warc", res[1].fileName)
	assert.NotContains(rec2.WarcHeader().GetAll(WarcConcurrentTo), rec2.WarcHeader().Get(WarcRecordID))
	assert.Contains(rec2.WarcHeader().GetAll(WarcConcurrentTo), rec1.WarcHeader().Get(WarcRecordID))
	assert.Contains(rec2.WarcHeader().GetAll(WarcConcurrentTo), rec3.WarcHeader().Get(WarcRecordID))

	assert.NoError(res[2].err)
	assert.Equalf(recSize, res[2].bytesWritten, "Expected size from writer %d, but was %d", uncompressedRecordWithWarcInfoIdSize, res[2].bytesWritten)
	assert.Equalf(recSize*2, res[2].fileOffset, "Expected offset from writer %d, but was %d", compressedWarcinfoSize+compressedRecordWithWarcInfoIdSize, res[2].fileOffset)
	assert.Equal("foo-20010912053020-0001-10.10.10.10.warc", res[2].fileName)
	assert.NotContains(rec3.WarcHeader().GetAll(WarcConcurrentTo), rec3.WarcHeader().Get(WarcRecordID))
	assert.Contains(rec3.WarcHeader().GetAll(WarcConcurrentTo), rec1.WarcHeader().Get(WarcRecordID))
	assert.Contains(rec3.WarcHeader().GetAll(WarcConcurrentTo), rec2.WarcHeader().Get(WarcRecordID))

	// Close writer
	assert.NoError(w.Close())
	assert.NoError(w.Shutdown())
}

func TestWarcFileWriter_Write(t *testing.T) {
	now = func() time.Time {
		return time.Date(2001, 9, 12, 5, 30, 20, 0, time.UTC)
	}
	type args struct {
		fileName             string
		compress             bool
		maxFileSize          int64
		maxConcurrentWriters int
	}
	type file struct {
		pattern string
		size    int64
	}
	tests := []struct {
		name             string
		numRecords       int
		writeInParallel  bool
		args             args
		wantFiles        []file
		fileCountBetween []int
		wantErr          bool
	}{
		{
			"Write uncompressed",
			2,
			false,
			args{
				fileName:             "foo.warc",
				compress:             false,
				maxConcurrentWriters: 1,
			},
			[]file{
				{pattern: "foo-20010912053020-0001-10\\.10\\.10\\.10.warc", size: uncompressedRecordSize * 2},
			},
			[]int{1, 1},
			false,
		},
		{
			"Write compressed",
			3,
			false,
			args{
				fileName:             "foo.warc",
				compress:             true,
				maxConcurrentWriters: 1,
			},
			[]file{
				{pattern: "foo-20010912053020-0001-10\\.10\\.10\\.10.warc.gz", size: compressedRecordSize * 3},
			},
			[]int{1, 1},
			false,
		},
		{
			"Limited file size Write uncompressed",
			3,
			false,
			args{
				fileName:             "foo.warc",
				compress:             false,
				maxFileSize:          1100,
				maxConcurrentWriters: 1,
			},
			[]file{
				{pattern: "foo-20010912053020-0001-10\\.10\\.10\\.10.warc", size: uncompressedRecordSize * 2},
				{pattern: "foo-20010912053020-0002-10\\.10\\.10\\.10.warc", size: uncompressedRecordSize},
			},
			[]int{2, 2},
			false,
		},
		{
			"Limited file size Write compressed",
			3,
			false,
			args{
				fileName:             "foo.warc",
				compress:             true,
				maxFileSize:          800,
				maxConcurrentWriters: 1,
			},
			[]file{
				{pattern: "foo-20010912053020-0001-10\\.10\\.10\\.10.warc.gz", size: compressedRecordSize * 2},
				{pattern: "foo-20010912053020-0002-10\\.10\\.10\\.10.warc.gz", size: compressedRecordSize},
			},
			[]int{2, 2},
			false,
		},
		{
			"Parallel/one writer, write uncompressed",
			3,
			true,
			args{
				fileName:             "foo.warc",
				compress:             false,
				maxConcurrentWriters: 1,
			},
			[]file{
				{pattern: "foo-20010912053020-0001-10\\.10\\.10\\.10.warc", size: uncompressedRecordSize * 3},
			},
			[]int{1, 1},
			false,
		},
		{
			"Parallel/one writer, write compressed",
			3,
			true,
			args{
				fileName:             "foo.warc",
				compress:             true,
				maxConcurrentWriters: 1,
			},
			[]file{
				{pattern: "foo-20010912053020-0001-10\\.10\\.10\\.10.warc.gz", size: compressedRecordSize * 3},
			},
			[]int{1, 1},
			false,
		},
		{
			"Parallel/two writers, write uncompressed",
			3,
			true,
			args{
				fileName:             "foo.warc",
				compress:             false,
				maxConcurrentWriters: 2,
			},
			[]file{
				{pattern: "foo-20010912053020-000\\d-10\\.10\\.10\\.10.warc", size: uncompressedRecordSize * 3},
			},
			[]int{2, 2},
			false,
		},
		{
			"Parallel/two writers, write compressed",
			3,
			true,
			args{
				fileName:             "foo.warc",
				compress:             true,
				maxConcurrentWriters: 2,
			},
			[]file{
				{pattern: "foo-20010912053020-000\\d-10\\.10\\.10\\.10.warc.gz", size: compressedRecordSize * 3},
			},
			[]int{2, 2},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)

			testdir := "tmp-test"
			nameGenerator := &PatternNameGenerator{Prefix: "foo-", Directory: testdir, Pattern: "%{prefix}s%{ts}s-%04{serial}d-10.10.10.10.warc"}

			assert.NoError(os.Mkdir(testdir, 0755))
			w := NewWarcFileWriter(
				WithCompression(tt.args.compress),
				WithFileNameGenerator(nameGenerator),
				WithMaxFileSize(tt.args.maxFileSize),
				WithMaxConcurrentWriters(tt.args.maxConcurrentWriters))
			defer func() { assert.NoError(os.RemoveAll(testdir)) }()

			if tt.writeInParallel {
				// Write two records in parallel
				wg := sync.WaitGroup{}
				for i := 0; i < tt.numRecords; i++ {
					wg.Add(1)
					go func() {
						writeRecord(assert, w, createTestRecord(), tt.wantErr)
						wg.Done()
					}()
				}
				wg.Wait()
			} else {
				// Write two records sequentially
				for i := 0; i < tt.numRecords; i++ {
					writeRecord(assert, w, createTestRecord(), tt.wantErr)
				}
			}

			fileCount(assert, testdir, tt.fileCountBetween)
			// Check that last file has open marker
			for i, wantFile := range tt.wantFiles {
				pattern := wantFile.pattern
				if i == len(tt.wantFiles) {
					pattern += ".open"
				}
				checkFile(assert, testdir, pattern, wantFile.size)
			}

			// Close writer
			assert.NoError(w.Close())
			// Check that last file was closed
			for _, wantFile := range tt.wantFiles {
				checkFile(assert, testdir, wantFile.pattern, wantFile.size)
			}
			assert.NoError(w.Shutdown())
		})
	}
}

func createTestRecord() WarcRecord {
	builder := NewRecordBuilder(Response, WithFixDigest(false), WithStrictValidation())
	_, err := builder.WriteString("HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n" +
		"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n" +
		"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the content\n")
	if err != nil {
		panic(err)
	}
	builder.AddWarcHeader(WarcRecordID, "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>")
	builder.AddWarcHeader(WarcDate, "2006-01-02T15:04:05Z")
	builder.AddWarcHeader(ContentLength, "258")
	builder.AddWarcHeader(ContentType, "application/http;msgtype=response")
	builder.AddWarcHeader(WarcBlockDigest, "sha1:7CBE117BFA2B22C3A02DEFF3BC04D5F912964A45")

	wr, _, err := builder.Build()
	if err != nil {
		panic(err)
	}
	return wr
}

func writeRecord(assert *assert.Assertions, w *WarcFileWriter, record WarcRecord, wantErr bool) {
	res := w.Write(record)
	if wantErr {
		assert.Error(res[0].err)
	} else {
		assert.NoError(res[0].err)
	}
	assert.Equalf(uncompressedRecordSize, res[0].bytesWritten, "Expected size from writer %d, but was %d", uncompressedRecordSize, res[0].bytesWritten)
}

func checkFile(assert *assert.Assertions, directory, pattern string, expectedSize int64) {
	d, e := os.Open(directory)
	assert.NoError(e)
	defer func() { assert.NoError(d.Close()) }()
	n, e := d.Readdirnames(0)
	assert.NoError(e)
	var totalSize int64
	for _, f := range n {
		match, err := regexp.MatchString(pattern, f)
		assert.NoError(err)
		if match {
			fi, err := os.Stat(d.Name() + "/" + f)
			assert.NoError(err)
			totalSize += fi.Size()
		}
	}
	assert.Equal(expectedSize, totalSize)
	if len(n) == 0 {
		assert.Failf("No file matching pattern", "Pattern: %s. Actual files: %v", pattern, n)
	}
}

func fileCount(assert *assert.Assertions, directory string, expected []int) {
	d, e := os.Open(directory)
	assert.NoError(e)
	defer func() { assert.NoError(d.Close()) }()
	n, e := d.Readdirnames(0)
	assert.NoError(e)
	assert.GreaterOrEqual(len(n), expected[0], fmt.Sprintf("expected number of files in '%s' to be greater than or equal to %d, but was %d", directory, expected[0], len(n)))
	assert.LessOrEqual(len(n), expected[1], fmt.Sprintf("expected number of files in '%s' to be less than or equal to %d, but was %d", directory, expected[1], len(n)))
}

func TestDefaultNameGenerator_NewWarcfileName(t *testing.T) {
	now = func() time.Time {
		return time.Date(2001, 9, 12, 5, 30, 20, 0, time.UTC)
	}
	tests := []struct {
		name        string
		generator   PatternNameGenerator
		invocations int
		wantDir     string
		wantMatch   string
	}{
		{"default", PatternNameGenerator{}, 5, "", "^20010912053020-000\\d-\\d{1,3}.\\d{1,3}.\\d{1,3}.\\d{1,3}.warc$"},
		{"prefix", PatternNameGenerator{Prefix: "foo-"}, 5, "", "^foo-20010912053020-000\\d-\\d{1,3}.\\d{1,3}.\\d{1,3}.\\d{1,3}.warc$"},
		{"dir", PatternNameGenerator{Directory: "mydir"}, 5, "mydir", "^20010912053020-000\\d-\\d{1,3}.\\d{1,3}.\\d{1,3}.\\d{1,3}.warc$"},
		{"dir+prefix", PatternNameGenerator{Prefix: "foo-", Directory: "mydir"}, 5, "mydir", "^foo-20010912053020-000\\d-\\d{1,3}.\\d{1,3}.\\d{1,3}.\\d{1,3}.warc$"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < tt.invocations; i++ {
				gotDir, gotName := tt.generator.NewWarcfileName()
				assert.Regexp(t, tt.wantDir, gotDir)
				assert.Regexp(t, tt.wantMatch, gotName)
			}
		})
	}
}
