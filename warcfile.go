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
	"bufio"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/gzip"
	"github.com/nlnwa/gowarc/v2/internal"
	"github.com/nlnwa/gowarc/v2/internal/countingreader"
	"github.com/nlnwa/gowarc/v2/internal/timestamp"
)

// WarcFileNameGenerator is the interface that wraps the NewWarcfileName function.
type WarcFileNameGenerator interface {
	// NewWarcfileName returns a directory (might be the empty string for current directory) and a file name
	NewWarcfileName() (string, string)
}

// PatternNameGenerator implements the WarcFileNameGenerator.
//
// New filenames are generated based on a pattern which defaults to the recommendation in the WARC 1.1 standard
// (https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#annex-c-informative-warc-file-size-and-name-recommendations).
// The pattern is like golangs fmt package (https://pkg.go.dev/fmt), but allows for named fields in curly braces.
// The available predefined names are:
//   - prefix   - content of the Prefix field
//   - ext      - content of the Extension field
//   - ts       - current time as 14-digit GMT Time-stamp
//   - serial   - atomically increased serial number for every generated file name. Initial value is 0 if Serial field is not set
//   - ip       - primary IP address of the node
//   - host     - host name of the node
//   - hostOrIp - host name of the node, falling back to IP address if host name could not be resolved
type PatternNameGenerator struct {
	Directory string         // Directory to store warcfiles. Defaults to the empty string
	Prefix    string         // Prefix available to be used in pattern. Defaults to the empty string
	Serial    int32          // Serial number available for use in pattern. It is atomically increased with every generated file name.
	Pattern   string         // Pattern for generated file name. Defaults to: "%{prefix}s%{ts}s-%04{serial}d-%{hostOrIp}s.%{ext}s"
	Extension string         // Extension for file name. Defaults to: "warc"
	Params    map[string]any // Parameters available to be used in pattern. If a custom parameter has the same key as a predefined field (prefix, ext, etc), the predefined field will take precedence
}

const (
	defaultPattern   = "%{prefix}s%{ts}s-%04{serial}d-%{hostOrIp}s.%{ext}s"
	defaultExtension = "warc"
)

// Allow overriding of time.Now for tests
var now = time.Now
var ip = internal.GetOutboundIP
var host = internal.GetHostName
var hostOrIp = internal.GetHostNameOrIP

// NewWarcfileName returns a directory (might be the empty string for current directory) and a file name
func (g *PatternNameGenerator) NewWarcfileName() (string, string) {
	if g.Pattern == "" {
		g.Pattern = defaultPattern
	}
	if g.Extension == "" {
		g.Extension = defaultExtension
	}

	// Initialize parameter map with any custom parameters
	p := make(map[string]any)
	if g.Params != nil {
		for k, v := range g.Params {
			p[k] = v
		}
	}

	// Built-in parameters which take precedence over any custom parameters
	defaultParams := map[string]any{
		"ts":       timestamp.UTC14(now()),
		"serial":   atomic.AddInt32(&g.Serial, 1),
		"prefix":   g.Prefix,
		"ext":      g.Extension,
		"ip":       ip(),
		"host":     host(),
		"hostOrIp": hostOrIp(),
	}

	// Add default parameters, overriding any custom parameters with the same key
	maps.Copy(p, defaultParams)

	name := internal.Sprintt(g.Pattern, p)
	return g.Directory, name
}

type WriteResponse struct {
	FileName     string // filename
	FileOffset   int64  // the offset in file
	BytesWritten int64  // number of uncompressed bytes written
	Err          error  // eventual error
}

// WarcFileWriter writes WARC records using a pool of independent file writers.
// Each worker owns one singleWarcFileWriter and thus one "current file" at a time.
//
// Close drains queued work and stops workers. Writes after Close return nil.
// Rotate is ordered w.r.t. queued writes: each worker closes its current file
// only after it has processed all requests that were queued before Rotate.
type WarcFileWriter struct {
	opts    *warcFileWriterOptions
	workers []*singleWarcFileWriter

	reqCh  chan request
	wg     sync.WaitGroup
	once   sync.Once
	closed atomic.Bool
}

func (w *WarcFileWriter) String() string {
	return fmt.Sprintf("WarcFileWriter (%s)", w.opts)
}

func NewWarcFileWriter(opts ...WarcFileWriterOption) *WarcFileWriter {
	o := defaultwarcFileWriterOptions()
	for _, opt := range opts {
		opt.apply(&o)
	}
	if o.maxConcurrentWriters <= 0 {
		o.maxConcurrentWriters = 1
	}
	if o.gzipLevel < gzip.DefaultCompression || o.gzipLevel > gzip.BestCompression {
		panic("illegal compression level " + strconv.Itoa(o.gzipLevel) + ", must be between -1 and 9")
	}

	if o.expectedCompressionRatio <= 0 || o.expectedCompressionRatio > 1 {
		// Keep it permissive but sane; ratio is only used for estimation.
		o.expectedCompressionRatio = 0.5
	}

	w := &WarcFileWriter{
		opts:  &o,
		reqCh: make(chan request),
	}
	w.workers = make([]*singleWarcFileWriter, 0, o.maxConcurrentWriters)

	for i := 0; i < o.maxConcurrentWriters; i++ {
		sw := &singleWarcFileWriter{
			opts:  &o,
			ctlCh: make(chan func(*singleWarcFileWriter) error),
		}
		if o.compress {
			sw.gz, _ = gzip.NewWriterLevel(nil, o.gzipLevel)
		}
		w.workers = append(w.workers, sw)

		w.wg.Add(1)
		go func(sw *singleWarcFileWriter) {
			defer func() {
				_ = sw.Close()
				w.wg.Done()
			}()
			for {
				select {
				case req, ok := <-w.reqCh:
					if !ok {
						return
					}
					res := make([]WriteResponse, len(req.records))
					for i, r := range req.records {
						res[i] = sw.Write(r)
					}
					req.writeCh <- res
				case ctlFn, ok := <-sw.ctlCh:
					if !ok {
						return
					}
					ctlFn(sw)
				}
			}
		}(sw)
	}

	return w
}

type request struct {
	records []WarcRecord
	writeCh chan []WriteResponse
}

// Write marshals one or more WarcRecords to file.
// If addConcurrentHeader is enabled, records in the same call cross-reference each other.
//
// Returns nil if writer is closed.
func (w *WarcFileWriter) Write(records ...WarcRecord) []WriteResponse {
	if w.closed.Load() {
		return nil
	}

	if w.opts.addConcurrentHeader {
		addConcurrentToHeaders(records)
	}

	respCh := make(chan []WriteResponse, 1)
	req := request{records: records, writeCh: respCh}

	if !w.trySend(req) {
		return nil
	}
	return <-respCh
}

func addConcurrentToHeaders(records []WarcRecord) {
	for i, wr := range records {
		for j, wr2 := range records {
			if i == j {
				continue
			}
			wr.WarcHeader().AddId(WarcConcurrentTo, wr2.WarcHeader().GetId(WarcRecordID))
		}
	}
}

// Rotate closes the current file of each worker, ordered after all previously queued requests.
func (w *WarcFileWriter) Rotate() error {
	if w.closed.Load() {
		return errors.New("warc writer is closed")
	}

	// Send close command to each worker's control channel and wait for acknowledgment.
	// This ensures each worker closes its own file exactly once.
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error

	for _, worker := range w.workers {
		wg.Add(1)
		go func(sw *singleWarcFileWriter) {
			defer wg.Done()
			done := make(chan error, 1)

			// Send to worker's control channel with acknowledgment
			select {
			case sw.ctlCh <- func(w *singleWarcFileWriter) error {
				err := w.Close()
				done <- err
				return err
			}:
				// Wait for the worker to complete the close operation
				select {
				case err := <-done:
					if err != nil {
						mu.Lock()
						errs = append(errs, err)
						mu.Unlock()
					}
				case <-time.After(5 * time.Second):
					mu.Lock()
					errs = append(errs, fmt.Errorf("timeout waiting for worker to close file"))
					mu.Unlock()
				}
			case <-time.After(5 * time.Second):
				mu.Lock()
				errs = append(errs, fmt.Errorf("timeout sending rotate to worker"))
				mu.Unlock()
			}
		}(worker)
	}

	wg.Wait()

	if len(errs) > 0 {
		return fmt.Errorf("rotate error: %w", errors.Join(errs...))
	}
	return nil
}

// Close drains queued work and stops workers.
func (w *WarcFileWriter) Close() error {
	w.once.Do(func() {
		w.closed.Store(true)
		close(w.reqCh)
	})
	w.wg.Wait()
	return nil
}

func (w *WarcFileWriter) trySend(req request) (ok bool) {
	if w.closed.Load() {
		return false
	}
	defer func() {
		if recover() != nil {
			ok = false // send on closed channel
		}
	}()
	w.reqCh <- req
	return true
}

// singleWarcFileWriter is owned by exactly one worker goroutine.
type singleWarcFileWriter struct {
	opts *warcFileWriterOptions

	fileName   string
	file       *os.File
	fileSize   int64 // bytes on disk (compressed if gzip is enabled)
	warcInfoID string

	gz *gzip.Writer        // reused gzip writer, if opts.compress
	cw *countingFileWriter // reused counting writer

	ctlCh chan func(*singleWarcFileWriter) error // per-worker control channel
}

func (w *singleWarcFileWriter) Write(record WarcRecord) (resp WriteResponse) {
	// Ensure record is closed.
	defer func() { _ = record.Close() }()

	// Best-effort rotate if it likely won't fit.
	if w.file != nil && w.opts.maxFileSize > 0 && w.wouldExceedMax(record) {
		if err := w.close(); err != nil {
			resp.Err = err
			return resp
		}
	}

	if w.file == nil {
		if err := w.createFile(); err != nil {
			resp.Err = err
			return resp
		}
	}

	resp.FileName = w.fileName
	resp.FileOffset = w.fileSize

	n, err := w.writeOne(record, w.maxRecordSize())
	resp.BytesWritten = n
	resp.Err = err
	return resp
}

func (w *singleWarcFileWriter) maxRecordSize() int64 {
	if !w.opts.useSegmentation || w.opts.maxFileSize <= 0 {
		return 0
	}
	if w.opts.compress {
		return int64(float64(w.opts.maxFileSize) / w.opts.expectedCompressionRatio)
	}
	return w.opts.maxFileSize
}

func (w *singleWarcFileWriter) wouldExceedMax(record WarcRecord) bool {
	rawLen, ok := contentLength(record)
	if !ok {
		return false
	}
	est := rawLen
	if w.opts.compress {
		est = int64(float64(rawLen) * w.opts.expectedCompressionRatio)
	}
	return w.fileSize > 0 && (w.fileSize+est) > w.opts.maxFileSize
}

func contentLength(r WarcRecord) (int64, bool) {
	s := r.WarcHeader().Get(ContentLength)
	if s == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

func (w *singleWarcFileWriter) createFile() error {
	dir, base := w.opts.nameGenerator.NewWarcfileName()

	suffix := ""
	if w.opts.compress {
		suffix = w.opts.compressSuffix
	}

	finalName := base + suffix
	tmpName := finalName + w.opts.openFileSuffix

	path := tmpName
	if dir != "" {
		path = filepath.Join(dir, tmpName)
	}

	if hook := w.opts.beforeFileCreationHook; hook != nil {
		_ = hook(path)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o666)
	if err != nil {
		return err
	}

	w.file = f
	w.fileName = finalName
	w.fileSize = 0
	w.warcInfoID = ""

	if w.opts.warcInfoFunc != nil {
		if _, err := w.createWarcInfo(finalName); err != nil {
			_ = w.close()
			return err
		}
	}

	return nil
}

type countingFileWriter struct {
	f *os.File
	n int64
}

func (c *countingFileWriter) Write(p []byte) (int, error) {
	n, err := c.f.Write(p)
	c.n += int64(n)
	return n, err
}

func (c *countingFileWriter) Reset(f *os.File) { c.f = f; c.n = 0 }

func (w *singleWarcFileWriter) writeOne(record WarcRecord, maxRecordSize int64) (uncompressed int64, err error) {
	// Ensure records in this file reference the current warcinfo.
	if w.warcInfoID != "" {
		record.WarcHeader().SetId(WarcWarcinfoID, w.warcInfoID)
	}

	if w.cw == nil {
		w.cw = &countingFileWriter{f: w.file}
	} else {
		w.cw.Reset(w.file)
	}
	var out io.Writer = w.cw

	if w.opts.compress {
		if w.gz == nil {
			w.gz, err = gzip.NewWriterLevel(nil, w.opts.gzipLevel)
			if err != nil {
				return 0, err
			}
		}
		w.gz.Reset(out)
		out = w.gz
	}

	next, size, err := w.opts.marshaler.Marshal(out, record, maxRecordSize)
	uncompressed = size
	if err != nil {
		if w.opts.compress {
			_ = w.gz.Close()
		}
		return uncompressed, err
	}
	if next != nil {
		_ = next.Close()
		if w.opts.compress {
			_ = w.gz.Close()
		}
		return uncompressed, fmt.Errorf("marshaler returned continuation record but segmentation is not supported")
	}

	// Close gzip writer to flush all data.
	if w.opts.compress {
		if cerr := w.gz.Close(); cerr != nil {
			return uncompressed, cerr
		}
	}

	if w.opts.flush {
		if err := w.file.Sync(); err != nil {
			return uncompressed, err
		}
	}

	w.fileSize += w.cw.n
	return uncompressed, nil
}

func (w *singleWarcFileWriter) createWarcInfo(fileName string) (int64, error) {
	r := NewRecordBuilder(Warcinfo, w.opts.recordOptions...)
	r.AddWarcHeaderTime(WarcDate, now())
	r.AddWarcHeader(WarcFilename, fileName)
	r.AddWarcHeader(ContentType, ApplicationWarcFields)

	if err := w.opts.warcInfoFunc(r); err != nil {
		return 0, err
	}

	warcinfo, _, err := r.Build()
	if err != nil {
		return 0, err
	}
	defer warcinfo.Close()

	w.warcInfoID = "" // don't self-reference
	n, err := w.writeOne(warcinfo, 0)
	if err != nil {
		return n, err
	}

	w.warcInfoID = warcinfo.WarcHeader().GetId(WarcRecordID)
	return n, nil
}

// Close closes the current file. Next Write creates a new file.
func (w *singleWarcFileWriter) Close() error {
	return w.close()
}

func (w *singleWarcFileWriter) close() error {
	if w.file == nil {
		return nil
	}

	f := w.file
	tmpPath := f.Name()

	// snapshot values for hook
	size := w.fileSize
	warcInfoID := w.warcInfoID

	// reset state early (idempotent even if errors later)
	w.file = nil
	w.fileName = ""
	w.fileSize = 0
	w.warcInfoID = ""

	if err := f.Close(); err != nil {
		return fmt.Errorf("close %s: %w", tmpPath, err)
	}

	finalPath := strings.TrimSuffix(tmpPath, w.opts.openFileSuffix)
	if err := rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("rename %s -> %s: %w", tmpPath, finalPath, err)
	}

	if hook := w.opts.afterFileCreationHook; hook != nil {
		_ = hook(finalPath, size, warcInfoID)
	}

	return nil
}

// rename renames a file and fsyncs the parent directory to persist the change.
func rename(from, to string) error {
	if err := os.Rename(from, to); err != nil {
		return err
	}

	// A directory entry changed due to rename; fsync parent dir to persist rename.
	pdir, err := os.Open(filepath.Dir(to))
	if err != nil {
		return err
	}

	if err = pdir.Sync(); err != nil {
		_ = pdir.Close()
		return err
	}
	return pdir.Close()
}

// WarcFileReader is used to read WARC files.
// Use [NewWarcFileReader] to create a new instance.
type WarcFileReader struct {
	file           io.Reader
	initialOffset  int64
	warcReader     Unmarshaler
	countingReader *countingreader.Reader
	bufferedReader *bufio.Reader
}

var inputBufPool = sync.Pool{
	New: func() any {
		return bufio.NewReaderSize(nil, 1024*1024)
	},
}

// NewWarcFileReader creates a new [WarcFileReader] from the supplied filename.
// If offset is > 0, the reader will start reading from that offset.
// The WarcFileReader can be configured with options. See [WarcRecordOption].
func NewWarcFileReader(filename string, offset int64, opts ...WarcRecordOption) (*WarcFileReader, error) {
	info, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return nil, errors.New("is directory")
	}

	file, err := os.Open(filename) // For read access.
	if err != nil {
		return nil, err
	}

	return NewWarcFileReaderFromStream(file, offset, opts...)
}

// NewWarcFileReaderFromStream creates a new [WarcFileReader] from the supplied io.Reader.
// The WarcFileReader can be configured with options. See [WarcRecordOption].
//
// It is the responsibility of the caller to close the io.Reader.
func NewWarcFileReaderFromStream(r io.Reader, offset int64, opts ...WarcRecordOption) (*WarcFileReader, error) {
	if s, ok := r.(io.Seeker); ok {
		_, err := s.Seek(offset, 0)
		if err != nil {
			return nil, err
		}
	}

	wf := &WarcFileReader{
		file:           r,
		initialOffset:  offset,
		warcReader:     NewUnmarshaler(opts...),
		countingReader: countingreader.New(r),
	}

	buf := inputBufPool.Get().(*bufio.Reader)
	buf.Reset(wf.countingReader)
	wf.bufferedReader = buf
	return wf, nil
}

// Next reads the next WarcRecord from the WarcFileReader.
// The method also provides the offset at which the record is found within the file.
//
// The returned values depend on the [ErrorPolicy] options set on the WarcFileReader:
//
//   - [ErrIgnore]: errors are suppressed. A [WarcRecord] and its offset are returned without
//     any validation. An error is only returned if the file is so badly formatted that nothing
//     meaningful can be parsed.
//
//   - [ErrWarn]: a [WarcRecord] and its offset are returned. Non-fatal validation findings
//     are collected in the validation slice, which should be inspected by the caller.
//
//   - [ErrFail]: the first validation failure is returned as err, and record may be nil.
//
//   - Mixed Policies: different [ErrorPolicy] values may be set per error category with
//     [WithSyntaxErrorPolicy], [WithSpecViolationPolicy] and [WithUnknownRecordTypePolicy].
//     The return values of Next are a mix of the above based on the configured policies.
//
// When at end of file, the returned offset equals the file length, record is nil
// and err is [io.EOF].
func (wf *WarcFileReader) Next() (record WarcRecord, offset int64, validation []error, err error) {
	offset = wf.initialOffset + wf.countingReader.N() - int64(wf.bufferedReader.Buffered())

	var recordOffset int64
	record, recordOffset, validation, err = wf.warcReader.Unmarshal(wf.bufferedReader)

	return record, offset + recordOffset, validation, err
}

// Close closes the WarcFileReader.
func (wf *WarcFileReader) Close() error {
	inputBufPool.Put(wf.bufferedReader)
	if wf.file != nil {
		if c, ok := wf.file.(io.Closer); ok {
			return c.Close()
		}
	}
	return nil
}

// Options for Warc file writer
type warcFileWriterOptions struct {
	maxFileSize              int64
	compress                 bool
	gzipLevel                int
	expectedCompressionRatio float64
	useSegmentation          bool
	compressSuffix           string
	openFileSuffix           string
	nameGenerator            WarcFileNameGenerator
	marshaler                Marshaler
	maxConcurrentWriters     int
	warcInfoFunc             func(recordBuilder WarcRecordBuilder) error
	addConcurrentHeader      bool
	flush                    bool
	beforeFileCreationHook   func(fileName string) error
	afterFileCreationHook    func(fileName string, size int64, warcInfoId string) error
	recordOptions            []WarcRecordOption
}

func (w *warcFileWriterOptions) String() string {
	return fmt.Sprintf("File size: %d, Compressed: %v, Num writers: %d", w.maxFileSize, w.compress, w.maxConcurrentWriters)
}

// WarcFileWriterOption configures how to write WARC files.
type WarcFileWriterOption func(*warcFileWriterOptions)

func (f WarcFileWriterOption) apply(o *warcFileWriterOptions) { f(o) }

func defaultwarcFileWriterOptions() warcFileWriterOptions {
	return warcFileWriterOptions{
		maxFileSize:              1024 * 1024 * 1024, // 1 GiB
		compress:                 true,
		gzipLevel:                gzip.DefaultCompression,
		expectedCompressionRatio: .5,
		useSegmentation:          false,
		compressSuffix:           ".gz",
		openFileSuffix:           ".open",
		nameGenerator:            &PatternNameGenerator{},
		marshaler:                &defaultMarshaler{},
		maxConcurrentWriters:     1,
		addConcurrentHeader:      false,
		recordOptions:            []WarcRecordOption{},
	}
}

// WithMaxFileSize sets the max size of the Warc file before creating a new one.
//
// defaults to 1 GiB
func WithMaxFileSize(size int64) WarcFileWriterOption {
	return func(o *warcFileWriterOptions) {
		o.maxFileSize = size
	}
}

// WithCompression sets if writer should write gzip compressed WARC files.
//
// defaults to true
func WithCompression(compress bool) WarcFileWriterOption {
	return func(o *warcFileWriterOptions) {
		o.compress = compress
	}
}

// WithCompressionLevel sets the gzip level (1-9) to use for compression.
//
// defaults to 5
func WithCompressionLevel(gzipLevel int) WarcFileWriterOption {
	return func(o *warcFileWriterOptions) {
		o.gzipLevel = gzipLevel
	}
}

// WithFlush sets if writer should commit each record to stable storage.
//
// defaults to false
func WithFlush(flush bool) WarcFileWriterOption {
	return func(o *warcFileWriterOptions) {
		o.flush = flush
	}
}

// WithSegmentation sets if writer should use segmentation for large WARC records.
//
// defaults to false
func WithSegmentation() WarcFileWriterOption {
	return func(o *warcFileWriterOptions) {
		o.useSegmentation = true
	}
}

// WithCompressedFileSuffix sets a suffix to be added after the name generated by the WarcFileNameGenerator id compression is on.
//
// defaults to ".gz"
func WithCompressedFileSuffix(suffix string) WarcFileWriterOption {
	return func(o *warcFileWriterOptions) {
		o.compressSuffix = suffix
	}
}

// WithOpenFileSuffix sets a suffix to be added to the file name while the file is open for writing.
//
// The suffix is automatically removed when the file is closed.
//
// defaults to ".open"
func WithOpenFileSuffix(suffix string) WarcFileWriterOption {
	return func(o *warcFileWriterOptions) {
		o.openFileSuffix = suffix
	}
}

// WithFileNameGenerator sets the WarcFileNameGenerator to use for generating new Warc file names.
//
// Default is to use a [PatternNameGenerator] with the default pattern.
func WithFileNameGenerator(generator WarcFileNameGenerator) WarcFileWriterOption {
	return func(o *warcFileWriterOptions) {
		o.nameGenerator = generator
	}
}

// WithMarshaler sets the Warc record marshaler to use.
//
// defaults to defaultMarshaler
func WithMarshaler(marshaler Marshaler) WarcFileWriterOption {
	return func(o *warcFileWriterOptions) {
		o.marshaler = marshaler
	}
}

// WithMaxConcurrentWriters sets the maximum number of Warc files that can be written simultaneously.
//
// defaults to one
func WithMaxConcurrentWriters(count int) WarcFileWriterOption {
	return func(o *warcFileWriterOptions) {
		o.maxConcurrentWriters = count
	}
}

// WithExpectedCompressionRatio sets the expectd reduction in size when using compression.
//
// This value is used to decide if a record will fit into a Warcfile's MaxFileSize when using compression
// since it's not possible to know this before the record is written. If the value is far from the actual size reduction,
// an under- or overfilled file might be the result.
//
// defaults to .5 (half the uncompressed size)
func WithExpectedCompressionRatio(ratio float64) WarcFileWriterOption {
	return func(o *warcFileWriterOptions) {
		o.expectedCompressionRatio = ratio
	}
}

// WithWarcInfoFunc sets a warcinfo-record generator function to be called for every new WARC-file created.
//
// The function receives a [WarcRecordBuilder] which is prepopulated with WARC-Record-ID, WARC-Type, WARC-Date and Content-Type.
// After the submitted function returns, Content-Length and WARC-Block-Digest fields are calculated.
//
// When this option is set, records written to the warcfile will have the WARC-Warcinfo-ID automatically set to point
// to the generated warcinfo record.
//
// Use [WithRecordOptions] to modify the options used to create the WarcInfo record.
//
// defaults nil (no generation of warcinfo record)
func WithWarcInfoFunc(f func(recordBuilder WarcRecordBuilder) error) WarcFileWriterOption {
	return func(o *warcFileWriterOptions) {
		o.warcInfoFunc = f
	}
}

// WithAddWarcConcurrentToHeader configures if records written in the same call to Write should have WARC-Concurrent-To
// headers added for cross-reference.
//
// default false
func WithAddWarcConcurrentToHeader(addConcurrentHeader bool) WarcFileWriterOption {
	return func(o *warcFileWriterOptions) {
		o.addConcurrentHeader = addConcurrentHeader
	}
}

// WithRecordOptions sets the options to use for creating WarcInfo records.
//
// See WithWarcInfoFunc
func WithRecordOptions(opts ...WarcRecordOption) WarcFileWriterOption {
	return func(o *warcFileWriterOptions) {
		o.recordOptions = opts
	}
}

// WithBeforeFileCreationHook sets a function to be called before a new file is created.
//
// The function receives the file name of the new file.
func WithBeforeFileCreationHook(f func(fileName string) error) WarcFileWriterOption {
	return func(o *warcFileWriterOptions) {
		o.beforeFileCreationHook = f
	}
}

// WithAfterFileCreationHook sets a function to be called after a new file is created.
//
// The function receives the file name of the new file, the size of the file and the WARC-Warcinfo-ID.
func WithAfterFileCreationHook(f func(fileName string, size int64, warcInfoId string) error) WarcFileWriterOption {
	return func(o *warcFileWriterOptions) {
		o.afterFileCreationHook = f
	}
}
