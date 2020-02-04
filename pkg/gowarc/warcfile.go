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

package gowarc

import (
	"bufio"
	"os"
)

type WarcFile struct {
	file           *os.File
	offset         int64
	warcReader     *WarcReader
	countingReader *Reader
	bufferedReader *bufio.Reader
	currentRecord  *WarcRecord
}

func NewWarcFilename(filename string, offset int64, opts *WarcReaderOpts) (*WarcFile, error) {
	file, err := os.Open(filename) // For read access.
	if err != nil {
		return nil, err
	}

	return NewWarcFile(file, offset, opts)
}

func NewWarcFile(file *os.File, offset int64, opts *WarcReaderOpts) (*WarcFile, error) {
	wf := &WarcFile{
		file:       file,
		offset:     offset,
		warcReader: NewWarcReader(opts),
	}
	_, err := file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	wf.countingReader = NewCountingReader(file)
	wf.countingReader.n = offset
	wf.bufferedReader = bufio.NewReaderSize(wf.countingReader, 4*1024)
	return wf, nil
}

func (wf *WarcFile) Next() (*WarcRecord, int64, error) {
	if wf.currentRecord != nil {
		wf.currentRecord.Close()
	}
	wf.offset = wf.countingReader.N() - int64(wf.bufferedReader.Buffered())
	fs, _ := wf.file.Stat()
	if fs.Size() <= wf.offset {
		wf.offset = 0
	}

	var err error
	wf.currentRecord, err = wf.warcReader.GetRecord(wf.bufferedReader)
	return wf.currentRecord, wf.offset, err
}

func (wf *WarcFile) Close() error {
	return wf.file.Close()
}
