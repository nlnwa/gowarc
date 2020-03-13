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

package warcwriter

import (
	"fmt"
	"github.com/nlnwa/gowarc/warcoptions"
	"github.com/nlnwa/gowarc/warcreader"
	"os"
	"testing"
)

func TestWarcWriter_WriteRecord(t *testing.T) {
	wf, err := warcreader.NewWarcFilename("../../testdata/example.warc", 0, &warcoptions.WarcOptions{Strict: false})
	if err != nil {
		return
	}
	defer wf.Close()

	wr, offset, err := wf.Next()
	if err != nil {
		return
	}

	fmt.Printf("offset: %v\n", offset)
	var wantBytesWritten int64 = 0
	ww := NewWriter(&warcoptions.WarcOptions{})
	gotBytesWritten, err := ww.WriteRecord(os.Stdout, wr)

	if err != nil {
		t.Errorf("WriteRecord() error = %v", err)
		return
	}
	if gotBytesWritten != wantBytesWritten {
		t.Errorf("WriteRecord() gotBytesWritten = %v, want %v", gotBytesWritten, wantBytesWritten)
	}
}
