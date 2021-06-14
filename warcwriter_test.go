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
	"os"
	"testing"
)

func TestWarcWriter_WriteRecord(t *testing.T) {
	wf, err := NewWarcFilename("../testdata/example.warc", 0, NewOptions(WithStrict(true)))
	if err != nil {
		return
	}
	defer wf.Close()

	wr, offset, err := wf.Next()
	if err != nil {
		t.Errorf("juhu %+v", err)
		return
	}

	fmt.Printf("offset: %v %s\n", offset, wr.Block().(WarcFieldsBlock).WarcFields())
	var wantBytesWritten int64 = 488
	ww := NewWriter(NewOptions(WithCompression(false)))
	gotBytesWritten, err := ww.WriteRecord(os.Stdout, wr)

	if err != nil {
		t.Errorf("WriteRecord() error = %v", err)
		return
	}
	if gotBytesWritten != wantBytesWritten {
		t.Errorf("WriteRecord() gotBytesWritten = %v, want %v", gotBytesWritten, wantBytesWritten)
	}
}
