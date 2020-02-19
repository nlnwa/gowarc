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

package warcrecord

import (
	"fmt"
	"os"
	"testing"
)

func TestWarcWriter_WriteRecord(t *testing.T) {
	wf, err := NewWarcFilename("../../testdata/example.warc", 0, &WarcReaderOpts{Strict: false})
	defer wf.Close()
	if err != nil {
		return
	}

	wr, offset, err := wf.Next()
	if err != nil {
		return
	}

	fmt.Printf("offset: %v\n", offset)
	wantBytesWritten := 0
	ww := NewWarcWriter(&WarcWriterOpts{})
	gotBytesWritten, err := ww.WriteRecord(os.Stdout, wr)

	if err != nil {
		t.Errorf("WriteRecord() error = %v", err)
		return
	}
	if gotBytesWritten != wantBytesWritten {
		t.Errorf("WriteRecord() gotBytesWritten = %v, want %v", gotBytesWritten, wantBytesWritten)
	}

	//type args struct {
	//	w      *io.Writer
	//	record *WarcRecord
	//}
	//tests := []struct {
	//	name             string
	//	args             args
	//	wantBytesWritten int64
	//	wantErr          bool
	//}{
	//	// TODO: Add test cases.
	//}
	//for _, tt := range tests {
	//	t.Run(tt.name, func(t *testing.T) {
	//		ww := NewWarcWriter(nil)
	//		gotBytesWritten, err := ww.WriteRecord(tt.args.w, tt.args.record)
	//		if (err != nil) != tt.wantErr {
	//			t.Errorf("WriteRecord() error = %v, wantErr %v", err, tt.wantErr)
	//			return
	//		}
	//		if gotBytesWritten != tt.wantBytesWritten {
	//			t.Errorf("WriteRecord() gotBytesWritten = %v, want %v", gotBytesWritten, tt.wantBytesWritten)
	//		}
	//	})
	//}
}
