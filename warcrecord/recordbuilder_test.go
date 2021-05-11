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

package warcrecord

import (
	"fmt"
	"os"
	"testing"
)

func TestNewResponseRecord(t *testing.T) {
	opts := NewOptions(WithCompression(false), WithStrict(true))
	rb := NewResponseRecord(opts)
	rb.WriteString("HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n" +
		"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n" +
		"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the content")

	rb.AddWarcHeader(WarcRecordID, "jadda")
	rb.AddWarcHeader(WarcDate, "2006-01-02T15:04:05Z")
	rb.AddWarcHeader(ContentLength, "257")
	wr, err := rb.Finalize()
	fmt.Printf("Err: %v\n", err)

	//d, e := ioutil.ReadAll(rec.Block().(HttpResponseBlock).HttpHeaderBytes())
	//fmt.Printf("Data: >>>%v<<<, Err: %v\n", string(d), e)
	//d, e = ioutil.ReadAll(rec.Block().(HttpResponseBlock).PayloadBytes())
	//fmt.Printf("Data: >>>%v<<<, Err: %v\n", string(d), e)

	//resp := rec.Block().(HttpResponseBlock).HttpHeader()
	//fmt.Printf("Http header: %v\n", resp)

	w := NewWriter(opts)
	fmt.Printf(">>>>>>>>>>>>>>>>>>>>>\n")
	n, err := w.WriteRecord(os.Stdout, wr)
	fmt.Printf("<<<<<<<<<<<<<<<<<<<<<\n")
	fmt.Printf("Bytes written: %v, BlockType %T, Err: %v\n", n, wr.Block(), err)

	resp := wr.Block().(HttpResponseBlock).HttpHeader()
	fmt.Printf("Http header: %v, Err: %v\n", resp, err)

	//type args struct {
	//	opts Options
	//}
	//tests := []struct {
	//	name    string
	//	args    args
	//	want    WarcRecord
	//	wantErr bool
	//}{
	//	// TODO: Add test cases.
	//}
	//for _, tt := range tests {
	//	t.Run(tt.name, func(t *testing.T) {
	//		got, err := NewResponseRecord(tt.args.opts)
	//		if (err != nil) != tt.wantErr {
	//			t.Errorf("NewResponseRecord() error = %v, wantErr %v", err, tt.wantErr)
	//			return
	//		}
	//		if !reflect.DeepEqual(got, tt.want) {
	//			t.Errorf("NewResponseRecord() got = %v, want %v", got, tt.want)
	//		}
	//	})
	//}
}
