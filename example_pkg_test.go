package gowarc

import (
	"fmt"
	"os"
)

var directory = "tmp-example"

var httpRecord = `HTTP/1.1 200 OK
Date: Tue, 19 Sep 2016 17:18:40 GMT
Server: Apache/2.0.54 (Ubuntu)
Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT
ETag: "3e45-67e-2ed02ec0"
Accept-Ranges: bytes
Content-Length: 19
Connection: close
Content-Type: text/plain

This is the content`

func Example() {
	if err := os.Mkdir(directory, 0755); err == nil {
		nameGenerator := &PatternNameGenerator{Directory: directory}
		w := NewWarcFileWriter(WithFileNameGenerator(nameGenerator))
		defer func() {
			w.Close()
			os.RemoveAll(directory)
		}()

		builder := NewRecordBuilder(Response)
		_, err := builder.WriteString(httpRecord)
		if err != nil {
			panic(err)
		}
		builder.AddWarcHeader(WarcRecordID, "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>")
		builder.AddWarcHeader(WarcDate, "2006-01-02T15:04:05Z")
		builder.AddWarcHeader(ContentType, "application/http;msgtype=response")

		if wr, _, err := builder.Build(); err == nil {
			res := w.Write(wr)
			fmt.Printf("%s: %s", res[0].FileName, wr)
		}
		// Output: 20010912053020-0001-example.warc.gz: WARC record: version: WARC/1.1, type: response, id: urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008
	}
}
