![Lint](https://github.com/nlnwa/gowarc/workflows/golangci-lint/badge.svg)
![GoReleaser](https://github.com/nlnwa/gowarc/workflows/goreleaser/badge.svg)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/nlnwa/gowarc)](https://pkg.go.dev/github.com/nlnwa/gowarc)

> This project is currently in alpha. Expect API changes and enhanced documentation to come.

# gowarc

A library for creating, parsing and evaluating WARC-records, written in go.

### What is WARC?

The WARC format offers a standard way to structure, manage and store billions of resources collected from the web and
elsewhere. It is used to build applications for harvesting, managing, accessing, mining and exchanging content.

To learn more about the WARC standard, read the specification
at https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/

## Library documentation

#### Installation

```
$ go get github.com/nlnwa/gowarc
```

#### Create a new WARC record

```go
package main

import (
	"fmt"
	"github.com/nlnwa/gowarc"
)

func main() {
	builder := gowarc.NewRecordBuilder(gowarc.Response)
	_, err := builder.WriteString("HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n" +
		"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n" +
		"Content-Length: 19\nConnection: close\nContent-Type: text/plain\n\nThis is the content")
	if err != nil {
		panic(err)
	}
	builder.AddWarcHeader(gowarc.WarcRecordID, "<urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>")
	builder.AddWarcHeader(gowarc.WarcDate, "2006-01-02T15:04:05Z")
	builder.AddWarcHeader(gowarc.ContentLength, "257")
	builder.AddWarcHeader(gowarc.ContentType, "application/http;msgtype=response")
	builder.AddWarcHeader(gowarc.WarcBlockDigest, "sha1:B285747AD7CC57AA74BCE2E30B453C8D1CB71BA4")

	if wr, v, err := builder.Finalize(); err == nil {
		fmt.Println(wr, v)
	}
}
```

#### Expected output

```
WARC record: version: WARC/1.1, type: response, id: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>
```

### godoc

For complete documentation and examples consult the godoc online at: https://pkg.go.dev/github.com/nlnwa/gowarc

## Command line

https://github.com/nlnwa/warchaeology is a command line tool that use gowarc.
