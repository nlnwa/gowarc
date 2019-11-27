/*
 * Copyright 2019 National Library of Norway.
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

package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/nlnwa/gowarc"
	"io"
	"os"
	"strconv"
)

var offset int64
var header bool

func init() {
	const (
		defaultOffset = -1
		usage         = "record offset"
	)
	flag.Int64Var(&offset, "offset", defaultOffset, usage)
	flag.Int64Var(&offset, "o", defaultOffset, usage+" (shorthand)")
	flag.BoolVar(&header, "h", false, usage+" (shorthand)")
}

func main() {
	flag.Parse()

	//args := os.Args[1:]
	args := flag.Args()
	fileName := args[0]
	//var offset int64

	//var record *gowarc.WarcRecord
	//var err error

	one := false
	//if len(args) > 1 {
	if offset >= 0 {
		//o, err := strconv.Atoi(args[1])
		//if err != nil {
		//	fmt.Println(err)
		//}
		//offset = int64(o)
		one = true
	}

	//var nextOffset int64
	//
	//file, err := os.Open(fileName) // For read access.
	//defer file.Close()
	//if err != nil {
	//	return
	//}
	//
	//if offset >= 0 {
	//	file.Seek(offset, 0)
	//}
	//
	//c := gowarc.NewCountingReader(file)
	//b := bufio.NewReaderSize(c, 64*1024)
	//
	//wr := gowarc.NewWarcReader(false)
	//count := 0
	//record, offset, err = wr.GetRecordFile(file, offset)
	//record, err = wr.GetRecord(b)
	//if err != nil {
	//	panic(err)
	//}
	//
	//nextOffset = offset + c.N() - int64(b.Buffered())

	if one {
		readOne(fileName, offset)
	} else {
		for _, f := range args {
			readFile(f)
		}
	}
}

func readOne(fileName string, offset int64) int64 {
	var record *gowarc.WarcRecord
	var err error

	file, err := os.Open(fileName) // For read access.
	defer file.Close()
	if err != nil {
		return -1
	}

	if offset >= 0 {
		file.Seek(offset, 0)
	}

	c := gowarc.NewCountingReader(file)
	b := bufio.NewReaderSize(c, 64*1024)

	wr := gowarc.NewWarcReader(false)

	record, err = wr.GetRecord(b)
	if err != nil {
		panic(err)
	}

	//fmt.Printf("%v %v\n", record.Type(), record.TargetUri())
	//record.Header.Print(os.Stdout)
	//fmt.Printf("\nPAYLOAD:\n%s\n", string(record.Block().RawBytes()))

	switch v := record.Block().(type) {
	case *gowarc.HttpResponseBlock:
		//fmt.Println(v.GetStatus())
		if header {
			fmt.Printf("%v\n\n%v %v\n", record.TargetUri(), v.Status().Proto, v.Status().Status)
			fmt.Println(v.HttpHeader().Write(os.Stdout))
		} else {
			//fmt.Println("Body>>>")
			fmt.Printf("%s\n", v.PayloadBytes())
			//fmt.Println("Body<<<")
		}
	}

	return offset + c.N() - int64(b.Buffered())
}

func readFile(fileName string) {
	fmt.Fprintln(os.Stderr, fileName)

	var record *gowarc.WarcRecord
	var err error

	var nextOffset int64

	file, err := os.Open(fileName) // For read access.
	defer file.Close()
	if err != nil {
		return
	}

	if offset >= 0 {
		file.Seek(offset, 0)
	}

	c := gowarc.NewCountingReader(file)
	b := bufio.NewReaderSize(c, 64*1024)

	wr := gowarc.NewWarcReader(false)
	count := 0

	record, err = wr.GetRecord(b)
	if err != nil {
		panic(err)
	}

	nextOffset = c.N() - int64(b.Buffered())

	for true {
		fmt.Fprint(os.Stderr, ".")
		//fmt.Printf("%v %v %v %T\n", record.Type(), record.RecordID(), record.ContentType(), record.Block())
		//fmt.Printf("%v\n", record.Version())
		//fmt.Printf("%v\t%s\t%s\n", offset, record.Header.RecordID, record.Header.ContentType)
		fmt.Printf("%s\t%v\t%s\t%v\n", fileName, offset, record.TargetUri(), record.Type())

		exNames := record.ExtensionFieldnames()
		if len(exNames) > 0 {
			fmt.Println("--")
			for _, k := range exNames {
				fmt.Fprintln(os.Stderr, "Extensions: ", k, " = ", record.ExtensionField(k))
			}
		}

		//switch t := record.Block().(type) {
		//case *gowarc.HttpRequestBlock:
		//	//fmt.Println("HEADER: ", t.HttpHeader())
		//case *gowarc.WarcFieldsBlock:
		//	//fmt.Printf("%v %T %v\n", t.WarcFields, t, record.Type())
		//}

		count++
		//if offset != 0 {
		//	break
		//}

		//record, offset, err = wr.GetRecordFile(file, offset)
		record, err = wr.GetRecord(b)
		if err == io.EOF {
			break
		}
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Error: %v, rec num: %v, Offset %v\n", err.Error(), strconv.Itoa(count), offset)
			break
		}
		offset = nextOffset
		nextOffset = c.N() - int64(b.Buffered())
	}
	fmt.Fprintln(os.Stderr, "Count: ", count)
	offset = 0
}
