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
	"fmt"
	"github.com/nlnwa/warc"
	"io"
	"os"
	"strconv"
)

func main() {
	args := os.Args[1:]
	fileName := args[0]
	var offset int64
	var record *warc.WarcRecord
	var err error
	one := false
	if len(args) > 1 {
		o, _ := strconv.Atoi(args[1])
		offset = int64(o)
		one = true
	}

	file, err := os.Open(fileName) // For read access.
	defer file.Close()
	if err != nil {
		return
	}
	b := bufio.NewReaderSize(file, 64*1024)

	wr := warc.NewWarcReader(false)
	count := 0
	//record, offset, err = wr.GetRecordFile(file, offset)
	record, err = wr.GetRecord(b)
	if err != nil {
		panic(err)
	}

	if one {
		fmt.Printf("%v %v", record.Type(), record.Version())
		//record.Header.Print(os.Stdout)
		////fmt.Printf("\nPAYLOAD:\n%s\n", record.Payload)
		//switch v := record.Payload.(type) {
		//case *warcreader.HttpPayload:
		//	fmt.Println(v.GetStatus())
		//	fmt.Println(v.GetHttpHeader())
		//	fmt.Println("Body>>>")
		//	fmt.Printf("%s\n", v.GetHttpPayloadBytes())
		//	fmt.Println("Body<<<")
		//}
	} else {
		for true {
			//fmt.Printf("%v %v %v %T\n", record.Type(), record.RecordID(), record.ContentType(), record.Block())
			//fmt.Printf("%v\n", record.Version())
			//fmt.Printf("%v\t%s\t%s\n", offset, record.Header.RecordID, record.Header.ContentType)
			exNames := record.ExtensionFieldnames()
			if len(exNames) > 0 {
				fmt.Println("--")
				for _, k := range exNames {
					fmt.Println("Extensions: ", k, " = ", record.ExtensionField(k))
				}
			}
			switch t := record.Block().(type) {
			case *warc.HttpRequestBlock:
				//fmt.Println("HEADER: ", t.HttpHeader())
			case *warc.WarcFieldsBlock:
				fmt.Printf("%v %T %v\n", t.WarcFields, t, record.Type())
			}

			count++
			if offset != 0 {
				break
			}

			//record, offset, err = wr.GetRecordFile(file, offset)
			record, err = wr.GetRecord(b)
			if err == io.EOF {
				break
			}
			if err != nil {
				panic(err.Error() + " " + strconv.Itoa(count))
			}
		}
		fmt.Println("Count: ", count)
	}
}
