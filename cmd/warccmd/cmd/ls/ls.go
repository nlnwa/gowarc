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
package ls

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/nlnwa/gowarc/pkg/gowarc"
	"github.com/spf13/cobra"
	"io"
	"os"
	"sort"
	"strconv"
)

type conf struct {
	offset    int64
	endOffset int64
	header    bool
	strict    bool
	fileName  string
	id        []string
}

func NewCommand() *cobra.Command {
	c := &conf{}
	var cmd = &cobra.Command{
		Use:   "ls",
		Short: "A brief description of your command",
		Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return errors.New("missing file name")
			}
			c.fileName = args[0]
			sort.Strings(c.id)
			return runE(c)
		},
	}

	cmd.Flags().Int64VarP(&c.offset, "offset", "o", -1, "record offset")
	cmd.Flags().Int64VarP(&c.endOffset, "offset-end", "e", -1, "record offset")
	cmd.Flags().BoolVar(&c.header, "header", false, "show header")
	cmd.Flags().BoolVarP(&c.strict, "strict", "s", false, "strict parsing")
	cmd.Flags().StringArrayVar(&c.id, "id", []string{}, "id")

	return cmd
}

func runE(c *conf) error {
	readFile(c, c.fileName)
	return nil
}

func readFile(c *conf, fileName string) {
	var currentOffset int64 = 0
	if c.offset >= 0 {
		currentOffset = c.offset
	}
	var record *gowarc.WarcRecord
	var err error

	var nextOffset int64

	file, err := os.Open(fileName) // For read access.
	defer file.Close()
	if err != nil {
		return
	}
	fileInfo, err := file.Stat()
	if err != nil {
		return
	}
	if fileInfo.IsDir() {
		return
	}

	if c.offset >= 0 {
		file.Seek(c.offset, 0)
	}

	r := gowarc.NewCountingReader(file)
	b := bufio.NewReaderSize(r, 64*1024)

	wr := gowarc.NewWarcReader(c.strict)
	count := 0

	record, err = wr.GetRecord(b)
	if err != nil {
		panic(err)
	}

	nextOffset = c.offset + r.N() - int64(b.Buffered())

	for true {
		if c.endOffset >= 0 && currentOffset > c.endOffset {
			return
		}

		if len(c.id) == 0 || contains(c.id, record.RecordID()) {
			printRecord(currentOffset, record)
		}
		//fmt.Fprint(os.Stderr, ".")
		//fmt.Printf("%v %v %v %T\n", record.Type(), record.RecordID(), record.ContentType(), record.Block())
		//fmt.Printf("%v\n", record.Version())
		//fmt.Printf("%v\t%s\t%s\n", offset, record.Header.RecordID, record.Header.ContentType)
		//fmt.Printf("%s\t%v\t%s\t%v\n", fileName, offset, record.TargetUri(), record.Type())

		//switch t := record.Block().(type) {
		//case *gowarc.HttpRequestBlock:
		//	//fmt.Println("HEADER: ", t.HttpHeader())
		//case *gowarc.WarcFieldsBlock:
		//	//fmt.Printf("%v %T %v\n", t.WarcFields, t, record.Type())
		//}

		count++

		record, err = wr.GetRecord(b)
		if err == io.EOF {
			break
		}
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Error: %v, rec num: %v, Offset %v\n", err.Error(), strconv.Itoa(count), c.offset)
			break
		}
		currentOffset = nextOffset
		nextOffset = c.offset + r.N() - int64(b.Buffered())
	}
	fmt.Fprintln(os.Stderr, "Count: ", count)
}

func printRecord(offset int64, record *gowarc.WarcRecord) {
	fmt.Printf("%v\t%s\t%s\t%s\n", offset, record.RecordID(), record.Type(), record.TargetUri())

	exNames := record.ExtensionFieldnames()
	if len(exNames) > 0 {
		fmt.Println("--")
		for _, k := range exNames {
			fmt.Fprintln(os.Stderr, "Extensions: ", k, " = ", record.ExtensionField(k))
		}
	}
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
