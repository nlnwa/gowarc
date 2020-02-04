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
package cat

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/nlnwa/gowarc/pkg/gowarc"
	"io"
	"os"
	"sort"
	"strconv"

	"github.com/spf13/cobra"
)

type conf struct {
	offset      int64
	recordCount int
	header      bool
	strict      bool
	fileName    string
	id          []string
}

func NewCommand() *cobra.Command {
	c := &conf{}
	var cmd = &cobra.Command{
		Use:   "cat",
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
			if c.offset >= 0 && c.recordCount == 0 {
				c.recordCount = 1
			}
			if c.offset < 0 {
				c.offset = 0
			}
			sort.Strings(c.id)
			return runE(c)
		},
	}

	cmd.Flags().Int64VarP(&c.offset, "offset", "o", -1, "record offset")
	cmd.Flags().IntVarP(&c.recordCount, "record-count", "c", 0, "The maximum number of records to show")
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
	opts := &gowarc.WarcReaderOpts{Strict: c.strict}
	wf, err := gowarc.NewWarcFilename(fileName, c.offset, opts)
	defer wf.Close()
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		return
	}

	count := 0

	for {
		wr, currentOffset, err := wf.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Error: %v, rec num: %v, Offset %v\n", err.Error(), strconv.Itoa(count), c.offset)
			break
		}
		count++

		printRecord(currentOffset, wr)

		if c.recordCount > 0 && count >= c.recordCount {
			break
		}
	}
	fmt.Fprintln(os.Stderr, "Count: ", count)
}

func printRecord(offset int64, record *gowarc.WarcRecord) {
	fmt.Printf("%v\t%s\t%s\t%s\n", offset, record.RecordID(), record.Type(), record.TargetUri())
	fmt.Printf("%v\n", record)

	f := record.GF()
	fmt.Printf("%v\n", f)
	for k, v := range f {
		fmt.Printf("K: %v, V: %v\n", k, v)
	}
	//if len(exNames) > 0 {
	//	fmt.Println("--")
	//	for _, k := range exNames {
	//		fmt.Fprintln(os.Stderr, "Extensions: ", k, " = ", record.ExtensionField(k))
	//	}
	//}

	exNames := record.ExtensionFieldnames()
	if len(exNames) > 0 {
		fmt.Println("--")
		for _, k := range exNames {
			fmt.Fprintln(os.Stderr, "Extensions: ", k, " = ", record.ExtensionField(k))
		}
	}

	b := record.Block()
	switch v := b.(type) {
	case gowarc.HttpResponseBlock:
		fmt.Printf("????????????? %T\n", v)
		buf := &bytes.Buffer{}
		v.RawBytes().WriteTo(buf)
		//fmt.Printf("\n%s\n", buf.String())
		//x, err := v.Response()
		//fmt.Printf("????????????? %v -- %v\n", x, err)
	case gowarc.HttpRequestBlock:
		fmt.Printf("????????????? %T\n", v)
		//x, err := v.Response()
		//fmt.Printf("????????????? %v -- %v\n", x, err)
		//fmt.Printf("%v\n%v\n", v.Status(), v.HttpHeader())

		buf := &bytes.Buffer{}
		v.RawBytes().WriteTo(buf)
		fmt.Printf("\n-----------\n%s\n------------------\n", buf.String())
		//fmt.Printf("\n%v\n", v.RawBytes())
	default:
		fmt.Printf("%T\n", v)
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
