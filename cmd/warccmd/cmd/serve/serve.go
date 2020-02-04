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
package serve

import (
	"fmt"
	"github.com/nlnwa/gowarc/pkg/gowarc"
	"github.com/nlnwa/gowarc/pkg/server"
	"github.com/spf13/cobra"
	"io"
	"os"
	"strconv"
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
		Use:   "serve",
		Short: "A brief description of your command",
		Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			//if len(args) == 0 {
			//	return errors.New("missing file name")
			//}
			//c.fileName = args[0]
			//if c.offset >= 0 && c.recordCount == 0 {
			//	c.recordCount = 1
			//}
			//if c.offset < 0 {
			//	c.offset = 0
			//}
			//sort.Strings(c.id)
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
	server.Serve()
	return nil
}

func readFile(c *conf, fileName string) {
	opts := &gowarc.WarcReaderOpts{Strict: c.strict}
	wf, err := gowarc.NewWarcFilename(fileName, c.offset, opts)
	defer wf.Close()
	if err != nil {
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
	//fmt.Printf("%v\t%s\t%s\n", offset, record.RecordID(), record.Type())
	fmt.Printf("%v\t%s\t%s \t%s\n", offset, record.RecordID(), record.Type(), cropString(record.TargetUri(), 100))

	//exNames := record.ExtensionFieldnames()
	//if len(exNames) > 0 {
	//	fmt.Println("--")
	//	for _, k := range exNames {
	//		fmt.Fprintln(os.Stderr, "Extensions: ", k, " = ", record.ExtensionField(k))
	//	}
	//}
}

func cropString(s string, size int) string {
	if len(s) > size {
		s = s[:size-3] + "..."
	}
	return s
}
