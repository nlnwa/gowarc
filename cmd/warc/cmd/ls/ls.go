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
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"

	"github.com/nlnwa/gowarc/warcoptions"
	"github.com/nlnwa/gowarc/warcreader"
	"github.com/nlnwa/gowarc/warcrecord"
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
		Use:   "ls",
		Short: "List records from warc files",
		Long:  ``,
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
	opts := &warcoptions.WarcOptions{Strict: c.strict}
	wf, err := warcreader.NewWarcFilename(fileName, c.offset, opts)
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
			_, _ = fmt.Fprintf(os.Stderr, "Error: %v, rec num: %v, Offset %v\n", err.Error(), strconv.Itoa(count), currentOffset)
			break
		}
		if len(c.id) > 0 {
			if !contains(c.id, wr.WarcHeader().Get(warcrecord.WarcRecordID)) {
				continue
			}
		}
		count++

		printRecord(currentOffset, wr)

		if c.recordCount > 0 && count >= c.recordCount {
			break
		}
	}
	fmt.Fprintln(os.Stderr, "Count: ", count)
}

func printRecord(offset int64, record warcrecord.WarcRecord) {
	fmt.Printf("%v\t%s\t%s \t%s\n", offset, record.WarcHeader().Get(warcrecord.WarcRecordID), record.Type(), cropString(record.WarcHeader().Get(warcrecord.WarcTargetURI), 100))
}

func cropString(s string, size int) string {
	if len(s) > size {
		s = s[:size-3] + "..."
	}
	return s
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
