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
package index

import (
	"errors"
	"fmt"
	"github.com/nlnwa/gowarc/pkg/gowarc"
	"github.com/nlnwa/gowarc/pkg/index"
	"github.com/spf13/cobra"
	"io"
	"os"
	"strconv"
)

type conf struct {
	fileName string
}

func NewCommand() *cobra.Command {
	c := &conf{}
	var cmd = &cobra.Command{
		Use:   "index",
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
			return runE(c)
		},
	}

	return cmd
}

func runE(c *conf) error {
	// TODO: make configurable
	db, err := index.NewIndexDb("/tmp/cdx")
	if err != nil {
		return err
	}
	defer db.Close()
	readFile(c, db)
	return nil
}

func readFile(c *conf, db *index.Db) {
	opts := &gowarc.WarcReaderOpts{Strict: false}
	wf, err := gowarc.NewWarcFilename(c.fileName, 0, opts)
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
		count++

		db.Add(wr.RecordID(), c.fileName, currentOffset)
	}
	fmt.Fprintln(os.Stderr, "Count: ", count)
}
