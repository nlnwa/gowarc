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
	"io"
	"os"
	"strconv"

	"github.com/nlnwa/gowarc/pkg/index"
	"github.com/nlnwa/gowarc/warcoptions"
	"github.com/nlnwa/gowarc/warcreader"
	"github.com/spf13/cobra"
)

type cdxFormat struct {
	name   string
	writer index.CdxWriter
}

func (c *cdxFormat) String() string {
	return "cdx"
}
func (c *cdxFormat) Set(name string) error {
	switch name {
	case "cdx":
		c.writer = &index.CdxLegacy{}
	case "cdxj":
		c.writer = &index.CdxJ{}
	case "cdxpb":
		c.writer = &index.CdxPb{}
	case "db":
		c.writer = &index.CdxDb{}
	default:
		return fmt.Errorf("unknwon format %v", name)
	}
	c.name = name
	return nil
}
func (c *cdxFormat) Type() string {
	return "cdxFormat"
}

type conf struct {
	fileName string
	format   cdxFormat
	writer   index.CdxWriter
}

func NewCommand() *cobra.Command {
	c := &conf{}
	var cmd = &cobra.Command{
		Use:   "index",
		Short: "Index a given warc file",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return errors.New("missing file name")
			}
			c.fileName = args[0]
			c.writer = c.format.writer
			return runE(c)
		},
	}

	cmd.Flags().VarP(&c.format, "format", "f", "cdx format")

	return cmd
}

func runE(c *conf) error {
	fmt.Printf("Format: %v\n", c.format)
	err := c.writer.Init()
	if err != nil {
		return err
	}
	defer c.writer.Close()
	readFile(c)
	return nil
}

func readFile(c *conf) {
	opts := &warcoptions.WarcOptions{Strict: false}
	wf, err := warcreader.NewWarcFilename(c.fileName, 0, opts)
	if err != nil {
		return
	}
	defer wf.Close()

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

		c.writer.Write(wr, c.fileName, currentOffset)
	}
	fmt.Fprintln(os.Stderr, "Count: ", count)
}
