/*
 * Copyright 2020 National Library of Norway.
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

package loader

import (
	"fmt"
	"github.com/nlnwa/gowarc/pkg/gowarc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Loader struct {
	StorageRefResolver func(warcId string) (storageRef string, err error)
	StorageLoader      func(storageRef string) (record *gowarc.WarcRecord, err error)
	NoUnpack           bool
}

func (l *Loader) Get(warcId string) (record *gowarc.WarcRecord, err error) {
	storageRef, err := l.StorageRefResolver(warcId)
	if err != nil {
		return
	}
	record, err = l.StorageLoader(storageRef)
	if err != nil {
		return
	}

	if l.NoUnpack {
		return
	}

	// TODO: Unpack revisits and continuation
	return
}

func FileStorageLoader(storageRef string) (record *gowarc.WarcRecord, err error) {
	p := strings.SplitN(storageRef, ":", 3)
	if len(p) != 3 || p[0] != "warcfile" {
		err = fmt.Errorf("storage ref '%s' can't be handled by FileStorageLoader", storageRef)
	}
	fmt.Printf("!!!!!!!!!!!! %v %v\n", p, storageRef)

	filename := p[1]
	x, y := filepath.Abs(filename)
	fmt.Printf("!!!!!!!!!!!! %v %v\n", x, y)

	offset, err := strconv.ParseInt(p[2], 0, 64)
	fmt.Printf("File: %s, Offset: %v\n", filename, offset)

	opts := &gowarc.WarcReaderOpts{Strict: false}
	wf, err := gowarc.NewWarcFilename(filename, offset, opts)
	if err != nil {
		return
	}
	defer wf.Close()

	var currentOffset int64
	record, currentOffset, err = wf.Next()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %v, Offset %v\n", err.Error(), offset)
		return nil, err
	}

	fmt.Printf("Offset: %v\n", currentOffset)
	return
}
