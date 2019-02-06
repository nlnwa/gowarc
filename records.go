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

package gowarc

import (
	"fmt"
	"strconv"
	"strings"
)

type Loader struct {
	StorageRefResolver func(warcId string) (storageRef string, err error)
	StorageLoader      func(storageRef string) (record *WarcRecord, err error)
	NoUnpack           bool
}

func (l *Loader) Get(warcId string) (record *WarcRecord, err error) {
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

func FileStorageLoader(storageRef string) (record *WarcRecord, err error) {
	p := strings.SplitN(storageRef, ":", 3)
	if len(p) != 3 || p[0] != "warcfile" {
		err = fmt.Errorf("storage ref '%s' can't be handled by FileStorageLoader", storageRef)
	}

	filename := p[1]
	offset, err := strconv.ParseInt(p[2], 0, 64)
	fmt.Printf("File: %s, Offset: %v\n", filename, offset)

	var n int64
	record, n, err = NewWarcReader(true).GetRecordFilename(filename, offset)
	fmt.Printf("Next offset: %v\n", n)
	return
}
