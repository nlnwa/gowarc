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
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
	"strings"
)

type FileStorageLoader struct {
	FilePathResolver func(fileName string) (filePath string, err error)
}

func (f *FileStorageLoader) Load(storageRef string) (record *gowarc.WarcRecord, err error) {
	filePath, offset, err := f.parseStorageRef(storageRef)
	if err != nil {
		return nil, err
	}
	log.Debugf("loading record from file: %s, offset: %v\n", filePath, offset)

	opts := &gowarc.WarcReaderOpts{Strict: false}
	wf, err := gowarc.NewWarcFilename(filePath, offset, opts)
	if err != nil {
		return
	}
	defer wf.Close()

	record, _, err = wf.Next()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %v, Offset %v\n", err.Error(), offset)
		return nil, err
	}
	return
}

func (f *FileStorageLoader) parseStorageRef(storageRef string) (fileName string, offset int64, err error) {
	p := strings.SplitN(storageRef, ":", 3)
	if len(p) != 3 || p[0] != "warcfile" {
		err = fmt.Errorf("storage ref '%s' can't be handled by FileStorageLoader", storageRef)
	}
	fileName = p[1]
	offset, err = strconv.ParseInt(p[2], 0, 64)

	if f.FilePathResolver != nil {
		fileName, err = f.FilePathResolver(fileName)
	}
	return
}
