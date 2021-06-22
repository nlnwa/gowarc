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

package index

import (
	"encoding/json"
	"fmt"
	"github.com/nlnwa/gowarc"
)

type CdxWriter interface {
	Write(wr gowarc.WarcRecord, fileName string, offset int64) error
}

type CdxLegacy struct {
}
type CdxJ struct {
}

func (c *CdxLegacy) Write(wr gowarc.WarcRecord, fileName string, offset int64) error {
	return nil
}

func (c *CdxJ) Write(wr gowarc.WarcRecord, fileName string, offset int64) error {
	if wr.Type() == gowarc.Response {
		rec := NewCdxRecord(wr, fileName, offset)
		cdxj, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		fmt.Printf("%s %s %s %s\n", rec.Ssu, rec.Sts, rec.Srt, cdxj)
	}
	return nil
}
