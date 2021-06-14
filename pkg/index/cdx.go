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
	"github.com/nlnwa/gowarc"
	"strconv"

	"github.com/nlnwa/gowarc/pkg/surt"
	"github.com/nlnwa/gowarc/pkg/timestamp"
	cdx "github.com/nlnwa/gowarc/proto"
)

func NewCdxRecord(wr gowarc.WarcRecord, fileName string, offset int64) *cdx.Cdx {
	cdx := &cdx.Cdx{
		Uri: wr.WarcHeader().Get(gowarc.WarcTargetURI),
		Sha: wr.WarcHeader().Get(gowarc.WarcPayloadDigest),
		Dig: wr.WarcHeader().Get(gowarc.WarcPayloadDigest),
		Ref: "warcfile:" + fileName + "#" + strconv.FormatInt(offset, 10),
		Rid: wr.WarcHeader().Get(gowarc.WarcRecordID),
		Cle: wr.WarcHeader().Get(gowarc.ContentLength),
		//Rle: wr.WarcHeader().Get(warcrecord.ContentLength),
		Rct: wr.WarcHeader().Get(gowarc.WarcConcurrentTo),
		Rou: wr.WarcHeader().Get(gowarc.WarcRefersToTargetURI),
		Rod: wr.WarcHeader().Get(gowarc.WarcRefersToDate),
		Roi: wr.WarcHeader().Get(gowarc.WarcRefersTo),
	}
	if ssu, err := surt.SsurtString(wr.WarcHeader().Get(gowarc.WarcTargetURI), true); err == nil {
		cdx.Ssu = ssu
	}
	cdx.Sts, _ = timestamp.To14(wr.WarcHeader().Get(gowarc.WarcDate))
	cdx.Srt = wr.Type().String()

	switch v := wr.Block().(type) {
	case gowarc.HttpResponseBlock:
		cdx.Hsc = strconv.Itoa(v.HttpStatusCode())
		cdx.Mct = v.HttpHeader().Get("Content-Type")
		cdx.Ple = v.HttpHeader().Get("Content-Length")
	case *gowarc.RevisitBlock:
		if resp, err := v.Response(); err == nil {
			cdx.Hsc = strconv.Itoa(resp.StatusCode)
			cdx.Mct = resp.Header.Get("Content-Type")
			cdx.Ple = resp.Header.Get("Content-Length")
		}
	}

	return cdx
}
