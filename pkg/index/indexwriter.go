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
	"fmt"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/nlnwa/gowarc/pkg/gowarc"
	"github.com/nlnwa/gowarc/pkg/surt"
	"github.com/nlnwa/gowarc/pkg/timestamp"
	cdx "github.com/nlnwa/gowarc/proto"
	"github.com/spf13/viper"
	"strconv"
)

type CdxWriter interface {
	Init() error
	Close()
	Write(wr *gowarc.WarcRecord, fileName string, offset int64) error
}

type CdxLegacy struct {
}
type CdxJ struct {
	jsonMarshaler *jsonpb.Marshaler
}
type CdxPb struct {
	jsonMarshaler *jsonpb.Marshaler
}
type CdxDb struct {
	db *Db
}

func (c *CdxDb) Init() (err error) {
	dbDir := viper.GetString("indexdir")
	c.db, err = NewIndexDb(dbDir)
	if err != nil {
		return err
	}
	return nil
}

func (c *CdxDb) Close() {
	c.db.Flush()
	c.db.Close()
}

func (c *CdxDb) Write(wr *gowarc.WarcRecord, fileName string, offset int64) error {
	return c.db.Add(wr, fileName, offset)
}

func (c *CdxLegacy) Init() (err error) {
	return nil
}

func (c *CdxLegacy) Close() {
}

func (c *CdxLegacy) Write(wr *gowarc.WarcRecord, fileName string, offset int64) error {
	return nil
}

func (c *CdxJ) Init() (err error) {
	c.jsonMarshaler = &jsonpb.Marshaler{}
	return nil
}

func (c *CdxJ) Close() {
}

func (c *CdxJ) Write(wr *gowarc.WarcRecord, fileName string, offset int64) error {
	if wr.RecordType == gowarc.RESPONSE {
		surtUrl, err := surt.GetSurtS(wr.TargetUri(), false)
		if err != nil {
			return err
		}
		ts := timestamp.To14(wr.Date())
		rec := &cdx.Cdx{
			Uri: wr.TargetUri(),
			Ref: "warcfile:" + fileName + "#" + strconv.Itoa(int(offset)),
		}
		cdxj, err := c.jsonMarshaler.MarshalToString(rec)
		if err != nil {
			return err
		}
		fmt.Printf("%v %v %v\n", surtUrl, ts, cdxj)
	}
	return nil
}

func (c *CdxPb) Init() (err error) {
	return nil
}

func (c *CdxPb) Close() {
}

func (c *CdxPb) Write(wr *gowarc.WarcRecord, fileName string, offset int64) error {
	if wr.RecordType == gowarc.RESPONSE {
		surtUrl, err := surt.GetSurtS(wr.TargetUri(), false)
		if err != nil {
			return err
		}
		ts := timestamp.To14(wr.Date())
		rec := &cdx.Cdx{
			Uri: wr.TargetUri(),
			Ref: "warcfile:" + fileName + "#" + strconv.Itoa(int(offset)),
		}
		cdxpb, err := proto.Marshal(rec)
		if err != nil {
			return err
		}
		fmt.Printf("%s %s %s\n", surtUrl, ts, cdxpb)
	}
	return nil
}
