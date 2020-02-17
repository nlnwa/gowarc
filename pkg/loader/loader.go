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
	"github.com/nlnwa/gowarc/pkg/gowarc"
	log "github.com/sirupsen/logrus"
)

type StorageRefResolver interface {
	Resolve(warcId string) (storageRef string, err error)
}

type StorageLoader interface {
	Load(storageRef string) (record *gowarc.WarcRecord, err error)
}

type Loader struct {
	Resolver StorageRefResolver
	Loader   StorageLoader
	NoUnpack bool
}

func (l *Loader) Get(warcId string) (record *gowarc.WarcRecord, err error) {
	storageRef, err := l.Resolver.Resolve(warcId)
	if err != nil {
		return
	}
	record, err = l.Loader.Load(storageRef)
	if err != nil {
		return
	}

	if l.NoUnpack {
		return
	}

	// TODO: Unpack revisits and continuation
	if record.RecordType == gowarc.REVISIT {
		log.Infof("resolving revisit  %v -> %v", record.RecordID(), record.RefersTo())
		storageRef, err = l.Resolver.Resolve(record.RefersTo())
		if err != nil {
			return
		}
		var revisitOf *gowarc.WarcRecord
		revisitOf, err = l.Loader.Load(storageRef)
		if err != nil {
			return
		}
		rb := record.Block().(*gowarc.RevisitBlock)
		rb.Merge(revisitOf)
		return record, nil
	}

	return
}