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

package warcserver

import (
	"fmt"
	"github.com/dgraph-io/badger/v2"
	"github.com/nlnwa/gowarc/pkg/timestamp"
	"sort"
	"strings"
)

type sorter struct {
	cdxApi  *cdxServerApi
	reverse bool
	closest string
	values  [][]interface{}
}

func (c *cdxServerApi) parseSort(sort, closest, matchType string) (*sorter, error) {
	s := &sorter{cdxApi: c}
	switch sort {
	case "reverse":
		s.reverse = true
	case "closest":
		if closest == "" || matchType != "exact" {
			return nil, fmt.Errorf("sort=closest requires a closest parameter and matchType=exact")
		}
		s.closest = closest
	}
	return s, nil
}

func (s *sorter) add(item *badger.Item) (stopIteration bool) {
	k := item.Key()
	if !s.cdxApi.dateRange.eval(k) {
		return false
	}

	ts := timestamp.From14ToTime(strings.Split(string(item.Key()), " ")[1]).Unix()
	v := []interface{}{ts, item}
	s.values = append(s.values, v)
	fmt.Printf("ADD: %s\n", item.Key())

	return false
}

func (s *sorter) write() error {
	s.sort()

	for _, i := range s.values {
		if s.cdxApi.writeItem(i[1].(*badger.Item)) {
			break
		}
	}
	return nil
}

func (s *sorter) sort() {
	closestTs := timestamp.From14ToTime(s.closest).Unix()
	sort.Slice(s.values, func(i, j int) bool {
		ts1 := s.values[i][0].(int64)
		ts2 := s.values[j][0].(int64)
		return AbsInt64(closestTs-ts1) < AbsInt64(closestTs-ts2)
	})
}

func AbsInt64(n int64) int64 {
	y := n >> 63
	return (n ^ y) - y
}
