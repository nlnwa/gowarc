/*
 * Copyright 2021 National Library of Norway.
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

package timestamp_test

import (
	"testing"
	"time"

	"github.com/nlnwa/gowarc/v2/internal/timestamp"
)

type TestData struct {
	time         time.Time
	iso8601Date  string
	gowarc14Date string
	invalidDate  string
}

func createTestData() TestData {
	return TestData{
		time:         time.Date(2020, 1, 5, 10, 44, 25, 0, time.UTC),
		iso8601Date:  "2020-01-05T10:44:25Z",
		gowarc14Date: "20200105104425",
		invalidDate:  "ThisIsNotADate20200303",
	}
}

func TestUTC14(t *testing.T) {
	data := createTestData()

	if ts := timestamp.UTC14(data.time); ts != data.gowarc14Date {
		t.Errorf("UTC14() = %s, want %s", ts, data.gowarc14Date)
	}
}
