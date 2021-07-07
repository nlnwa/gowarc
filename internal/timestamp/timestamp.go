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

package timestamp

import (
	"time"
)

func To14(s string) (string, error) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return "", err
	}

	return t.Format("20060102150405"), nil
}

func From14ToTime(s string) (time.Time, error) {
	t, err := time.Parse("20060102150405", s)
	return t, err
}

func UTCNow() time.Time {
	return time.Now().In(time.UTC)
}

func UTCNow14() string {
	return time.Now().In(time.UTC).Format("20060102150405")
}

func UTCNowW3cIso8601() string {
	return time.Now().In(time.UTC).Format(time.RFC3339)
}