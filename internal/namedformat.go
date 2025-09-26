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

package internal

import (
	"fmt"
	"strconv"
	"strings"
)

// Sprintt is like fmt.Sprintf, but accepts named parameters from a map.
//
// Example:
//   params := map[string]any{
//     "hello": "world",
//     "num":   42,
//   }
//
//   result := internal.Sprintt("Hello %{hello}s. The answer is %{num}d", params)
//
// Result will then be: 'Hello world. The answer is 42'
func Sprintt(format string, params map[string]any) string {
	pos := 1
	var args []any
	for key, val := range params {
		replaced := strings.Replace(format, "{"+key+"}", "["+strconv.Itoa(pos)+"]", -1)
		if replaced != format {
			pos++
			args = append(args, val)
			format = replaced
		}
	}
	return fmt.Sprintf(format, args...)
}
