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

package gowarc

import (
	"strconv"
	"strings"
)

// Validation contain validation results.
type Validation []error

func (v *Validation) String() string {
	if len(*v) == 0 {
		return ""
	}

	sb := strings.Builder{}
	sb.WriteString("gowarc: Validation errors:\n")
	for i, e := range *v {
		sb.WriteString("  ")
		sb.WriteString(strconv.Itoa(i + 1))
		sb.WriteString(": ")
		sb.WriteString(e.Error())
		sb.WriteByte('\n')
	}
	return sb.String()
}

func (v *Validation) AddError(err error) {
	*v = append(*v, err)
}

type position struct {
	lineNumber int
}

func (p *position) incrLineNumber() *position {
	p.lineNumber++
	return p
}
