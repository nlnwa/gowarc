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

/*
Package gowarc provides a framework for handling WARC files, enabling their parsing, creation, and validation.

# WARC Overview

The WARC format offers a standard way to structure, manage and store billions of resources collected from the web and elsewhere.
It is used to build applications for harvesting, managing, accessing, mining and exchanging content.

For more details, visit the WARC specification: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/

# WARC record creation

The [WarcRecordBuilder], initialized via [NewRecordBuilder], is the primary tool for creating WARC records.
By default, the WarcRecordBuilder generates a record id and calculates the 'Content-Length' and 'WARC-Block-Digest'.

Use [WarcFileWriter], initialized with [NewWarcFileWriter], to write WARC files.

# WARC record parsing

To parse single WARC records, use the [Unmarshaler] initialized with [NewUnmarshaler].

To read entire WARC files, employ the [WarcFileReader] initialized through [NewWarcFileReader].

# Validation and repair

The gowarc package supports validation during both the creation and parsing of WARC records.
Control over the scope of validation and the handling of validation errors can be achieved by setting the appropriate
options in the [WarcRecordBuilder], [Unmarshaler], or [WarcFileReader].
*/
package gowarc
