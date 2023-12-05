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
Package gowarc allows parsing, creating and validating WARC-files.

# WARC

The WARC format offers a standard way to structure, manage and store billions of resources collected from the web and elsewhere.
It is used to build applications for harvesting, managing, accessing, mining and exchanging content.

To learn more about the WARC standard, read the specification at https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/

# Create WARC records

The [WarcRecordBuilder] is used to create WARC records. It is initialized with [NewRecordBuilder]. The RecordBuilder will by default
generate a record id and calculate the Content-Length and WARC-Block-Digest.

The [WarcFileWriter] is used to write WARC files. It is initialized with [NewWarcFileWriter].

# Parse WARC records

The [Unmarshaler] is used to parse single WARC records. It is initialized with [NewUnmarshaler].

The [WarcFileReader] is used to read WARC files. It is initialized with [NewWarcFileReader].

# Validation and repair

Validation can be done both when creating and parsing WARC records. What is validated and how validation errors are handled can be controlled
by setting the appropriate options when creating the [WarcRecordBuilder], [Unmarshaler] or [WarcFileReader].
*/
package gowarc
