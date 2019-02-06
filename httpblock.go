/*
 * Copyright 2019 National Library of Norway.
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
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"
)

func NewHttpBlock(block Block) (PayloadBlock, error) {
	r := bufio.NewReader(bytes.NewReader(block.RawBytes()))
	tp := textproto.NewReader(r)

	// Parse the first line of the response.
	line, err := tp.ReadLine()
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}

	status, err := parseStatusLine(line)
	if err == nil {
		return createHttpResponseBlock(block, tp, status)
	}

	req, err := parseRequestLine(line)
	if err == nil {
		return createHttpRequestBlock(block, tp, req)
	}

	return nil, err
}

func createHttpRequestBlock(block Block, tp *textproto.Reader, requestLine *HttpRequestLine) (*HttpRequestBlock, error) {
	resp := &HttpRequestBlock{Block: block, request: requestLine}

	header, err := parseHeaders(tp)
	resp.httpHeader = header
	if err == io.EOF {
		resp.httpPayloadBytes = []byte{}
	} else if err != nil {
		return resp, err
	} else {
		headSize := upcomingHeaderNewlines(block.RawBytes())
		resp.httpPayloadBytes = block.RawBytes()[headSize:]
	}
	return resp, nil
}

func createHttpResponseBlock(block Block, tp *textproto.Reader, statusLine *HttpStatusLine) (*HttpResponseBlock, error) {
	resp := &HttpResponseBlock{Block: block, status: statusLine}

	header, err := parseHeaders(tp)
	resp.httpHeader = header
	if err == io.EOF {
		resp.httpPayloadBytes = []byte{}
	} else if err != nil {
		return resp, err
	} else {
		headSize := upcomingHeaderNewlines(block.RawBytes())
		resp.httpPayloadBytes = block.RawBytes()[headSize:]
	}
	return resp, nil
}

func upcomingHeaderNewlines(rawBytes []byte) (l int) {
	peek := rawBytes
	for len(peek) > 0 {
		i := bytes.IndexByte(peek, '\n')
		l += i + 1
		if i < 3 {
			// Not present (-1) or found within the next few bytes,
			// implying we're at the end ("\r\n\r\n" or "\n\n")
			return
		}
		peek = peek[i+1:]
	}
	return
}

func parseHeaders(tp *textproto.Reader) (header http.Header, err error) {
	mimeHeader, err := tp.ReadMIMEHeader()
	header = http.Header(mimeHeader)
	return
}

func parseStatusLine(line string) (*HttpStatusLine, error) {
	s := &HttpStatusLine{}

	if i := strings.IndexByte(line, ' '); i == -1 {
		return nil, &badStringError{"malformed HTTP response", line}
	} else {
		s.Proto = line[:i]
		s.Status = strings.TrimLeft(line[i+1:], " ")
	}
	statusCode := s.Status
	if i := strings.IndexByte(s.Status, ' '); i != -1 {
		statusCode = s.Status[:i]
	}
	if len(statusCode) != 3 {
		return nil, &badStringError{"malformed HTTP status code", statusCode}
	}
	var err error
	s.StatusCode, err = strconv.Atoi(statusCode)
	if err != nil || s.StatusCode < 0 {
		return nil, &badStringError{"malformed HTTP status code", statusCode}
	}
	var ok bool
	if s.ProtoMajor, s.ProtoMinor, ok = http.ParseHTTPVersion(s.Proto); !ok {
		return nil, &badStringError{"malformed HTTP version", s.Proto}
	}

	return s, nil
}

// parseRequestLine parses "GET /foo HTTP/1.1" into its three parts.
func parseRequestLine(line string) (requestLine *HttpRequestLine, err error) {
	s1 := strings.Index(line, " ")
	s2 := strings.Index(line[s1+1:], " ")
	if s1 < 0 || s2 < 0 {
		return
	}
	s2 += s1 + 1

	r := &HttpRequestLine{
		Method:     line[:s1],
		RequestURI: line[s1+1 : s2],
		Proto:      line[s2+1:],
	}

	return r, nil
	//return line[:s1], line[s1+1 : s2], line[s2+1:], true
}

type badStringError struct {
	what string
	str  string
}

func (e *badStringError) Error() string { return fmt.Sprintf("%s %q", e.what, e.str) }
