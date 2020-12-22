![Docker](https://github.com/nlnwa/gowarc/workflows/Docker/badge.svg)

# go-warc

A tool for handling everything warc, written in go.

# Requirements

go 1.13 or newer

# Build

Run `go build ./cmd/warc/`

# Config file

You can configure certain aspect of gowarc with a config file. Here are all posible fields. These can also be overwritten by enviournment variables with same name


| Name          | Type           | Description  | Default |
| ------------- | -------------  | -----------  | ------- |
| warcdir       |  List of paths | The register of all directories where warcs will be located  | ["."] |
| indexdir      |  path          | The root directory for indices files  | "." |
| autoindex     |  bool          | Wether gowarc should index from the warcdir(s) when serving automatically or not  | true |