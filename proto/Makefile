PROTOC_VERSION=3.11.4

.PHONY: all
.PHONY: clean

all:	tools *.pb.go #$(PROTO)

clean:
	rm -rf tools

tools:
	mkdir tools
	cd tools \
	&& wget -q https://github.com/google/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip \
	&& unzip protoc-${PROTOC_VERSION}-linux-x86_64.zip \
	&& rm protoc-${PROTOC_VERSION}-linux-x86_64.zip
	go get github.com/golang/protobuf/protoc-gen-go

%.pb.go: %.proto
	find ./ -name *.proto -exec 'tools/bin/protoc' '--go_out=paths=source_relative:.' '{}' ';'
