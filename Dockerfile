FROM golang:1.13 as build

WORKDIR /build

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

# Compile the binary statically, so it can be run without libraries.
RUN CGO_ENABLED=0 GOOS=linux go install -a -ldflags '-extldflags "-s -w -static"' ./cmd/warc
#RUN go test ./... && go install -v ./...

# Now copy it into our base image.
FROM gcr.io/distroless/base
COPY --from=build /go/bin/warc /
EXPOSE 9999

ENTRYPOINT ["/warc"]
CMD ["serve"]
