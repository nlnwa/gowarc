FROM golang:1.13 as build

WORKDIR /build

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

# -trimpath remove file system paths from executable
# -ldflags arguments passed to go tool link:
#   -s disable symbol table
#   -w disable DWARF generation
RUN go build -trimpath -ldflags "-s -w" ./cmd/warc

# Now copy it into our base image.
FROM gcr.io/distroless/base
COPY --from=build /build/warc /warc
EXPOSE 9999

ENTRYPOINT ["/warc"]
CMD ["serve"]
