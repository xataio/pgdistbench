# Build the driver binary
FROM golang:1.24 AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace

# Build go-tpc project directly
RUN git clone https://github.com/pingcap/go-tpc.git && \
    cd go-tpc && \
    go build ./cmd/go-tpc

# Build delve
RUN go install github.com/go-delve/delve/cmd/dlv@latest

# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the project
COPY . .

# Build
# the GOARCH has not a default value to allow the binary be built according to the host where the command
# was called. For example, if we call make docker-build in a local env which has the Apple Silicon M1 SO
# the docker BUILDPLATFORM arg will be linux/arm64 when for Apple x86 it will be linux/amd64. Therefore,
# by leaving it empty we can ensure that the container and binary shipped on it will have the same platform.
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -gcflags "all=-N -l" -a -o driver ./cmd/benchdriver

FROM ubuntu:latest
WORKDIR /

RUN mkdir -p /usr/bin && \
  mkdir -p /usr/local/go/src && \
  mkdir -p /go/pkg/mod && \
  mkdir /workspace

COPY --from=builder /workspace/go-tpc/go-tpc /usr/bin/go-tpc
COPY --from=builder /workspace/driver .
USER 65532:65532

ENTRYPOINT ["/driver"]
