# syntax=docker/dockerfile:1
ARG GO_VERSION=1.26

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-bookworm AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev

# clang + llvm compile and embed the eBPF object; no libbpf or kernel headers.
RUN apt-get update \
    && apt-get install -y --no-install-recommends clang llvm make ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

# Compile bpf/probe.c into the embedded bindings (arch-independent), then
# cross-compile the Go binary for the target platform.
RUN make generate
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /siphon-probe .

FROM alpine:3.21
COPY --from=build /siphon-probe /bin/siphon-probe

# The probe attaches an eBPF TC classifier, so the container needs host
# networking and NET_ADMIN/BPF capabilities, e.g.:
#   docker run --network host --cap-add NET_ADMIN --cap-add BPF \
#     serger89/siphon-probe -iface eth0 -collector 10.0.0.1:4739
ENTRYPOINT ["siphon-probe"]
