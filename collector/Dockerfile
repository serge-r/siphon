# syntax=docker/dockerfile:1
ARG GO_VERSION=1.26
ARG ALPINE_VERSION=3.21

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /siphon-collector .

FROM alpine:${ALPINE_VERSION}
RUN apk --update --no-cache add ca-certificates
COPY --from=build /siphon-collector /bin/siphon-collector

ENTRYPOINT ["siphon-collector"]
