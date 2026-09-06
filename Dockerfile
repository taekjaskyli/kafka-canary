FROM --platform=$BUILDPLATFORM golang:1.26.8-alpine AS builder

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG VERSION=dev

ENV CGO_ENABLED=0 \
    GOTOOLCHAIN=local

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" \
    go build \
      -trimpath \
      -ldflags="-s -w -X 'main.version=${VERSION}'" \
      -o /kafka-canary \
      ./cmd/

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /kafka-canary /kafka-canary

LABEL org.opencontainers.image.source="https://github.com/taekjaskyli/kafka-canary" \
      org.opencontainers.image.url="https://github.com/taekjaskyli/kafka-canary" \
      org.opencontainers.image.title="kafka-canary" \
      org.opencontainers.image.description="A Kafka availability and health canary" \
      org.opencontainers.image.licenses="Apache-2.0"

USER 65532:65532
WORKDIR /
EXPOSE 8080
ENTRYPOINT ["/kafka-canary"]
