FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder
ARG VERSION=dev
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-X main.version=${VERSION}" -o /steeplechase ./cmd/steeplechase

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /steeplechase /steeplechase
EXPOSE 4317 4318 9090
ENTRYPOINT ["/steeplechase"]
