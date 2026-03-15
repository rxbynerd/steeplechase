FROM golang:1.23-alpine AS builder
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-X main.version=${VERSION}" -o /steeplechase ./cmd/steeplechase

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /steeplechase /steeplechase
EXPOSE 4317 4318
ENTRYPOINT ["/steeplechase"]
