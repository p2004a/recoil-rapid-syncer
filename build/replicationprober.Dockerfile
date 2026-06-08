FROM docker.io/library/golang:1.25-bookworm AS build

WORKDIR /go/src/app

COPY go.mod ./
COPY go.sum ./
RUN go mod download -x

COPY . .
RUN CGO_ENABLED=0 go build ./cmd/replicationprober

FROM gcr.io/distroless/static-debian12
WORKDIR /
USER nonroot:nonroot
COPY --from=build /go/src/app/replicationprober /
ENTRYPOINT ["/replicationprober"]
