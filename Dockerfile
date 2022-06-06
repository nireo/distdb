FROM golang:1.18-alpine AS build
WORKDIR /go/src/distdb
COPY . .
RUN CGO_ENABLED=0 go build -o /go/bin/distdb ./cmd/distdb

FROM scratch
COPY --from=build /go/bin/distdb /bin/distdb
ENTRYPOINT ["/bin/distdb"]
