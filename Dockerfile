FROM golang AS builder

WORKDIR /go/src/app
COPY src/. /go/src/app
RUN go get
RUN CGO_ENABLED=0 go build -o main .

FROM alpine
RUN apk --no-cache add ca-certificates
COPY --from=builder /go/src/app/main /app
CMD ["/app"]

EXPOSE 8080