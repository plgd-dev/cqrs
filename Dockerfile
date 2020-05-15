FROM golang:1.13.11-alpine3.10 AS build
RUN apk add --no-cache curl git build-base
WORKDIR $GOPATH/src/github.com/go-ocf/cqrs
COPY go.mod go.sum ./
RUN go mod download
COPY . .

