FROM golang:1.13

RUN mkdir /test
WORKDIR /test

COPY go.mod go.sum ./
RUN go mod download

COPY . ./
RUN go install ./...

ENTRYPOINT ["./entrypoint.sh"]
