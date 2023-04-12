export PATH := $(PATH):$(GOPATH)/bin

all: payload.pb.go go-kafka-serde

clean:
	rm -rf payload.pb.go go-kafka-serde

go-kafka-serde: payload.pb.go
	go build

PROTOC_GEN_GO := $(GOPATH)/bin/protoc-gen-go

$(PROTOC_GEN_GO):
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.30.0

payload.pb.go: ../../protobuf/payload.proto | $(PROTOC_GEN_GO)
	protoc --proto_path=../../protobuf --go_out=. --go_opt=Mpayload.proto=. payload.proto

compile: payload.pb.go
