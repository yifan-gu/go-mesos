all: *.proto
	protoc --proto_path=${GOPATH}/src:. --gogo_out=. *.proto
	sed -i 's/code\.google\.com/github\.com\/yifan-gu\/go-mesos\/3rdparty\/code.google.com/g' *.go
