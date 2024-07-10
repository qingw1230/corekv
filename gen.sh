#! /bin/bash

protoDir="utils/codec/pb"
outDir="utils/codec/pb"
protoc -I ${protoDir}/ ${protoDir}/pb.proto --gofast_out=plugins=grpc:${outDir}