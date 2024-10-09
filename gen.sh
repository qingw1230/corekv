#! /bin/bash

SRC_DIR=.
DST_DIR=.
protoc -I=$SRC_DIR --go_out=$DST_DIR $SRC_DIR/pb/pb.proto
