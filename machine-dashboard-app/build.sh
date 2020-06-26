#!/bin/bash

cd `dirname $0`

FG='\033[0;35m'
BG='\033[0m'

echo -e "${FG}building builder image${BG}"
BUILD_ID=`docker build -f Dockerfile.builder . | tee /dev/fd/2 | awk '/Successfully built/{print $3}'`

echo -e "${FG}compiling with --release, this may take a while${BG}"
docker run -v `pwd`/..:/src -u $(id -u):$(id -g) -it $BUILD_ID cargo build --release --examples

echo -e "${FG}copying binary${BG}"
mkdir -pv bin
cp -v ../target/release/examples/machine-dashboard bin

echo -e "${FG}building docker image${BG}"
docker build -t machine-dashboard .
