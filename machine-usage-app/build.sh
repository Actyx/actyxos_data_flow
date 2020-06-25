#!/bin/bash

cd `dirname $0`

FG='\033[0;35m'
BG='\033[0m'
pushd ..
echo -e "${FG}building builder image${BG}"
BUILDER_ID=`docker build -f Dockerfile.builder -q .`
echo -e "${FG}built builder image ($BUILDER_ID)${BG}"

echo -e "${FG}compiling with --release, this may take a while${BG}"
docker run -v `pwd`:/src -u $(id -u ):$(id -g) -it $BUILDER_ID cargo build --release --examples
popd

echo -e "${FG}copying binary${BG}"
mkdir -pv bin
cp -v ../target/release/examples/machine-usage bin

echo -e "${FG}building docker image${BG}"
docker build -t machine-usage .
