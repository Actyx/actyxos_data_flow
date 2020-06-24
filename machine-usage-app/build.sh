#!/bin/bash

cd `dirname $0`

FG='\033[0;35m'
BG='\033[0m'

echo -e "${FG}compiling with --release, this may take a while${BG}"
cargo build --release --examples

echo -e "${FG}copying binary${BG}"
mkdir -pv bin
cp -v ../target/release/examples/machine-usage bin

echo -e "${FG}building docker image${BG}"
docker build -t machine-usage .
