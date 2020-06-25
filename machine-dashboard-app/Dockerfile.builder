FROM rust:1.44.1-slim-stretch AS builder
RUN apt update && apt install -y libssl-dev pkg-config

WORKDIR /src
