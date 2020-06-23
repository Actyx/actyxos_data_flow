FROM debian
RUN apt update && apt install -y openssl curl

COPY target/release/examples/machine-dashboard /bin
CMD ["machine-dashboard"]
