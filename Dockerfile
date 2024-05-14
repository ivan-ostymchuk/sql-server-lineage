FROM golang:1.22-bullseye as go_build
RUN apt-get update && apt-get install -y bzip2 curl gcc-aarch64-linux-gnu
RUN go install honnef.co/go/tools/cmd/staticcheck@latest
RUN go install golang.org/x/vuln/cmd/govulncheck@latest
ENV CGO_ENABLED=1
ENV GOOS=linux
ENV GOARCH=arm64
ENV CC=aarch64-linux-gnu-gcc
COPY . .
RUN go test ./sql_server_lineage
RUN staticcheck -tests=false ./...
RUN govulncheck ./...

FROM python:3.12-slim
RUN apt-get update && apt-get install -y bzip2 curl gcc-aarch64-linux-gnu
RUN curl -LO https://dl.google.com/go/go1.22.2.linux-amd64.tar.gz
RUN tar xzf go1.22.2.linux-amd64.tar.gz
RUN ln -s /go/bin/go /usr/local/bin
RUN python3 -m pip install build
ENV CGO_ENABLED=1
ENV GOOS=linux
ENV GOARCH=arm64
ENV CC=aarch64-linux-gnu-gcc
COPY . .
WORKDIR sql_server_lineage_python/src
RUN python3 -m build --wheel
RUN python3 -m pip install "$(find dist -name *.whl)"
RUN cd ../../ && python3 sql_server_lineage_python/tests/test_sql_server_lineage.py
