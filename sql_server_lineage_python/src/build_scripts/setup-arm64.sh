#!/bin/sh
if [ ! -f "/usr/bin/go" ]; then
    echo "Downloading arm64 Golang"
    if [ -x /usr/bin/curl ]; then
        curl -L -O https://golang.org/dl/go1.22.2.linux-arm64.tar.gz
    else
        wget -v https://golang.org/dl/go1.22.2.linux-arm64.tar.gz
    fi
    tar -xf go1.22.2.linux-arm64.tar.gz
    if [ -x /lib/libc.musl-* ]; then
        apk add --no-cache libc6-compat
    fi
fi