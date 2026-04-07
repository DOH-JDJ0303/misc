FROM ubuntu:24.04

RUN apt-get update && \
    apt-get install -y --no-install-recommends pigz bash && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

SHELL ["/bin/bash", "-c"]
WORKDIR /work
