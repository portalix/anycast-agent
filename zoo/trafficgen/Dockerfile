FROM debian:12-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        dnsutils iproute2 \
    && rm -rf /var/lib/apt/lists/*
COPY run.sh /run.sh
RUN chmod +x /run.sh
ENTRYPOINT ["/run.sh"]
