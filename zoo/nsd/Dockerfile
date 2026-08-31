FROM debian:12-slim
# Debian's NSD is built with --enable-dnstap; nsd -v is the build-time assert.
RUN apt-get update && apt-get install -y --no-install-recommends nsd \
    && rm -rf /var/lib/apt/lists/* \
    && nsd -v 2>&1 | grep -q enable-dnstap
COPY nsd.conf /etc/nsd/nsd.conf
ENTRYPOINT ["nsd", "-d", "-c", "/etc/nsd/nsd.conf"]
