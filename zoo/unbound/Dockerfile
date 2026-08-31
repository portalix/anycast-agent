FROM debian:12-slim
# Debian's Unbound is built with dnstap support; the configure line in
# `unbound -V` is the build-time assert (same pattern as NSD/BIND).
RUN apt-get update && apt-get install -y --no-install-recommends unbound \
    && rm -rf /var/lib/apt/lists/* \
    && unbound -V 2>&1 | grep -qi dnstap
COPY unbound.conf /etc/unbound/unbound.conf
ENTRYPOINT ["unbound", "-d", "-c", "/etc/unbound/unbound.conf"]
