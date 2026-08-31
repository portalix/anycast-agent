FROM debian:12-slim
# knot-module-dnstap is packaged separately in Debian; apt fails loudly if
# the package does not exist (build-time assert).
RUN apt-get update && apt-get install -y --no-install-recommends \
        knot knot-module-dnstap \
    && rm -rf /var/lib/apt/lists/*
COPY knot.conf /etc/knot/knot.conf
RUN mkdir -p /rundir /var/lib/knot && chown knot:knot /rundir /var/lib/knot
ENTRYPOINT ["knotd", "-c", "/etc/knot/knot.conf"]
