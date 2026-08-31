FROM debian:12-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        bind9 bind9-utils \
    && rm -rf /var/lib/apt/lists/* \
    && named -V | grep -qi dnstap
COPY named.conf /etc/bind/named.conf
RUN mkdir -p /var/cache/bind && chown bind:bind /var/cache/bind
ENTRYPOINT ["named", "-g", "-c", "/etc/bind/named.conf", "-u", "bind"]
