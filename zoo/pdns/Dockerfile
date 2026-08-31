# PowerDNS Auth 4.7 + dnsdist 1.7 from Debian 12 — a common production
# pairing: pdns cannot speak dnstap itself, dnsdist in front does.
FROM debian:12-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        pdns-server pdns-backend-bind dnsdist \
    && rm -rf /var/lib/apt/lists/*
COPY pdns.conf /etc/powerdns/pdns.conf
COPY named.conf /etc/powerdns/named.conf
COPY dnsdist.conf /etc/dnsdist/dnsdist.conf
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
