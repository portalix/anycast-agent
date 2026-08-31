#!/bin/sh
set -e
pdns_server --daemon=no --disable-syslog --write-pid=no &
exec dnsdist --supervised --disable-syslog -C /etc/dnsdist/dnsdist.conf
