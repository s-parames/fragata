from __future__ import annotations

import ipaddress
import socket
from urllib.parse import urlparse


BLOCKED_HOSTS = {
    "localhost",
    "localhost.localdomain",
    "127.0.0.1",
    "::1",
    "0.0.0.0",
}

BLOCKED_HOST_SUFFIXES = (
    ".local",
    ".internal",
    ".home",
    ".lan",
)


def _is_forbidden_ip(ip: ipaddress._BaseAddress) -> bool:  # type: ignore[name-defined]
    return (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    )


def _hostname_is_forbidden(hostname: str) -> bool:
    host = (hostname or "").strip().lower()
    if not host:
        return True
    if host in BLOCKED_HOSTS:
        return True
    if any(host.endswith(suffix) for suffix in BLOCKED_HOST_SUFFIXES):
        return True

    try:
        ip = ipaddress.ip_address(host)
        return _is_forbidden_ip(ip)
    except ValueError:
        pass

    try:
        infos = socket.getaddrinfo(host, None)
    except socket.gaierror:
        return False
    except Exception:
        return False

    for info in infos:
        address = info[4][0]
        try:
            ip = ipaddress.ip_address(address)
        except ValueError:
            continue
        if _is_forbidden_ip(ip):
            return True
    return False


def validate_public_http_url(url: str) -> str:
    raw = (url or "").strip()
    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("URL must use http or https")
    if not parsed.hostname:
        raise ValueError("URL must include a host")
    if _hostname_is_forbidden(parsed.hostname):
        raise ValueError("URL host is blocked (private/local addresses are not allowed)")
    return raw
