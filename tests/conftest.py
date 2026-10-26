"""Offline by default: mocks and temporary loopback test chains only."""

import ipaddress
import socket

import pytest


def _loopback(host):
    if isinstance(host, bytes):
        host = host.decode("ascii", errors="replace")
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


@pytest.fixture(autouse=True)
def local_network_only(monkeypatch):
    """Prevent an incomplete mock from contacting a live RPC or local proxy.

    MockTransport never reaches these socket hooks. Loopback sockets remain
    available for synthetic Anvil markets, HTTP servers and process tests.
    """
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY",
                 "http_proxy", "https_proxy", "all_proxy", "no_proxy"):
        monkeypatch.delenv(name, raising=False)

    original_getaddrinfo = socket.getaddrinfo
    original_connect = socket.socket.connect
    original_connect_ex = socket.socket.connect_ex
    original_sendto = socket.socket.sendto

    def require_local(host):
        if not _loopback(host):
            pytest.fail("External network access blocked: use a mock or loopback test service.",
                        pytrace=False)

    def getaddrinfo(host, *args, **kwargs):
        require_local(host)
        result = original_getaddrinfo(host, *args, **kwargs)
        for row in result:
            require_local(row[4][0])
        return result

    def connect(sock, address):
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            require_local(address[0])
        return original_connect(sock, address)

    def connect_ex(sock, address):
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            require_local(address[0])
        return original_connect_ex(sock, address)

    def sendto(sock, data, *args):
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            require_local(args[-1][0])
        return original_sendto(sock, data, *args)

    monkeypatch.setattr(socket, "getaddrinfo", getaddrinfo)
    monkeypatch.setattr(socket.socket, "connect", connect)
    monkeypatch.setattr(socket.socket, "connect_ex", connect_ex)
    monkeypatch.setattr(socket.socket, "sendto", sendto)
