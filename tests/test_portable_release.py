"""The public source tree is reproducible without private runtime files."""

import socket

import pytest

from searcher.source_files import REQUIRED_FILES, source_files
from searcher.upgrade import manifest


def test_manifest_excludes_local_state_and_covers_viewer_assets(tmp_path, monkeypatch):
    from searcher import artifacts

    files = (*REQUIRED_FILES, "searcher/worker.py", "src/Executor.sol",
             "tools/live-view/view.py", "tools/live-view/index.html",
             "tools/live-view/style.css", "tools/live-view/app.js",
             "tests/fixtures/synthetic.json", "tests/fixtures/nested/synthetic.json",
             "config/node.local.json", "evidence/runtime.json",
             "research/prepared_lane.py", "Start-trading.command")
    for name in files:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("placeholder")
    expected = set(files) - {"config/node.local.json", "evidence/runtime.json",
                             "research/prepared_lane.py", "Start-trading.command"}
    assert {str(path.relative_to(tmp_path)) for path in source_files(tmp_path)} == expected
    monkeypatch.setattr(artifacts, "ROOT", tmp_path)
    assert artifacts.source_manifest() == manifest(tmp_path)


def test_release_creates_backup_directory_for_a_fresh_checkout(tmp_path):
    from test_release_install import release
    from searcher.upgrade import install

    target, bundle = release(tmp_path)
    (target / "releases").rmdir()
    result = install(bundle, target)
    assert result["state"] == "installed"
    assert (target / "releases").is_dir()


@pytest.mark.parametrize("host", ["rpc.example.invalid", "198.51.100.1", "2001:db8::1"])
def test_live_network_access_is_blocked_before_name_resolution(host):
    with pytest.raises(pytest.fail.Exception, match="External network access blocked"):
        socket.getaddrinfo(host, 443)


def test_loopback_service_remains_available():
    with socket.socket() as server, socket.socket() as client:
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        client.connect(server.getsockname())
        connection, _ = server.accept()
        with connection:
            client.sendall(b"local-only")
            assert connection.recv(32) == b"local-only"
