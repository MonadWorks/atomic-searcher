"""The published sample must never enter wallet or transaction setup."""
import pytest

from searcher import __main__ as cli


@pytest.mark.parametrize("command,args", [
    ("wallet-import", []), ("setup-executor", []), ("launch", []),
    ("trade", ["--executor", "0x" + "2" * 40, "--keystore", "unused.json"]),
])
def test_example_owner_rejects_before_network_or_wallet(monkeypatch, capsys, command, args):
    def unexpected(*args, **kwargs):
        pytest.fail("Sample configuration reached an external or wallet operation")

    from searcher import onboarding
    monkeypatch.setattr(cli, "RPC", unexpected)
    monkeypatch.setattr(onboarding, "unlock_wallet", unexpected)
    monkeypatch.setattr(onboarding, "import_wallet", unexpected)
    monkeypatch.setattr("sys.argv", ["atomic-searcher", command, *args])
    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 2
    assert "public example" in capsys.readouterr().err
