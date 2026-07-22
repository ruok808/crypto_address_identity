from __future__ import annotations

from crypto_address_identity import __version__
from crypto_address_identity.cli import build_parser


def test_package_version_is_available() -> None:
    assert __version__ == "0.1.0"


def test_cli_registers_btc_first_commands() -> None:
    parser = build_parser()

    assert parser.prog == "cai"
