"""Minimal smoke tests verifying the repository tooling is wired up."""

import sys


def test_python_version_is_supported():
    assert sys.version_info >= (3, 11)
