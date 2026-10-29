from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from ingest.contracts import load_contracts
from tests.helpers import CONTRACTS_DIR, FIXTURE_INBOX


@pytest.fixture(scope="session")
def contracts():
    return load_contracts(CONTRACTS_DIR)


@pytest.fixture
def inbox(tmp_path: Path) -> Path:
    """A writable copy of the committed fixture inbox."""
    destination = tmp_path / "inbox"
    shutil.copytree(FIXTURE_INBOX, destination)
    return destination


@pytest.fixture
def data(tmp_path: Path) -> Path:
    return tmp_path / "data"
