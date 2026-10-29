"""Documentation gates: links resolve, component docs follow the spec."""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DOCS = REPO_ROOT / "docs"
COMPONENTS = DOCS / "components"
LINK = re.compile(r"\]\(([^)]+)\)")


def _markdown_files() -> list[Path]:
    return sorted(DOCS.rglob("*.md")) + [REPO_ROOT / "README.md"]


def test_relative_links_resolve():
    broken: list[str] = []
    for document in _markdown_files():
        for match in LINK.finditer(document.read_text(encoding="utf-8")):
            target = match.group(1).strip()
            if target.startswith(("http://", "https://", "#", "mailto:")):
                continue
            target = target.split("#", 1)[0]
            if target and not (document.parent / target).resolve().exists():
                broken.append(f"{document.relative_to(REPO_ROOT)} -> {target}")

    assert not broken, "broken links:\n" + "\n".join(broken)


def test_component_docs_follow_the_writing_spec():
    problems: list[str] = []
    for document in sorted(COMPONENTS.glob("*.md")):
        if document.name == "index.md":
            continue
        text = document.read_text(encoding="utf-8")
        lines = text.splitlines()
        name = document.name

        if not lines or not lines[0].startswith("# "):
            problems.append(f"{name}: missing title")
        for section in ("## Overview", "## Current Limitations"):
            if section not in text:
                problems.append(f"{name}: missing {section}")
        if "## Schema" not in text and "## API" not in text:
            problems.append(f"{name}: missing Schema or API")
        if len(lines) < 30:
            problems.append(f"{name}: {len(lines)} lines, stub")
        if "<div" in text or "<details>" in text:
            problems.append(f"{name}: contains HTML")

    assert not problems, "component docs:\n" + "\n".join(problems)


def test_decisions_index_covers_every_record():
    index = (DOCS / "decisions_index.md").read_text(encoding="utf-8")
    missing = [
        path.name
        for path in sorted((DOCS / "decisions").glob("*.md"))
        if path.name not in index
    ]

    assert not missing, "decision records not indexed:\n" + "\n".join(missing)
