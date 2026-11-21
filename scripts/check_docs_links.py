#!/usr/bin/env python3
"""Fail on broken relative links in the repo's Markdown docs.

Checks every relative link and #anchor in docs/**/*.md and the top-level *.md files:
the target file must exist, and an anchor must match a heading in the target (GitHub's
slug rules). Also checks that every page in the docs site nav (site/docs/index.html)
has a Markdown file. External URLs are not fetched.

Usage: python3 scripts/check_docs_links.py   (from anywhere; exits 1 on any broken link)
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LINK = re.compile(r"(?<!!)\[(?:[^\[\]]|\[[^\]]*\])*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
FENCE = re.compile(r"^\s*(```|~~~)")


def slug(text):
    """GitHub heading anchor: rendered text, lowercased, punctuation dropped, spaces to dashes."""
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)  # [text](url) -> text
    text = re.sub(r"<[^>]+>", "", text).strip().lower()
    return re.sub(r"[^\w\- ]", "", text).replace(" ", "-")


def scan(path):
    """Return (anchors, links) for a Markdown file, ignoring fenced code blocks."""
    anchors, links, seen, in_fence = set(), [], {}, False
    for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if FENCE.match(line):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        m = re.match(r"^#{1,6}\s+(.*?)\s*#*\s*$", line)
        if m:
            s = slug(m.group(1))
            k = seen.get(s, 0)
            anchors.add(s if k == 0 else f"{s}-{k}")  # GitHub suffixes duplicate headings
            seen[s] = k + 1
        for target in LINK.findall(re.sub(r"`[^`]*`", "", line)):
            links.append((n, target))
    return anchors, links


def main():
    files = sorted(ROOT.glob("docs/**/*.md")) + sorted(ROOT.glob("*.md"))
    files = [f for f in files if "docs/history" not in f.as_posix()]
    cache = {}

    def anchors_of(p):
        if p not in cache:
            cache[p] = scan(p)[0]
        return cache[p]

    errors = []
    for f in files:
        for n, target in scan(f)[1]:
            if re.match(r"^[a-z][a-z0-9+.-]*:", target, re.I):  # http:, mailto:, ...
                continue
            path, _, anchor = target.partition("#")
            dest = (f.parent / path).resolve() if path else f
            where = f"{f.relative_to(ROOT)}:{n}: {target}"
            if not dest.exists():
                errors.append(f"{where} -> missing file")
            elif anchor and dest.suffix == ".md" and anchor.lower() not in anchors_of(dest):
                errors.append(f"{where} -> no heading '#{anchor}' in {dest.relative_to(ROOT)}")

    site = ROOT / "site/docs/index.html"
    if site.exists():
        nav = re.search(r"const NAV = \[(.*?)\n\];", site.read_text(encoding="utf-8"), re.S)
        for page in re.findall(r'\["([\w\-/]+)",\s*"', nav.group(1) if nav else ""):
            if not (ROOT / "docs" / f"{page}.md").exists():
                errors.append(f"site/docs/index.html: nav page '{page}' -> missing docs/{page}.md")

    for e in errors:
        print(e)
    print(f"{len(files)} files checked, {len(errors)} broken link(s)")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
