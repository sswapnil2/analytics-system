#!/usr/bin/env python3
"""Prepare site/ for publishing the docs.

- Copies docs/*.md and docs/adr/ to site/docs/md/, which the viewer (site/docs/index.html) renders.
- Writes site/llms.txt (an index of every page, https://llmstxt.org) and site/llms-full.txt (every
  page concatenated), so an agent can read the docs as plain Markdown in one or two fetches.

Page order and titles come from the NAV list in site/docs/index.html, so there is one list to edit.
Run by .github/workflows/pages.yml; run it locally to preview (see docs/contributing.md).
"""
import re
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SITE = ROOT / "site"
BASE = "https://sswapnil2.github.io/analytics-system/"  # matches the canonical URL in site/index.html


def nav():
    """[(section, [(page, title), ...]), ...] parsed from the viewer's NAV constant."""
    src = (SITE / "docs/index.html").read_text(encoding="utf-8")
    block = re.search(r"const NAV = \[(.*?)\n\];", src, re.S).group(1)
    return [(sec, re.findall(r'\["([\w\-/]+)",\s*"([^"]+)"\]', items))
            for sec, items in re.findall(r'\["([^"]+)",\s*\[(.*?)\]\],', block, re.S)]


def summary(md):
    """First prose paragraph of a page, flattened to one line, for the llms.txt index."""
    paras = [p.strip() for p in re.split(r"\n\s*\n", md)]
    for i, p in enumerate(paras):
        if p and not p.startswith(("#", "|", "```", ">", "-", "*", "<")):
            if p.endswith(":") and i + 1 < len(paras) and paras[i + 1].startswith("- "):
                # "X provides:" followed by a list -> "X provides: a, b, c"
                p += " " + ", ".join(l[2:].strip() for l in paras[i + 1].splitlines() if l.startswith("- "))
            p = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", " ".join(p.split()))
            return p if len(p) <= 220 else p[:217].rsplit(" ", 1)[0] + "…"
    return ""


def main():
    out = SITE / "docs/md"
    shutil.rmtree(out, ignore_errors=True)
    (out / "adr").mkdir(parents=True)
    for f in (ROOT / "docs").glob("*.md"):
        shutil.copy(f, out / f.name)
    for f in (ROOT / "docs/adr").glob("*.md"):
        shutil.copy(f, out / "adr" / f.name)

    sections = nav()
    adrs = sorted(p.stem for p in (ROOT / "docs/adr").glob("0*.md"))
    sections.append(("Design decisions (ADRs)", [(f"adr/{a}", None) for a in adrs]))

    index = ["# Pulse", "",
             "> Open-source, self-hosted product analytics in one Rust binary: event ingest, exact funnels, "
             "retention and trends, privacy-masked session replay, feature flags and experiments, AI agent "
             "observability (with OTLP GenAI spans), and an MCP endpoint for AI agents.",
             "",
             f"Every page below is plain Markdown. All of them in one file: {BASE}llms-full.txt",
             f"Human-readable site: {BASE}docs/ · Source: https://github.com/sswapnil2/analytics-system", ""]
    full = []
    for sec, pages in sections:
        index += [f"## {sec}", ""]
        for page, title in pages:
            md = (ROOT / "docs" / f"{page}.md").read_text(encoding="utf-8")
            title = title or re.search(r"^# (.+)$", md, re.M).group(1)
            url = f"{BASE}docs/md/{page}.md"
            desc = summary(md)
            index.append(f"- [{title}]({url})" + (f": {desc}" if desc else ""))
            full += [f"<!-- source: {url} -->", md.rstrip(), ""]
        index.append("")

    (SITE / "llms.txt").write_text("\n".join(index), encoding="utf-8")
    (SITE / "llms-full.txt").write_text("\n".join(full), encoding="utf-8")
    print(f"site/docs/md: {sum(len(p) for _, p in sections)} pages; wrote site/llms.txt and site/llms-full.txt")


if __name__ == "__main__":
    main()
