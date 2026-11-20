#!/usr/bin/env python3
"""
Generates the single-header distribution of gorun:
    single_include/gorun/gorun.hpp

All internal `#include "gorun/..."` directives are inlined recursively (each file once),
so the result can be dropped into any project or included by URL (e.g. on Compiler Explorer).

Usage:
    python scripts/amalgamate.py
"""

import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parent.parent
INCLUDE_DIR = ROOT / "include"
OUTPUT = ROOT / "single_include" / "gorun" / "gorun.hpp"
LOCAL_INCLUDE = re.compile(r'^\s*#\s*include\s*"(gorun/[^"]+)"\s*$')

_seen: set[str] = set()


def expand(rel_path: str) -> str:
    if rel_path in _seen:
        return ""
    _seen.add(rel_path)

    text = (INCLUDE_DIR / rel_path).read_text(encoding="utf-8")
    out = [f"// ---- begin {rel_path} ----"]
    for line in text.splitlines():
        if line.strip() == "#pragma once":
            continue
        match = LOCAL_INCLUDE.match(line)
        if match:
            out.append(expand(match.group(1)))
            continue
        out.append(line)
    out.append(f"// ---- end {rel_path} ----")
    return "\n".join(out)


HEADER = """#pragma once
// =====================================================================================
//  gorun — single-header distribution (GENERATED FILE, DO NOT EDIT)
//  M:N Green Threads, Channels, select(), Context, Defer & Structured Concurrency
//  for C++20/23.
//
//  Source:  https://github.com/Takatochi/gorun
//  Author:  Artur Havor
//  License: Apache License 2.0
//
//  Regenerate with: python scripts/amalgamate.py
// =====================================================================================
"""


def main() -> None:
    body = expand("gorun/gorun.hpp")
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(HEADER + body + "\n", encoding="utf-8", newline="\n")
    print(f"Wrote {OUTPUT.relative_to(ROOT)} ({len(_seen)} headers inlined)")


if __name__ == "__main__":
    main()
