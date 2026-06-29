#!/usr/bin/env python3
"""build_notebook.py — convert the demo .sql chapter files into two
notebook-format outputs:

  * ``pg_xarray_demo.ipynb`` — Jupyter notebook (VS Code, JupyterLab,
    Colab, etc). Executes via jupysql + psycopg2 if installed.
  * ``demo.sqlbook``        — SQL Notebook (VS Code SQL Notebook
    extension / sqlbook.dev). Plain-text format with
    ``-- SQLBook: <CellType>`` cell markers — also readable as a plain
    ``.sql`` file via psql.

The .sql files remain the source of truth — this script just re-emits
them as cells:

  * Leading ``-- ====`` block at the top of each chapter → markdown cell
    (chapter title + narrative).
  * Each ``-- --- N.M section ---`` divider → new markdown cell.
  * Anything between dividers → one SQL code cell.
  * ``\\echo`` lines and psql backslash commands are stripped from code
    cells (they're psql-only and don't run in a Jupyter SQL kernel).

Regenerate after editing any .sql file:

    /usr/bin/python3 demo/build_notebook.py
"""

from __future__ import annotations

import json
import re
from pathlib import Path

CHAPTERS = [
    "00_setup.sql",
    "01_register_local.sql",
    "02_cloud_native.sql",
    "03_fdw_and_joins.sql",
    "04_feature_views.sql",
    "05_unstructured_mesh.sql",
]


def md_cell(src: str) -> dict:
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": _split_lines(src),
    }


def sql_cell(src: str) -> dict:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {"vscode": {"languageId": "sql"}},
        "outputs": [],
        "source": _split_lines(src),
    }


def _split_lines(s: str) -> list[str]:
    # Jupyter convention: each entry is a line ending in '\n', except the
    # last which has no trailing newline.
    lines = s.splitlines(keepends=True)
    if lines and not lines[-1].endswith("\n"):
        return lines
    if lines and lines[-1] == "\n":
        lines = lines[:-1]
    return lines


def strip_psql_directives(sql: str) -> str:
    """Remove psql-only lines (``\\echo``, ``\\set``, ``\\pset``, etc) and
    ``-- comment`` lines that are pure narrative — we move those to the
    markdown cell preceding the SQL cell. Keep CTE-style inline comments."""
    out_lines = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("\\"):
            continue
        if not stripped:
            out_lines.append(line)
            continue
        out_lines.append(line)
    text = "\n".join(out_lines).strip()
    return text


def parse_chapter(path: Path) -> list[dict]:
    """Split a chapter .sql into a sequence of (markdown, code) cells.

    The chapter is read line by line. Lines belong to either:
      * the current narrative block (markdown — comments + \\echo),
      * the current SQL block (code).
    A "--- section ---" comment marker flushes both and starts a new
    section. The first narrative block emitted carries the chapter
    header (the leading ``-- ====`` block).
    """
    text = path.read_text()
    cells: list[dict] = []
    narrative: list[str] = []
    sql: list[str] = []

    def flush(force_md: bool = False) -> None:
        nonlocal narrative, sql
        md = "\n".join(narrative).strip()
        code = strip_psql_directives("\n".join(sql))
        if md or force_md:
            cells.append(md_cell(md if md else "*(no narration)*"))
        if code:
            cells.append(sql_cell(code))
        narrative, sql = [], []

    for line in text.splitlines():
        stripped = line.strip()
        # Section divider: lines like "-- 1.1 — Foo" or
        # "-- --- 1.1 — Foo ---" or "-- ====...===="
        is_divider = bool(
            re.match(r"^-- =+\s*$", stripped)
            or re.match(r"^-- ---+\s*$", stripped)
        )
        if is_divider:
            flush()
            continue

        is_narration = (
            stripped.startswith("--")
            or stripped.startswith("\\echo")
            or stripped == ""
        )

        if is_narration:
            # If we already have SQL accumulated, flush the section.
            if sql and any(s.strip() for s in sql):
                flush()
            # Normalise `\echo 'foo'` and `-- foo` into plain prose lines
            # for the markdown cell.
            if stripped.startswith("\\echo"):
                m = re.match(r"\\echo\s*'?(.*?)'?\s*$", stripped)
                if m:
                    msg = m.group(1).strip()
                    if msg:
                        narrative.append(msg)
            elif stripped.startswith("--"):
                msg = stripped.lstrip("-").strip()
                if msg:
                    narrative.append(msg)
            # else: blank line → preserved as paragraph break
            else:
                if narrative and narrative[-1] != "":
                    narrative.append("")
        else:
            sql.append(line)

    flush()
    return cells


SHARED_INTRO_MD = (
    "# pg_xarray Demo Book\n"
    "\n"
    "A five-chapter tour: from \"I have some scientific files\" to "
    "\"my Postgres is an ML feature store.\"\n"
    "\n"
    "**Pre-requisites:**\n"
    "1. Postgres + PostGIS + pg_xarray installed "
    "(`cargo pgrx install --features \"reader-netcdf reader-grib\"`).\n"
    "2. The demo fixture files are committed under `demo/fixtures/` — "
    "no fixture-build step. The chapters hardcode the absolute path "
    "`/home/ubuntu/dev/pg_extensions/extensions/pg_xarray/demo/fixtures`. "
    "If your checkout lives elsewhere, search-and-replace that path "
    "across all chapter cells before running them. The path must be "
    "absolute because `fs://` URIs are resolved by the Postgres "
    "backend, not the client.\n"
    "\n"
    "**Source of truth:** the `.sql` files in this directory. This "
    "book is generated by `build_notebook.py` — re-run it after "
    "editing any chapter."
)

IPYNB_SETUP_CODE = (
    "# Optional: load jupysql to execute cells against Postgres.\n"
    "# Skip this cell if you'd rather copy SQL into your own client.\n"
    "%load_ext sql\n"
    "%config SqlMagic.autopandas = True\n"
    "%sql postgresql+psycopg2://postgres@localhost:5432/mydb\n"
)


def chapter_cells(here: Path) -> list[dict]:
    """Parse every chapter .sql under ``here`` and return the coalesced
    list of cells (markdown + code, one merged paragraph per markdown
    run)."""
    out: list[dict] = []
    for fname in CHAPTERS:
        chapter_path = here / fname
        if not chapter_path.exists():
            raise SystemExit(f"missing chapter: {chapter_path}")
        out.extend(parse_chapter(chapter_path))

    # Coalesce consecutive markdown cells — dividers in the .sql files
    # often force flushes when there's no SQL yet, leaving a string of
    # tiny one-line markdown cells. Merge them into single rich cells.
    coalesced: list[dict] = []
    for cell in out:
        if (
            cell["cell_type"] == "markdown"
            and coalesced
            and coalesced[-1]["cell_type"] == "markdown"
        ):
            prev = coalesced[-1]
            joined = "".join(prev["source"]).rstrip() + "\n\n" + "".join(cell["source"]).lstrip()
            prev["source"] = _split_lines(joined)
        else:
            coalesced.append(cell)
    return coalesced


def main() -> None:
    here = Path(__file__).resolve().parent
    chapters = chapter_cells(here)

    # ----- Jupyter .ipynb -----------------------------------------------------
    ipynb_cells: list[dict] = [
        md_cell(
            SHARED_INTRO_MD + "\n\n"
            "**Running cells:** install [jupysql] "
            "(`pip install jupysql sqlalchemy psycopg2-binary`) and run "
            "the setup cell below.\n\n"
            "[jupysql]: https://jupysql.ploomber.io/"
        ),
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": _split_lines(IPYNB_SETUP_CODE),
        },
    ] + chapters
    notebook = {
        "cells": ipynb_cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python", "version": "3.x"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    ipynb_path = here / "pg_xarray_demo.ipynb"
    ipynb_path.write_text(json.dumps(notebook, indent=1) + "\n")

    # ----- SQL Notebook .sqlbook ----------------------------------------------
    # Plain text — no Python kernel, so no setup cell. The user picks
    # their connection in the extension's UI.
    sqlbook_intro_md = (
        SHARED_INTRO_MD + "\n\n"
        "**Running cells:** the VS Code SQL Notebook extension connects "
        "via its own connection picker — no kernel setup needed. Each "
        "SQL cell runs against the chosen Postgres."
    )
    sqlbook_cells = [md_cell(sqlbook_intro_md)] + chapters
    sqlbook_path = here / "demo.sqlbook"
    sqlbook_path.write_text(emit_sqlbook(sqlbook_cells))

    print(
        f"Wrote {ipynb_path}\n"
        f"      {sqlbook_path}\n"
        f"  ({sum(1 for c in chapters if c['cell_type'] == 'code')} code cells, "
        f"{sum(1 for c in chapters if c['cell_type'] == 'markdown')} markdown cells per book)"
    )


def emit_sqlbook(cells: list[dict]) -> str:
    """Render cells in the VS Code SQL Notebook (.sqlbook) format —
    plain text with ``-- SQLBook: <CellType>`` cell delimiters.

    The cell-type word MUST match a ``vscode.NotebookCellKind`` enum
    name. The enum is `{Markup: 1, Code: 2}` — so markdown cells use
    ``-- SQLBook: Markup`` (not "Markdown"); code cells use
    ``-- SQLBook: Code``. Anything else silently falls back to Code via
    the extension's `?? NotebookCellKind.Code`, which is the bug we
    just hit.
    """
    out: list[str] = []
    for cell in cells:
        body = "".join(cell["source"]).rstrip()
        if cell["cell_type"] == "markdown":
            out.append("-- SQLBook: Markup")
            out.append(body)
        else:
            out.append("-- SQLBook: Code")
            out.append(body)
        out.append("")  # blank line between cells for readability
    return "\n".join(out).rstrip() + "\n"


if __name__ == "__main__":
    main()
