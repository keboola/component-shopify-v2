"""Rewrite recorded bulk-operation polls from `currentBulkOperation` to `node(id:)` (CFTL-840).

The client now polls its own bulk operation by id instead of the app's most recent one. Every
cassette recorded the old poll, so its response sits under `data.currentBulkOperation` and the
client no longer finds it. This rewrites only those poll interactions: the request becomes the
`BulkOperationById` query with the operation id as its variable, and the response key becomes
`node`. The BulkOperation fields Shopify returns are the same for both queries, so expected
outputs stay byte-identical. The other interactions are left untouched.

A live re-record was not usable: the test store no longer holds the data in most test windows.

Run from repo root:  uv run python tests/setup/migrate_bulk_poll_to_node.py
"""

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))

from shopify_cli.query_loader import QueryLoader  # noqa: E402

FUNCTIONAL_DIR = REPO_ROOT / "tests" / "functional"
POLL_QUERY = QueryLoader().load_query("BulkOperationById")


def migrate_interaction(interaction: dict) -> dict:
    response = json.loads(interaction["response"]["body"]["string"])
    operation = response["data"].pop("currentBulkOperation")
    response["data"]["node"] = operation
    interaction["response"]["body"]["string"] = json.dumps(response, separators=(",", ":"))

    body = json.dumps({"query": POLL_QUERY, "variables": {"id": operation["id"]}, "operationName": None})
    interaction["request"]["body"] = body
    interaction["request"]["headers"]["Content-Length"] = [str(len(body.encode()))]
    return interaction


def migrate_cassette(path: Path) -> int:
    """Cassettes come in two layouts: one interaction per line, or fully indented JSON."""
    text = path.read_text()
    if '\n    {"request"' in text:
        return migrate_line_per_interaction(path, text)
    return migrate_indented(path, text)


def migrate_indented(path: Path, text: str) -> int:
    cassette = json.loads(text)
    polls = [i for i in cassette["interactions"] if "currentBulkOperation" in (i["request"]["body"] or "")]
    if not polls:
        return 0
    trailing_newline = "\n" if text.endswith("\n") else ""
    layouts = [{"indent": n, "ensure_ascii": a} for n in (1, 2, 4) for a in (True, False)]
    layout = next((kw for kw in layouts if json.dumps(cassette, **kw) + trailing_newline == text), None)
    if layout is None:
        raise ValueError(f"{path}: layout does not round-trip, refusing to rewrite it")
    for interaction in polls:
        migrate_interaction(interaction)
    path.write_text(json.dumps(cassette, **layout) + trailing_newline)
    return len(polls)


def migrate_line_per_interaction(path: Path, text: str) -> int:
    lines = text.split("\n")
    migrated = 0
    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped.startswith('{"request"') or "currentBulkOperation" not in stripped:
            continue
        trailing_comma = stripped.endswith(",")
        interaction = migrate_interaction(json.loads(stripped.rstrip(",")))
        indent = line[: len(line) - len(line.lstrip())]
        lines[i] = indent + json.dumps(interaction) + ("," if trailing_comma else "")
        migrated += 1
    if migrated:
        path.write_text("\n".join(lines))
    return migrated


def main() -> None:
    for cassette in sorted(FUNCTIONAL_DIR.glob("*/source/data/cassettes/requests.json")):
        print(f"{cassette.parts[-5]}: {migrate_cassette(cassette)} poll(s) migrated")


if __name__ == "__main__":
    main()
