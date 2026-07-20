"""Regenerate orders fixtures for the LineItem product_id + gift_card change (L1-141, PR 6).

For each affected fixture this:
  1. injects the two new lineItems fields (`product { id }`, `giftCard: isGiftCard`) into the
     recorded bulk-mutation request body so the cassette stays faithful to the query the
     component now sends,
  2. adds `product` and `giftCard` to every LineItem line of the recorded bulk JSONL download
     (scenario content otherwise unchanged), including a gift-card line item and a deleted-product
     (product = null) line item in fixture 12 to prove null-safety, and
  3. replays the component against the edited cassette to regenerate the expected output tables
     and the output snapshot.

Run from repo root:  uv run python tests/setup/regen_line_item_product_giftcard.py
"""

import json
import os
import shutil
import sys
from pathlib import Path
from runpy import run_path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))

from keboola.datadirtest.vcr import VCRRecorder, save_output_snapshot  # noqa: E402

FUNCTIONAL = REPO_ROOT / "tests" / "functional"
COMPONENT_SCRIPT = REPO_ROOT / "src" / "component.py"

FIXTURES = [
    "09_orders_real_data",
    "11_orders_customer_journey",
    "12_orders_line_item_children",
    "13_orders_transactions_with_line_item_children",
]

# The exact snippet added to the lineItems node selection in BulkOrders.graphql (kept in sync
# with the query file so the recorded request body matches what the component sends).
QUERY_VARIANT_BLOCK = (
    "                  variant {\n"
    "                    id\n"
    "                    title\n"
    "                    sku\n"
    "                    price\n"
    "                  }\n"
)
QUERY_VARIANT_BLOCK_WITH_FIELDS = QUERY_VARIANT_BLOCK + (
    "                  product {\n                    id\n                  }\n                  giftCard: isGiftCard\n"
)

# Per-line-item scenario for the new fields. Any LineItem not listed here gets a realistic
# product GID (derived below) and gift_card = false.
#   product = None  -> deleted product (product_id must still exist, empty)
#   gift_card = True -> gift card purchase (excluded from revenue per Shopify metric definitions)
LINE_ITEM_OVERRIDES = {
    "12_orders_line_item_children": {
        "gid://shopify/LineItem/2202": {"gift_card": True},
        "gid://shopify/LineItem/2203": {"product": None},
    },
}


def _default_product_id(line_item_id: str) -> str:
    num = int(line_item_id.rsplit("/", 1)[-1])
    return f"gid://shopify/Product/{num + 2000}"


def _rebuild_line_item(obj: dict, overrides: dict) -> dict:
    """Return the line item with product + giftCard inserted right after `variant`."""
    product = overrides["product"] if "product" in overrides else {"id": _default_product_id(obj["id"])}
    gift_card = overrides.get("gift_card", False)

    rebuilt: dict = {}
    for key, value in obj.items():
        rebuilt[key] = value
        if key == "variant":
            rebuilt["product"] = product
            rebuilt["giftCard"] = gift_card
    if "variant" not in obj:  # fallback: append before __parentId
        rebuilt = {}
        for key, value in obj.items():
            if key == "__parentId":
                rebuilt["product"] = product
                rebuilt["giftCard"] = gift_card
            rebuilt[key] = value
    return rebuilt


def _edit_cassette(cassette: dict, fixture: str) -> None:
    overrides = LINE_ITEM_OVERRIDES.get(fixture, {})
    for interaction in cassette["interactions"]:
        request = interaction["request"]
        body = request.get("body")

        # 1. Bulk-mutation request body: inject the new lineItems fields. The body is a JSON
        # string, so its newlines are escaped (\n); match/replace the escaped form to preserve
        # the recorded body byte-for-byte apart from the two added fields.
        if request["method"] == "POST" and body and "bulkOperationRunQuery" in body:
            escaped_old = QUERY_VARIANT_BLOCK.replace("\n", "\\n")
            escaped_new = QUERY_VARIANT_BLOCK_WITH_FIELDS.replace("\n", "\\n")
            if escaped_new not in body:
                request["body"] = body.replace(escaped_old, escaped_new, 1)

        # 2. Bulk JSONL download response: add product + giftCard to LineItem lines.
        if request["method"] == "GET" and "storage.googleapis" in request["uri"]:
            payload = interaction["response"]["body"]["string"]
            new_lines = []
            for line in payload.split("\n"):
                if not line.strip():
                    continue
                obj = json.loads(line)
                if "gid://shopify/LineItem/" in obj.get("id", ""):
                    obj = _rebuild_line_item(obj, overrides.get(obj["id"], {}))
                    new_lines.append(json.dumps(obj))
                else:
                    new_lines.append(line)
            interaction["response"]["body"]["string"] = "\n".join(new_lines) + "\n"


def regen_fixture(fixture: str) -> None:
    source_data = FUNCTIONAL / fixture / "source" / "data"
    cassette_path = source_data / "cassettes" / "requests.json"
    expected_out = FUNCTIONAL / fixture / "expected" / "data" / "out"

    cassette = json.loads(cassette_path.read_text())
    _edit_cassette(cassette, fixture)
    cassette_path.write_text(json.dumps(cassette, indent=1))

    out_dir = source_data / "out"
    if out_dir.exists():
        shutil.rmtree(out_dir)
    (out_dir / "tables").mkdir(parents=True, exist_ok=True)

    recorder = VCRRecorder.from_test_dir(test_data_dir=source_data, freeze_time_at="auto")

    def runner() -> None:
        os.environ["KBC_DATADIR"] = str(source_data)
        run_path(str(COMPONENT_SCRIPT), run_name="__main__")

    recorder.replay(runner)

    if expected_out.exists():
        shutil.rmtree(expected_out)
    expected_out.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(out_dir, expected_out)

    save_output_snapshot(source_data)

    print(f"Regenerated {fixture}. Output tables:")
    for p in sorted((expected_out / "tables").glob("*.csv")):
        print("  ", p.name)


def main() -> None:
    for fixture in FIXTURES:
        regen_fixture(fixture)


if __name__ == "__main__":
    main()
