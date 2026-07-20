"""Generate the 13_orders_transactions_with_line_item_children functional fixture.

Builds a hand-crafted cassette (orders + order transactions enabled) containing one order
WITH transactions and a line item carrying 2+ taxLines and 2+ discountAllocations, replays
the component to produce the expected outputs, and writes the output snapshot.

Run from repo root:  uv run python tests/setup/gen_fixture_13.py
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

FIXTURE = REPO_ROOT / "tests" / "functional" / "13_orders_transactions_with_line_item_children"
SOURCE_DATA = FIXTURE / "source" / "data"
CASSETTE_DIR = SOURCE_DATA / "cassettes"
EXPECTED_OUT = FIXTURE / "expected" / "data" / "out"
COMPONENT_SCRIPT = REPO_ROOT / "src" / "component.py"

GRAPHQL_URI = "https://keboola-dummy.myshopify.com/admin/api/2025-10/graphql.json"
DOWNLOAD_URI = (
    "https://storage.googleapis.com/shopify-tiers-assets-prod-us-east1/"
    "bulk-operation-outputs/orders-transactions?GoogleAccessId=REDACTED&Expires=REDACTED&Signature=REDACTED"
)

CONFIG = {
    "parameters": {
        "#api_token": "DUMMY_TOKEN",
        "store_name": "keboola-dummy",
        "endpoints": {"orders": True, "order_transactions": True},
        "loading_options": {
            "date_since": "2025-01-01",
            "date_to": "2026-01-01",
            "fetch_parameter": "updated_at",
            "incremental_output": 1,
        },
    }
}


def _money(amount: str) -> dict:
    return {"shopMoney": {"amount": amount, "currencyCode": "EUR"}}


ORDER = {
    "id": "gid://shopify/Order/1301",
    "name": "#1301",
    "email": "buyer@example.com",
    "phone": None,
    "createdAt": "2025-07-01T10:00:00Z",
    "updatedAt": "2025-07-01T10:05:00Z",
    "processedAt": "2025-07-01T10:00:00Z",
    "cancelledAt": None,
    "cancelReason": None,
    "displayFinancialStatus": "PAID",
    "displayFulfillmentStatus": "FULFILLED",
    "test": False,
    "paymentGatewayNames": ["manual"],
    "totalPriceSet": _money("120.00"),
    "subtotalPriceSet": _money("110.00"),
    "totalTaxSet": _money("11.50"),
    "totalShippingPriceSet": _money("10.00"),
    "currentTotalAdditionalFeesSet": None,
    "currentTotalDutiesSet": None,
    "totalTipReceivedSet": _money("0.00"),
    "transactions": [
        {
            "id": "gid://shopify/OrderTransaction/7301",
            "kind": "SALE",
            "status": "SUCCESS",
            "test": False,
            "amountSet": _money("120.00"),
            "gateway": "manual",
            "createdAt": "2025-07-01T10:00:00Z",
            "processedAt": "2025-07-01T10:00:00Z",
            "errorCode": None,
            "authorizationCode": "AUTH123",
            "authorizationExpiresAt": None,
        },
        {
            "id": "gid://shopify/OrderTransaction/7302",
            "kind": "REFUND",
            "status": "SUCCESS",
            "test": False,
            "amountSet": _money("20.00"),
            "gateway": "manual",
            "createdAt": "2025-07-02T09:00:00Z",
            "processedAt": "2025-07-02T09:00:00Z",
            "errorCode": None,
            "authorizationCode": None,
            "authorizationExpiresAt": None,
        },
    ],
    "customer": {
        "id": "gid://shopify/Customer/5301",
        "firstName": "Buyer",
        "lastName": "Example",
        "email": "buyer@example.com",
        "phone": None,
    },
    "shippingAddress": {
        "firstName": "Buyer",
        "lastName": "Example",
        "address1": "1 Test Street",
        "address2": None,
        "city": "Prague",
        "province": None,
        "country": "Czechia",
        "zip": "11000",
        "phone": None,
    },
    "billingAddress": {
        "firstName": "Buyer",
        "lastName": "Example",
        "address1": "1 Test Street",
        "address2": None,
        "city": "Prague",
        "province": None,
        "country": "Czechia",
        "zip": "11000",
        "phone": None,
    },
}

LINE_ITEM = {
    "id": "gid://shopify/LineItem/2301",
    "title": "Widget A",
    "quantity": 2,
    "currentQuantity": 2,
    "sku": "WID-A",
    "originalUnitPriceSet": _money("50.00"),
    "variant": {
        "id": "gid://shopify/ProductVariant/3301",
        "title": "Widget A",
        "sku": "WID-A",
        "price": "50.00",
    },
    "taxLines": [
        {
            "title": "VAT 21%",
            "rate": 0.21,
            "ratePercentage": 21.0,
            "priceSet": _money("10.50"),
            "channelLiable": False,
            "source": "shopify",
        },
        {
            "title": "City Tax",
            "rate": 0.02,
            "ratePercentage": 2.0,
            "priceSet": _money("1.00"),
            "channelLiable": True,
            "source": "external",
        },
    ],
    "discountAllocations": [
        {"allocatedAmountSet": _money("5.00"), "discountApplication": {"index": 0}},
        {"allocatedAmountSet": _money("2.50"), "discountApplication": {"index": 1}},
    ],
    "__parentId": "gid://shopify/Order/1301",
}


def build_cassette() -> dict:
    jsonl = "\n".join(json.dumps(o) for o in (ORDER, LINE_ITEM)) + "\n"

    def graphql_request(body: str) -> dict:
        return {
            "body": body,
            "headers": {"Accept": ["application/json"], "Content-Type": ["application/json"]},
            "method": "POST",
            "uri": GRAPHQL_URI,
        }

    def json_response(payload: str) -> dict:
        return {
            "status": {"code": 200, "message": "OK"},
            "headers": {"Content-Type": ["application/json"]},
            "body": {"string": payload},
        }

    create_resp = json.dumps(
        {
            "data": {
                "bulkOperationRunQuery": {
                    "bulkOperation": {
                        "id": "gid://shopify/BulkOperation/9000000000021",
                        "status": "CREATED",
                    },
                    "userErrors": [],
                }
            }
        }
    )
    status_resp = json.dumps(
        {
            "data": {
                "currentBulkOperation": {
                    "id": "gid://shopify/BulkOperation/9000000000022",
                    "status": "COMPLETED",
                    "errorCode": None,
                    "createdAt": "2026-07-14T07:59:32Z",
                    "completedAt": "2026-07-14T07:59:33Z",
                    "objectCount": "2",
                    "fileSize": "3000",
                    "url": DOWNLOAD_URI,
                    "partialDataUrl": None,
                }
            }
        }
    )

    return {
        "_metadata": {
            "freeze_time": "2026-07-14T08:00:00",
            "keboola_vcr_version": "0.0.0",
            "recorded_at": "2026-07-14T08:00:00+00:00",
        },
        "interactions": [
            {
                "request": graphql_request('{"query": "mutation { bulkOperationRunQuery ... }"}'),
                "response": json_response(create_resp),
            },
            {
                "request": graphql_request('{"query": "query { currentBulkOperation { ... } }"}'),
                "response": json_response(status_resp),
            },
            {
                "request": {
                    "body": None,
                    "headers": {"Accept": ["*/*"]},
                    "method": "GET",
                    "uri": DOWNLOAD_URI,
                },
                "response": json_response(jsonl),
            },
        ],
        "version": 1,
    }


def main() -> None:
    # Reset output dirs but keep any git-tracked scaffolding fresh.
    CASSETTE_DIR.mkdir(parents=True, exist_ok=True)
    (SOURCE_DATA / "out").mkdir(parents=True, exist_ok=True)

    (SOURCE_DATA / "config.json").write_text(json.dumps(CONFIG, indent=2) + "\n")
    (CASSETTE_DIR / "requests.json").write_text(json.dumps(build_cassette(), indent=1))

    # Clean any previous output so the snapshot reflects a fresh run.
    out_dir = SOURCE_DATA / "out"
    if out_dir.exists():
        shutil.rmtree(out_dir)
    (out_dir / "tables").mkdir(parents=True, exist_ok=True)

    recorder = VCRRecorder.from_test_dir(test_data_dir=SOURCE_DATA, freeze_time_at="auto")

    def runner() -> None:
        os.environ["KBC_DATADIR"] = str(SOURCE_DATA)
        run_path(str(COMPONENT_SCRIPT), run_name="__main__")

    recorder.replay(runner)

    # Publish produced outputs as the expected fixture.
    if EXPECTED_OUT.exists():
        shutil.rmtree(EXPECTED_OUT)
    EXPECTED_OUT.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(out_dir, EXPECTED_OUT)

    save_output_snapshot(SOURCE_DATA)

    print("Generated fixture 13. Output tables:")
    for p in sorted((EXPECTED_OUT / "tables").glob("*.csv")):
        print("  ", p.name)


if __name__ == "__main__":
    main()
