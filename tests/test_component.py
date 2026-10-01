import csv
import json
import logging
import os
import shutil
import tempfile
import time
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

import duckdb
from freezegun import freeze_time
from keboola.component.exceptions import UserException

import component
from component import (
    CUSTOMER_JOURNEY_SUMMARY_COLUMNS,
    Component,
    build_window_chunks,
    duckdb_memory_limit_for_container_mb,
    resolve_duckdb_memory_limit_mb,
)
from configuration import Configuration
from shopify_cli.client import ShopifyGraphQLClient


class TestParseLoadingOptionDates(unittest.TestCase):
    def setUp(self):
        with (
            mock.patch.dict(os.environ, {"KBC_DATADIR": "/tmp"}),
            mock.patch("component.Component.__init__", return_value=None),
        ):
            self.comp = Component.__new__(Component)

    @freeze_time("2026-03-19T13:56:13")
    def test_both_none_returns_none_tuple(self):
        result = self.comp._parse_loading_option_dates(None, None)
        self.assertEqual(result, (None, None))

    @freeze_time("2026-03-19T13:56:13")
    def test_absolute_dates(self):
        # Explicit bare-date user values still anchor to UTC midnight; both bounds are emitted as
        # full timestamps, so date-style configs produce the same window as before (L1-151).
        start, end = self.comp._parse_loading_option_dates("2026-03-12", "2026-03-19")
        self.assertEqual(start, "2026-03-12T00:00:00Z")
        self.assertEqual(end, "2026-03-19T00:00:00Z")

    @freeze_time("2026-03-19T13:56:13")
    def test_date_to_now_includes_same_day_records(self):
        # date_to="now" must resolve to the actual run moment (not midnight), so a record updated
        # earlier the same day (e.g. 2026-03-19T11:32:42Z) falls below the upper bound and is included.
        start, end = self.comp._parse_loading_option_dates("1 week ago", "now")
        self.assertEqual(start, "2026-03-12T13:56:13Z")
        self.assertEqual(end, "2026-03-19T13:56:13Z")
        self.assertGreater(end, "2026-03-19T11:32:42Z")

    @freeze_time("2026-03-19T13:56:13")
    def test_date_since_relative_resolves_to_full_timestamp(self):
        # Relative date_since resolves to the exact run time-of-day (no midnight flooring): a
        # 13:56 run of "12 hours ago" means exactly 12 hours back, not ~26 h to the prior midnight.
        start, end = self.comp._parse_loading_option_dates("12 hours ago", "now")
        self.assertEqual(start, "2026-03-19T01:56:13Z")
        self.assertEqual(end, "2026-03-19T13:56:13Z")

    @freeze_time("2026-03-19T13:56:13")
    def test_date_since_bare_date_unchanged(self):
        # Bare calendar dates parse to UTC midnight, so date-style configs are unaffected by the
        # timestamp change: the resolved lower bound is midnight of that day.
        start, _ = self.comp._parse_loading_option_dates("2026-03-12", "now")
        self.assertEqual(start, "2026-03-12T00:00:00Z")

    @freeze_time("2026-03-19T13:56:13")
    def test_only_date_since_set(self):
        start, end = self.comp._parse_loading_option_dates("2026-01-01", None)
        self.assertEqual(start, "2026-01-01T00:00:00Z")
        self.assertIsNone(end)

    @freeze_time("2026-03-19T13:56:13")
    def test_unset_date_to_emits_no_upper_bound(self):
        start, end = self.comp._parse_loading_option_dates("7 years ago", None)
        self.assertEqual(start, "2019-03-19T13:56:13Z")
        self.assertIsNone(end)

    @freeze_time("2026-03-19T13:56:13")
    def test_only_date_to_set(self):
        start, end = self.comp._parse_loading_option_dates(None, "2026-03-19")
        self.assertIsNone(start)
        self.assertEqual(end, "2026-03-19T00:00:00Z")

    def test_bounds_are_identical_across_timezones(self):
        # The emitted bounds must be a real UTC instant regardless of the container's TZ env
        # (dateparser resolves in UTC), not "correct only while the container runs UTC".
        results = []
        original_tz = os.environ.get("TZ")
        try:
            for tz in ("America/New_York", "Europe/Prague"):
                os.environ["TZ"] = tz
                time.tzset()
                with freeze_time("2026-03-19T13:56:13"):
                    results.append(self.comp._parse_loading_option_dates("7 years ago", "now"))
        finally:
            if original_tz is None:
                os.environ.pop("TZ", None)
            else:
                os.environ["TZ"] = original_tz
            time.tzset()
        self.assertEqual(results[0], results[1])
        self.assertEqual(results[0], ("2019-03-19T13:56:13Z", "2026-03-19T13:56:13Z"))


class TestGetPrimaryKey(unittest.TestCase):
    def setUp(self):
        with (
            mock.patch.dict(os.environ, {"KBC_DATADIR": "/tmp"}),
            mock.patch("component.Component.__init__", return_value=None),
        ):
            self.comp = Component.__new__(Component)

    def test_products_child_tables_have_id_primary_key(self):
        # metafield / product_variant / product_image are written incremental, so they must declare
        # a primary key or Storage appends on every run (L1-151, same bug class as line_item PR #26).
        self.assertEqual(self.comp._get_primary_key("metafield"), ["id"])
        self.assertEqual(self.comp._get_primary_key("product_variant"), ["id"])
        self.assertEqual(self.comp._get_primary_key("product_image"), ["id"])


class TestLegacyOrdersQueryBuilder(unittest.TestCase):
    """Pin the legacy get_orders query string: timestamp date bounds must be quoted
    (Shopify search uses ':' as its field separator) and the upper bound must be exclusive."""

    def _build_orders_query(self, date_since, date_to):
        from shopify_cli.client import ShopifyGraphQLClient

        with mock.patch.object(ShopifyGraphQLClient, "_setup_session", return_value=None):
            client = ShopifyGraphQLClient(
                store_name="test-shop", api_token="TEST_TOKEN", api_version="2025-10", debug=False
            )
        captured: dict[str, str] = {}

        def fake_paginate(query, root_field, batch_size):
            captured["query"] = query
            return iter([])

        with mock.patch.object(client, "_paginate", side_effect=fake_paginate):
            list(client.get_orders(date_since=date_since, date_to=date_to))
        return captured["query"]

    def test_legacy_orders_timestamp_bounds_quoted_and_exclusive(self):
        query = self._build_orders_query("2026-03-12", "2026-03-19T13:56:13Z")
        self.assertIn("created_at:>='2026-03-12'", query)
        self.assertIn("created_at:<'2026-03-19T13:56:13Z'", query)
        # Upper bound must be exclusive ('<'), never inclusive ('<='), matching the bulk paths.
        self.assertNotIn("created_at:<=", query)


class TestBulkOrdersQueryBuilder(unittest.TestCase):
    """Pin the bulk get_orders_bulk query string. The VCR functional cassettes match requests on
    method/host/path/query only (not body), so they cannot assert what date bound the bulk mutation
    actually carries; this captures the mutation sent to Shopify and asserts the timestamp date_to
    comes out quoted and exclusive."""

    def _build_bulk_orders_mutation(self, date_since, date_to):
        from shopify_cli.client import ShopifyGraphQLClient

        with mock.patch.object(ShopifyGraphQLClient, "_setup_session", return_value=None):
            client = ShopifyGraphQLClient(
                store_name="test-shop", api_token="TEST_TOKEN", api_version="2025-10", debug=False
            )
        captured: dict[str, str] = {}
        responses = iter(
            [
                {"bulkOperationRunQuery": {"bulkOperation": {"id": "gid://shopify/BulkOperation/1"}, "userErrors": []}},
                {"currentBulkOperation": {"status": "COMPLETED", "url": None, "objectCount": "0"}},
            ]
        )

        def fake_execute_query(query, variables=None):
            if "bulkOperationRunQuery" in query:
                captured["mutation"] = query
            return next(responses)

        with (
            mock.patch.object(client, "execute_query", side_effect=fake_execute_query),
            mock.patch("shopify_cli.client.time.sleep", return_value=None),
        ):
            client.get_orders_bulk(temp_file_path="/tmp/bulk_orders_test.jsonl", date_since=date_since, date_to=date_to)
        return captured["mutation"]

    def test_bulk_orders_timestamp_bound_quoted_and_exclusive(self):
        mutation = self._build_bulk_orders_mutation("2026-03-12", "2026-03-19T13:56:13Z")
        self.assertIn("updated_at:>='2026-03-12'", mutation)
        self.assertIn("updated_at:<'2026-03-19T13:56:13Z'", mutation)
        self.assertNotIn("updated_at:<=", mutation)


class TestComponent(unittest.TestCase):
    # set global time to 2010-10-10 - affects functions like datetime.now()
    @freeze_time("2010-10-10")
    # set KBC_DATADIR env to non-existing dir
    @mock.patch.dict(os.environ, {"KBC_DATADIR": "./non-existing-dir"})
    def test_run_no_cfg_fails(self):
        with self.assertRaises(ValueError):
            comp = Component()
            comp.run()

    def test_configuration_validation(self):
        """Test configuration validation"""
        # Test valid configuration
        valid_config = {
            "store_name": "test-shop",
            "#api_token": "TEST_TOKEN",
            "endpoints": {"orders": True, "products": True},
            "batch_size": 50,
        }

        config = Configuration(**valid_config)
        self.assertEqual(config.store_name, "test-shop")
        self.assertEqual(config.api_token, "TEST_TOKEN")
        self.assertTrue(config.endpoints.orders)
        self.assertTrue(config.endpoints.products)
        self.assertEqual(config.batch_size, 50)

    def test_configuration_get_enabled_endpoints(self):
        """Test getting enabled endpoints"""
        config_data = {
            "store_name": "test-shop",
            "#api_token": "TEST_TOKEN",
            "endpoints": {"orders": True, "products": True, "customers": False},
        }

        config = Configuration(**config_data)
        enabled = config.enabled_endpoints
        self.assertIn("orders", enabled)
        self.assertIn("products", enabled)
        self.assertNotIn("customers", enabled)

    def test_configuration_store_name_cleanup(self):
        """Test store name cleanup removes .myshopify.com"""
        config_data = {"store_name": "test-shop.myshopify.com", "#api_token": "TEST_TOKEN"}

        config = Configuration(**config_data)
        self.assertEqual(config.store_name, "test-shop")


class TestCustomerJourneyFlattening(unittest.TestCase):
    """Assert Order.customerJourneySummary is flattened to fixed columns on the order table.

    Reads the 11_orders_customer_journey functional fixture output, which covers three
    attribution cases: an attributed order (ready=true, both visits + UTM populated), an order
    with ready=false and null visits, and an order with customerJourneySummary null entirely.
    Every one of the flattened columns must be present in all three cases.
    """

    @classmethod
    def setUpClass(cls):
        order_csv = (
            Path(__file__).parent
            / "functional"
            / "11_orders_customer_journey"
            / "expected"
            / "data"
            / "out"
            / "tables"
            / "order.csv"
        )
        with open(order_csv, newline="") as f:
            reader = csv.DictReader(f)
            cls.header = reader.fieldnames
            cls.rows = {row["name"]: row for row in reader}

    def test_all_flattened_columns_present(self):
        expected_columns = [name for name, _ in CUSTOMER_JOURNEY_SUMMARY_COLUMNS]
        self.assertEqual(len(expected_columns), 29)
        for col in expected_columns:
            self.assertIn(col, self.header)
            for order_name, row in self.rows.items():
                self.assertIn(col, row, f"{col} missing for order {order_name}")

    def test_attributed_order_values(self):
        row = self.rows["#1001"]
        self.assertEqual(row["customer_journey_summary__ready"], "true")
        self.assertEqual(row["customer_journey_summary__first_visit__source"], "google")
        self.assertEqual(row["customer_journey_summary__first_visit__utm_parameters__campaign"], "brand")
        self.assertEqual(row["customer_journey_summary__last_visit__referral_code"], "INFL10")

    def test_not_ready_order_has_null_visits(self):
        row = self.rows["#1002"]
        self.assertEqual(row["customer_journey_summary__ready"], "false")
        self.assertEqual(row["customer_journey_summary__first_visit__landing_page"], "")
        self.assertEqual(row["customer_journey_summary__last_visit__source"], "")

    def test_null_summary_order_all_empty(self):
        row = self.rows["#1003"]
        for name, _ in CUSTOMER_JOURNEY_SUMMARY_COLUMNS:
            self.assertEqual(row[name], "", f"{name} should be empty when summary is null")


class TestResolveDuckdbMemoryLimit(unittest.TestCase):
    """The DuckDB memory budget must scale with the container, never shrink below 0.3.3."""

    @staticmethod
    def _cgroup(values: dict[str, str]):
        """Patch Path.read_text so only the given cgroup paths exist."""
        real_read_text = Path.read_text

        def fake_read_text(self, *args, **kwargs):
            key = str(self)
            if key in values:
                return values[key]
            if key in component.CGROUP_MEMORY_LIMIT_PATHS:
                raise FileNotFoundError(key)
            return real_read_text(self, *args, **kwargs)

        return mock.patch.object(Path, "read_text", fake_read_text)

    def test_small_backend_matches_previous_hardcoded_limit(self):
        # 512 MiB container (Keboola "small") must resolve to exactly the 320MB that
        # 0.3.3 hardcoded - this change must be a no-op on the default backend.
        with self._cgroup({"/sys/fs/cgroup/memory.max": str(512 * 1024 * 1024)}):
            self.assertEqual(resolve_duckdb_memory_limit_mb(), 320)

    def test_larger_backend_gets_proportionally_more(self):
        # 1 GiB container ("medium") gets 640MB instead of being capped at 320MB.
        with self._cgroup({"/sys/fs/cgroup/memory.max": str(1024 * 1024 * 1024)}):
            self.assertEqual(resolve_duckdb_memory_limit_mb(), 640)

    def test_cgroup_v1_is_read_when_v2_absent(self):
        with self._cgroup({"/sys/fs/cgroup/memory/memory.limit_in_bytes": str(2048 * 1024 * 1024)}):
            self.assertEqual(resolve_duckdb_memory_limit_mb(), 1280)

    def test_unlimited_cgroup_v2_falls_back_to_floor(self):
        with self._cgroup({"/sys/fs/cgroup/memory.max": "max"}):
            self.assertEqual(resolve_duckdb_memory_limit_mb(), 320)

    def test_cgroup_v1_unlimited_sentinel_falls_back_to_floor(self):
        # cgroup v1 reports "no limit" as a huge sentinel, which must not be trusted.
        with self._cgroup({"/sys/fs/cgroup/memory/memory.limit_in_bytes": "9223372036854771712"}):
            self.assertEqual(resolve_duckdb_memory_limit_mb(), 320)

    def test_implausibly_large_limit_falls_back_to_floor(self):
        # An unconstrained container reports host memory; refuse to size against it.
        with self._cgroup({"/sys/fs/cgroup/memory.max": str(64 * 1024 * 1024 * 1024)}):
            self.assertEqual(resolve_duckdb_memory_limit_mb(), 320)

    def test_unreadable_cgroup_falls_back_to_floor(self):
        with self._cgroup({}):
            self.assertEqual(resolve_duckdb_memory_limit_mb(), 320)

    def test_garbage_cgroup_value_falls_back_to_floor(self):
        with self._cgroup({"/sys/fs/cgroup/memory.max": "not-a-number"}):
            self.assertEqual(resolve_duckdb_memory_limit_mb(), 320)

    def test_tiny_container_never_drops_below_floor(self):
        # Smaller than "small" must still behave exactly as 0.3.3 did, not worse.
        with self._cgroup({"/sys/fs/cgroup/memory.max": str(256 * 1024 * 1024)}):
            self.assertEqual(resolve_duckdb_memory_limit_mb(), 320)

    def test_clamp_helper_is_pure_and_handles_undetected(self):
        # The clamp is separated from the I/O so the cgroup file is read exactly once.
        self.assertEqual(duckdb_memory_limit_for_container_mb(None), 320)
        self.assertEqual(duckdb_memory_limit_for_container_mb(512), 320)
        self.assertEqual(duckdb_memory_limit_for_container_mb(1024), 640)

    def test_emitted_setting_is_byte_identical_to_the_previous_literal(self):
        # End-to-end proof of the no-op claim: on a 512 MiB container the value DuckDB
        # actually ends up with must equal what `SET memory_limit='320MB'` produced.
        with self._cgroup({"/sys/fs/cgroup/memory.max": str(512 * 1024 * 1024)}):
            emitted = f"{resolve_duckdb_memory_limit_mb()}MB"
        self.assertEqual(emitted, "320MB")

        with duckdb.connect(":memory:") as conn:
            conn.execute("SET memory_limit='320MB'")
            previous = conn.execute("SELECT current_setting('memory_limit')").fetchone()[0]
            conn.execute(f"SET memory_limit='{emitted}'")
            current = conn.execute("SELECT current_setting('memory_limit')").fetchone()[0]
        self.assertEqual(current, previous)


class TestBuildWindowChunks(unittest.TestCase):
    """Chunk boundaries must tile the window exactly: no gap, no overlap, no lost upper bound."""

    NOW = "2026-03-31T12:00:00Z"

    def test_disabled_returns_the_original_window_unchanged(self):
        # chunk_size_days=0 is the default, and must reproduce today's single request.
        self.assertEqual(
            build_window_chunks("2026-01-01T00:00:00Z", "2026-03-01T00:00:00Z", 0, self.NOW),
            [("2026-01-01T00:00:00Z", "2026-03-01T00:00:00Z")],
        )

    def test_window_shorter_than_one_chunk_is_a_single_chunk(self):
        self.assertEqual(
            build_window_chunks("2026-01-01T00:00:00Z", "2026-01-10T00:00:00Z", 30, self.NOW),
            [("2026-01-01T00:00:00Z", "2026-01-10T00:00:00Z")],
        )

    def test_window_splits_into_contiguous_half_open_chunks(self):
        chunks = build_window_chunks("2026-01-01T00:00:00Z", "2026-03-02T00:00:00Z", 30, self.NOW)
        self.assertEqual(
            chunks,
            [
                ("2026-01-01T00:00:00Z", "2026-01-31T00:00:00Z"),
                ("2026-01-31T00:00:00Z", "2026-03-02T00:00:00Z"),
            ],
        )
        # No gap and no overlap: every chunk's upper bound is the next chunk's lower bound.
        for earlier, later in zip(chunks, chunks[1:]):
            self.assertEqual(earlier[1], later[0])
        # The tiling covers exactly the requested window.
        self.assertEqual(chunks[0][0], "2026-01-01T00:00:00Z")
        self.assertEqual(chunks[-1][1], "2026-03-02T00:00:00Z")

    def test_unset_upper_bound_is_preserved_on_the_final_chunk(self):
        # date_to=None means "no upper bound", so records updated during the run are still
        # caught. The run moment may only lay boundaries out, never become an emitted bound.
        chunks = build_window_chunks("2026-01-01T00:00:00Z", None, 30, self.NOW)
        self.assertGreater(len(chunks), 1)
        self.assertIsNone(chunks[-1][1])
        for _, upper in chunks[:-1]:
            self.assertIsNotNone(upper)

    def test_unset_lower_bound_cannot_be_chunked(self):
        # There is no anchor to step from, so the window is fetched in one pass.
        self.assertEqual(
            build_window_chunks(None, "2026-03-01T00:00:00Z", 30, self.NOW), [(None, "2026-03-01T00:00:00Z")]
        )

    def test_inverted_window_is_left_alone(self):
        self.assertEqual(
            build_window_chunks("2026-03-01T00:00:00Z", "2026-01-01T00:00:00Z", 30, self.NOW),
            [("2026-03-01T00:00:00Z", "2026-01-01T00:00:00Z")],
        )

    def test_chunk_bounds_keep_the_shopify_timestamp_format(self):
        for lower, upper in build_window_chunks("2026-01-01T06:03:00Z", "2026-02-20T06:03:00Z", 7, self.NOW):
            for bound in (lower, upper):
                if bound is not None:
                    datetime.strptime(bound, "%Y-%m-%dT%H:%M:%SZ")

    def test_sub_day_precision_is_carried_through(self):
        chunks = build_window_chunks("2026-01-01T06:03:00Z", "2026-01-03T06:03:00Z", 1, self.NOW)
        self.assertEqual(
            chunks,
            [("2026-01-01T06:03:00Z", "2026-01-02T06:03:00Z"), ("2026-01-02T06:03:00Z", "2026-01-03T06:03:00Z")],
        )


class TestChunkedBulkLoadEquivalence(unittest.TestCase):
    """Loading N chunk files must produce what loading the concatenation produces.

    This is the correctness gate for chunking: if the pinned-union load drops a column,
    narrows a STRUCT, or coerces a type differently, a chunked run silently returns
    different data from an unchunked one.

    Scope note: these fixtures are small, so they compare the two loads below DuckDB's
    default JSON sample size. Above it the single-file load samples only the first 20480
    rows while the chunked load samples every row, so the chunked load can legitimately
    find a column or widen a type where the single-file load raises instead. That
    divergence is intentional and documented in the CHANGELOG.
    """

    # Chunk 2 widens the STRUCT, adds a top-level column, and conflicts on a scalar type.
    CHUNK_1 = [
        {"id": "gid://shopify/Order/1", "note": "a", "money": {"amount": 1}, "tags": ["x"]},
        {"id": "gid://shopify/Order/2", "note": None, "money": {"amount": 2}, "tags": []},
    ]
    CHUNK_2 = [
        {"id": "gid://shopify/Order/3", "note": "c", "money": {"amount": 3, "currency": "EUR"}, "extra": 7},
        {"id": "gid://shopify/Order/4", "note": "d", "money": {"amount": "4"}, "tags": ["y", "z"], "extra": 8},
    ]

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.paths = []
        for index, rows in enumerate((self.CHUNK_1, self.CHUNK_2), start=1):
            path = os.path.join(self.tmp, f"chunk{index}.jsonl")
            with open(path, "w") as f:
                for row in rows:
                    f.write(json.dumps(row) + "\n")
            self.paths.append(path)
        self.combined = os.path.join(self.tmp, "combined.jsonl")
        with open(self.combined, "w") as f:
            for rows in (self.CHUNK_1, self.CHUNK_2):
                for row in rows:
                    f.write(json.dumps(row) + "\n")

        with (
            mock.patch.dict(os.environ, {"KBC_DATADIR": "/tmp"}),
            mock.patch("component.Component.__init__", return_value=None),
        ):
            self.comp = Component.__new__(Component)
        self.comp.conn = duckdb.connect(":memory:")

    def tearDown(self):
        self.comp.conn.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _describe(self, table):
        return self.comp.conn.execute(f'DESCRIBE "{table}"').fetchall()

    def _rows(self, table):
        columns = [c[0] for c in self._describe(table)]
        order = ", ".join(f'"{c}"' for c in columns)
        return self.comp.conn.execute(f'SELECT {order} FROM "{table}" ORDER BY "id"').fetchall()

    def test_chunked_load_matches_single_file_load(self):
        self.comp._load_bulk_files("chunked", self.paths)
        self.comp._load_bulk_files("single", [self.combined])

        self.assertEqual(
            self._describe("chunked"), self._describe("single"), "chunked load inferred a different schema"
        )
        self.assertEqual(self._rows("chunked"), self._rows("single"), "chunked load produced different rows")

    def test_no_column_is_dropped_by_the_chunked_load(self):
        # A column that appears only in a later chunk must survive; an explicit column spec
        # silently drops keys it does not name, so this is the regression that matters.
        self.comp._load_bulk_files("chunked", self.paths)
        columns = {c[0] for c in self._describe("chunked")}
        self.assertIn("extra", columns)
        self.assertIn("tags", columns)

    def test_single_file_uses_the_original_unchanged_statement(self):
        # The one-chunk path must not drift onto the new machinery, otherwise every
        # existing config silently changes its load semantics.
        executed = []
        real_conn = self.comp.conn

        class RecordingConnection:
            def __init__(self, inner):
                self._inner = inner

            def execute(self, sql, *args, **kwargs):
                executed.append(sql)
                return self._inner.execute(sql, *args, **kwargs)

        self.comp.conn = RecordingConnection(real_conn)
        try:
            self.comp._load_bulk_files("single", [self.combined])
        finally:
            self.comp.conn = real_conn

        self.assertEqual(
            executed[-1],
            f"CREATE TABLE \"single\" AS SELECT * FROM read_json_auto('{self.combined}')",
        )
        self.assertFalse(any("union_by_name" in sql for sql in executed))


class TestChunkOverlapDeduplication(unittest.TestCase):
    """A record returned by two chunks must reach the output table once, as its newest copy."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        with (
            mock.patch.dict(os.environ, {"KBC_DATADIR": "/tmp"}),
            mock.patch("component.Component.__init__", return_value=None),
        ):
            self.comp = Component.__new__(Component)
        self.comp.conn = duckdb.connect(":memory:")
        self.comp.logger = logging.getLogger("test")

    def tearDown(self):
        self.comp.conn.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _write(self, name, rows):
        path = os.path.join(self.tmp, name)
        with open(path, "w") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")
        return path

    def test_record_returned_by_two_chunks_is_kept_once_as_the_newer_copy(self):
        # Order 1 is updated again while chunk 2 is being fetched, so it matches both
        # windows. Without deduplication a full load would emit it twice.
        first = self._write("c1.jsonl", [{"id": "gid://shopify/Order/1", "note": "old"}])
        second = self._write(
            "c2.jsonl",
            [{"id": "gid://shopify/Order/1", "note": "new"}, {"id": "gid://shopify/Order/2", "note": "b"}],
        )
        self.comp._load_bulk_files("t", [first, second])

        rows = self.comp.conn.execute('SELECT "id", "note" FROM "t" ORDER BY "id"').fetchall()
        self.assertEqual(rows, [("gid://shopify/Order/1", "new"), ("gid://shopify/Order/2", "b")])

    def test_helper_column_never_survives_into_the_table(self):
        # __chunk_index must not reach _normalize_table, or it becomes an output column.
        first = self._write("c1.jsonl", [{"id": "gid://shopify/Order/1"}])
        second = self._write("c2.jsonl", [{"id": "gid://shopify/Order/2"}])
        for paths in ([first], [first, second]):
            self.comp._load_bulk_files("t", paths)
            columns = {c[0] for c in self.comp.conn.execute('DESCRIBE "t"').fetchall()}
            self.assertNotIn("__chunk_index", columns)

    def test_rows_without_an_id_are_not_collapsed_together(self):
        first = self._write("c1.jsonl", [{"id": None, "note": "x"}])
        second = self._write("c2.jsonl", [{"id": None, "note": "y"}])
        self.comp._load_bulk_files("t", [first, second])
        self.assertEqual(self.comp.conn.execute('SELECT COUNT(*) FROM "t"').fetchone()[0], 2)

    def test_distinct_records_across_chunks_are_all_kept(self):
        first = self._write("c1.jsonl", [{"id": "gid://shopify/Order/1"}])
        second = self._write("c2.jsonl", [{"id": "gid://shopify/Order/2"}])
        self.comp._load_bulk_files("t", [first, second])
        self.assertEqual(self.comp.conn.execute('SELECT COUNT(*) FROM "t"').fetchone()[0], 2)


class TestFetchBulkWindows(unittest.TestCase):
    """The fetch loop must issue exactly one request per chunk and drop empty results."""

    def setUp(self):
        with (
            mock.patch.dict(os.environ, {"KBC_DATADIR": "/tmp"}),
            mock.patch("component.Component.__init__", return_value=None),
        ):
            self.comp = Component.__new__(Component)
        self.comp.logger = logging.getLogger("test")

    def _params(self, chunk_size_days):
        loading_options = mock.Mock(chunk_size_days=chunk_size_days)
        self.comp.params = mock.Mock(loading_options=loading_options)

    @staticmethod
    def _fetch(calls, item_count=1):
        def fetch(path, since, to):
            calls.append((since, to))
            Path(path).write_text("")
            return mock.Mock(file_path=path, item_count=item_count, api_wait_time=0.0, download_time=0.0)

        return fetch

    def test_chunking_off_issues_one_request_with_the_original_bounds(self):
        self._params(0)
        calls = []
        results = self.comp._fetch_bulk_windows(
            self._fetch(calls), "orders", "2026-01-01T00:00:00Z", "2026-06-01T00:00:00Z"
        )
        self.assertEqual(calls, [("2026-01-01T00:00:00Z", "2026-06-01T00:00:00Z")])
        self.assertEqual(len(results), 1)
        for result in results:
            Path(result.file_path).unlink(missing_ok=True)

    def test_chunking_on_issues_one_request_per_chunk(self):
        self._params(30)
        calls = []
        results = self.comp._fetch_bulk_windows(
            self._fetch(calls), "orders", "2026-01-01T00:00:00Z", "2026-03-02T00:00:00Z"
        )
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0][1], calls[1][0])
        self.assertEqual(len(results), 2)
        for result in results:
            Path(result.file_path).unlink(missing_ok=True)

    def test_empty_chunks_are_dropped_and_their_temp_files_removed(self):
        self._params(30)
        calls = []
        paths = []

        def fetch(path, since, to):
            calls.append((since, to))
            paths.append(path)
            Path(path).write_text("")
            return mock.Mock(file_path=path, item_count=0, api_wait_time=0.0, download_time=0.0)

        results = self.comp._fetch_bulk_windows(fetch, "orders", "2026-01-01T00:00:00Z", "2026-03-02T00:00:00Z")
        self.assertEqual(results, [])
        for path in paths:
            self.assertFalse(Path(path).exists(), "empty chunk left a temp file behind")

    def test_missing_period_start_falls_back_to_a_single_request(self):
        self._params(30)
        calls = []
        results = self.comp._fetch_bulk_windows(self._fetch(calls), "orders", None, "2026-06-01T00:00:00Z")
        self.assertEqual(calls, [(None, "2026-06-01T00:00:00Z")])
        for result in results:
            Path(result.file_path).unlink(missing_ok=True)


class TestBulkSlotContention(unittest.TestCase):
    """A bulk operation left behind by a cancelled job must not fail every later run."""

    def setUp(self):
        self.client = ShopifyGraphQLClient.__new__(ShopifyGraphQLClient)
        self.client.logger = logging.getLogger("test")

    @staticmethod
    def _busy(operation_id="gid://shopify/BulkOperation/1"):
        return {
            "bulkOperationRunQuery": {
                "bulkOperation": None,
                "userErrors": [
                    {
                        "field": None,
                        "message": f"A bulk query operation for this app and shop is already in progress: {operation_id}.",
                    }
                ],
            }
        }

    @staticmethod
    def _started(operation_id="gid://shopify/BulkOperation/2"):
        return {"bulkOperationRunQuery": {"bulkOperation": {"id": operation_id}, "userErrors": []}}

    def test_busy_error_is_recognised(self):
        self.assertTrue(ShopifyGraphQLClient._is_bulk_slot_busy(self._busy()["bulkOperationRunQuery"]["userErrors"]))
        self.assertFalse(ShopifyGraphQLClient._is_bulk_slot_busy([{"message": "Something else went wrong"}]))
        self.assertFalse(ShopifyGraphQLClient._is_bulk_slot_busy([]))

    def test_free_slot_submits_exactly_once(self):
        # The happy path must not make any extra request, or every cassette would break.
        self.client.execute_query = mock.Mock(return_value=self._started())
        bulk_op = self.client._start_bulk_operation("mutation {}")
        self.assertEqual(bulk_op, {"id": "gid://shopify/BulkOperation/2"})
        self.assertEqual(self.client.execute_query.call_count, 1)

    def test_busy_slot_is_waited_out_then_the_operation_starts(self):
        self.client._load_bulk_status_query = mock.Mock(return_value="query {}")
        self.client.execute_query = mock.Mock(
            side_effect=[
                self._busy(),  # first submit is blocked
                {"currentBulkOperation": {"id": "gid://shopify/BulkOperation/1", "status": "COMPLETED"}},
                self._started(),  # retry succeeds
            ]
        )
        with mock.patch("shopify_cli.client.time.sleep"):
            bulk_op = self.client._start_bulk_operation("mutation {}")
        self.assertEqual(bulk_op, {"id": "gid://shopify/BulkOperation/2"})
        self.assertEqual(self.client.execute_query.call_count, 3)

    def test_blocking_operation_is_never_cancelled(self):
        # Cancelling could kill another configuration's run, or a production run.
        self.client._load_bulk_status_query = mock.Mock(return_value="query {}")
        self.client.execute_query = mock.Mock(
            side_effect=[
                self._busy(),
                {"currentBulkOperation": {"id": "gid://shopify/BulkOperation/1", "status": "COMPLETED"}},
                self._started(),
            ]
        )
        with mock.patch("shopify_cli.client.time.sleep"):
            self.client._start_bulk_operation("mutation {}")
        sent = " ".join(str(call) for call in self.client.execute_query.call_args_list)
        self.assertNotIn("bulkOperationCancel", sent)

    def test_wait_gives_up_with_an_actionable_error(self):
        self.client._load_bulk_status_query = mock.Mock(return_value="query {}")
        self.client.execute_query = mock.Mock(
            return_value={"currentBulkOperation": {"id": "gid://shopify/BulkOperation/1", "status": "RUNNING"}}
        )
        # Freeze sleep and jump the clock past the timeout on the first poll.
        with (
            mock.patch("shopify_cli.client.time.sleep"),
            mock.patch("shopify_cli.client.time.time", side_effect=[0, 0, 10_000, 10_000]),
            self.assertRaises(UserException) as ctx,
        ):
            self.client._wait_for_free_bulk_slot([{"message": "already in progress"}])
        self.assertIn("bulkOperationCancel", str(ctx.exception))

    def test_other_user_errors_still_fail_immediately(self):
        self.client.execute_query = mock.Mock(
            return_value={"bulkOperationRunQuery": {"bulkOperation": None, "userErrors": [{"message": "Bad query"}]}}
        )
        with self.assertRaises(UserException) as ctx:
            self.client._start_bulk_operation("mutation {}")
        self.assertIn("Bad query", str(ctx.exception))
        self.assertEqual(self.client.execute_query.call_count, 1)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
