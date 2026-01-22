# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Tests for namespace-aware SQL sessions backed by DataFusion catalogs.

These tests mirror the Rust tests in
rust/lance-namespace-datafusion/tests/namespace_sql.rs but exercise the
PyO3 bindings exposed via PyNamespaceSessionBuilder / PyNamespaceSession.
"""

from __future__ import annotations

import lance
import pyarrow as pa
import pytest
from lance.namespace import NamespaceSession
from lance_namespace import (
    CreateNamespaceRequest,
    connect,
)


def customers_data() -> pa.Table:
    return pa.table(
        {
            "customer_id": pa.array([1, 2, 3], type=pa.int32()),
            "name": pa.array(["Alice", "Bob", "Carol"], type=pa.string()),
            "city": pa.array(["NY", "SF", "LA"], type=pa.string()),
        }
    )


def orders_data() -> pa.Table:
    return pa.table(
        {
            "order_id": pa.array([101, 102, 103], type=pa.int32()),
            "customer_id": pa.array([1, 2, 3], type=pa.int32()),
            "amount": pa.array([100, 200, 300], type=pa.int32()),
        }
    )


def orders2_data() -> pa.Table:
    return pa.table(
        {
            "order_id": pa.array([201, 202], type=pa.int32()),
            "customer_id": pa.array([1, 2], type=pa.int32()),
            "amount": pa.array([150, 250], type=pa.int32()),
        }
    )


def customers_dim_data() -> pa.Table:
    return pa.table(
        {
            "customer_id": pa.array([1, 2, 3], type=pa.int32()),
            "segment": pa.array(["Silver", "Gold", "Platinum"], type=pa.string()),
        }
    )


@pytest.fixture(scope="module")
def populated_namespaces(tmp_path_factory):
    """Create two DirectoryNamespace trees and populate test tables.

    ns_root contains retail/sales customers & orders and wholesale/sales2/orders2.
    ns_crm contains crm/dim/customers_dim and is mounted as the "crm" catalog.
    """

    root_dir = tmp_path_factory.mktemp("ns_session_root")
    crm_dir = tmp_path_factory.mktemp("ns_session_crm")

    root_props = {
        "root": f"file://{root_dir}",
        "manifest_enabled": "true",
        "dir_listing_enabled": "true",
    }
    crm_props = {
        "root": f"file://{crm_dir}",
        "manifest_enabled": "true",
        "dir_listing_enabled": "true",
    }

    ns_root = connect("dir", root_props)
    ns_crm = connect("dir", crm_props)

    # Create nested namespaces in the root tree: retail/sales and wholesale/sales2.
    ns_root.create_namespace(CreateNamespaceRequest(id=["retail"]))
    ns_root.create_namespace(CreateNamespaceRequest(id=["retail", "sales"]))
    ns_root.create_namespace(CreateNamespaceRequest(id=["wholesale"]))
    ns_root.create_namespace(CreateNamespaceRequest(id=["wholesale", "sales2"]))

    # Create nested namespaces in the CRM tree: crm/dim.
    ns_crm.create_namespace(CreateNamespaceRequest(id=["crm"]))
    ns_crm.create_namespace(CreateNamespaceRequest(id=["crm", "dim"]))

    # Write datasets into the namespaces.
    lance.write_dataset(
        customers_data(),
        namespace=ns_root,
        table_id=["retail", "sales", "customers"],
        mode="create",
    )
    lance.write_dataset(
        orders_data(),
        namespace=ns_root,
        table_id=["retail", "sales", "orders"],
        mode="create",
    )
    lance.write_dataset(
        orders2_data(),
        namespace=ns_root,
        table_id=["wholesale", "sales2", "orders2"],
        mode="create",
    )
    lance.write_dataset(
        customers_dim_data(),
        namespace=ns_crm,
        table_id=["crm", "dim", "customers_dim"],
        mode="create",
    )

    yield ns_root, ns_crm


@pytest.fixture
def ns_session(populated_namespaces) -> NamespaceSession:
    ns_root, ns_crm = populated_namespaces
    return NamespaceSession(
        root=ns_root,
        catalogs={"crm": (ns_crm, ["crm"])},
    )


@pytest.mark.parametrize(
    "sql,exp0,exp1",
    [
        (
            "SELECT customers.name, orders.amount "
            "FROM retail.sales.customers customers "
            "JOIN retail.sales.orders orders "
            "  ON customers.customer_id = orders.customer_id "
            "WHERE customers.customer_id = 2",
            "Bob",
            200,
        ),
        (
            "SELECT c.name, o2.amount "
            "FROM retail.sales.customers c "
            "JOIN wholesale.sales2.orders2 o2 "
            "  ON c.customer_id = o2.customer_id "
            "WHERE o2.order_id = 202",
            "Bob",
            250,
        ),
        (
            "SELECT customers.name, dim.segment "
            "FROM retail.sales.customers customers "
            "JOIN crm.dim.customers_dim dim "
            "  ON customers.customer_id = dim.customer_id "
            "WHERE customers.customer_id = 3",
            "Carol",
            "Platinum",
        ),
    ],
)
def test_join_scenarios(ns_session: NamespaceSession, sql: str, exp0, exp1):
    """Exercise join queries across catalogs and schemas."""

    table = ns_session.sql(sql).combine_chunks()

    assert table.num_rows == 1
    assert table.num_columns == 2
    assert table.column(0)[0].as_py() == exp0
    assert table.column(1)[0].as_py() == exp1


def test_aggregation_city_totals(ns_session: NamespaceSession):
    """Aggregation with GROUP BY / ORDER BY over joined tables."""

    sql = (
        "SELECT city, SUM(amount) AS total "
        "FROM retail.sales.orders o "
        "JOIN retail.sales.customers c "
        "  ON c.customer_id = o.customer_id "
        "GROUP BY city "
        "ORDER BY city"
    )
    table = ns_session.sql(sql).combine_chunks()

    assert table.num_rows == 3
    cities = table.column(0)
    totals = table.column(1)

    assert list(cities.to_pylist()) == ["LA", "NY", "SF"]
    assert list(totals.to_pylist()) == [300, 100, 200]


def test_cte_view_customer_orders(ns_session: NamespaceSession):
    """CTE query to ensure more complex SQL syntax is supported."""

    sql = (
        "WITH customer_orders AS ( "
        "    SELECT c.customer_id, c.name, o.order_id, o.amount "
        "    FROM retail.sales.customers c "
        "    JOIN retail.sales.orders o "
        "      ON c.customer_id = o.customer_id "
        ") "
        "SELECT order_id, name, amount "
        "FROM customer_orders "
        "WHERE customer_id = 1"
    )
    table = ns_session.sql(sql).combine_chunks()

    assert table.num_rows == 1
    assert table.num_columns == 3

    order_id_col = table.column(0)
    name_col = table.column(1)
    amount_col = table.column(2)

    assert order_id_col[0].as_py() == 101
    assert name_col[0].as_py() == "Alice"
    assert amount_col[0].as_py() == 100


def test_invalid_table_reference(ns_session: NamespaceSession):
    """Selecting from a non-existent table should raise an error."""

    sql = "SELECT * FROM retail.sales.nonexistent"
    with pytest.raises(Exception):
        ns_session.sql(sql).combine_chunks()


def test_invalid_schema_reference(ns_session: NamespaceSession):
    """Selecting from a non-existent schema should raise an error."""

    sql = "SELECT * FROM bogus.orders"
    with pytest.raises(Exception):
        ns_session.sql(sql).combine_chunks()
