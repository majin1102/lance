# Lance Namespace-DataFusion Integration

This crate provides a bridge between Lance Namespaces and Apache DataFusion, allowing Lance tables to be queried as if they were native DataFusion catalogs, schemas, and tables.

It exposes a `SessionBuilder` that constructs a DataFusion `SessionContext` with `CatalogProvider` and `SchemaProvider` implementations backed by a `lance_namespace::LanceNamespace` instance.

## Features

- **Dynamic Catalogs**: Maps top-level Lance namespaces to DataFusion catalogs.
- **Dynamic Schemas**: Maps child namespaces to DataFusion schemas.
- **Lazy Table Loading**: Tables are loaded on-demand from the namespace when queried.
- **Read-Only**: This integration focuses solely on providing read access (SQL `SELECT`) to Lance datasets. DML operations are not included.

## API usage

### Rust

First, build one or more `LanceNamespace` instances (for example directory-backed namespaces), then use `SessionBuilder` to construct a `SessionContext` that mounts them as a root and named catalogs.

```rust,ignore
use std::sync::Arc;
use datafusion::prelude::SessionContext;
use lance_namespace::LanceNamespace;
use lance_namespace_impls::DirectoryNamespaceBuilder;
use lance_namespace_datafusion::SessionBuilder;

async fn run_query() -> datafusion::error::Result<()> {
    // Build two namespaces: a root tree and a separate "crm" tree.
    let root_ns: Arc<dyn LanceNamespace> = Arc::new(
        DirectoryNamespaceBuilder::new("memory://root".to_string())
            .build()
            .await?,
    );
    let crm_ns: Arc<dyn LanceNamespace> = Arc::new(
        DirectoryNamespaceBuilder::new("memory://crm".to_string())
            .build()
            .await?,
    );

    // Construct a SessionContext with a root and an explicit "crm" catalog.
    let ctx: SessionContext = SessionBuilder::new()
        .with_root(root_ns.clone().into())
        .add_catalog("crm", (crm_ns.clone(), vec!["crm".to_string()]).into())
        .build()
        .await?;

    let df = ctx
        .sql(
            "SELECT c.customer_id, c.name, dim.segment \
             FROM retail.sales.customers AS c \
             JOIN crm.dim.customers_dim AS dim \
               ON c.customer_id = dim.customer_id",
        )
        .await?;
    df.show().await?;
    Ok(())
}
```

### Python

The same integration is exposed to Python via the `pylance` extension. The recommended API is the high-level `NamespaceSession` wrapper from `lance.namespace`. Its `sql()` method returns a `pyarrow.Table`.

```python
import lance.namespace
import lance_namespace

# Build namespaces (for example, directory-backed) using lance-namespace
root_ns = lance_namespace.connect("dir", {"root": "memory://root"})
crm_ns = lance_namespace.connect("dir", {"root": "memory://crm"})

session = lance.namespace.NamespaceSession(
    root=root_ns,
    catalogs={"crm": (crm_ns, ["crm"])},
    default_catalog="retail",
    default_schema="sales",
)

table = session.sql(
    "SELECT c.customer_id, c.name, dim.segment "
    "FROM retail.sales.customers AS c "
    "JOIN crm.dim.customers_dim AS dim "
    "  ON c.customer_id = dim.customer_id"
)

print(table)  # table is a pyarrow.Table
```

This shows how to build a `SessionContext` / `NamespaceSession` from Lance namespaces using `with_root` and `add_catalog`, then run a simple SQL query against `catalog.schema.table` identifiers backed by this crate's DataFusion integration.
