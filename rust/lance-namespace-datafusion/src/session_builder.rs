// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use crate::catalog::LanceCatalogProviderList;
use crate::namespace_level::NamespaceLevel;
use crate::url_factory::{LanceUrlTableFactory, MultiUrlTableFactory};
use crate::LanceCatalogProvider;
use datafusion::catalog::{CatalogProvider, DynamicFileCatalog, SchemaProvider, UrlTableFactory};
use datafusion::datasource::dynamic_file::DynamicListTableFactory;
use datafusion::error::Result;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion_session::SessionStore;
use std::sync::Arc;

/// Builder for configuring a `SessionContext` with Lance namespaces.
#[derive(Clone, Debug, Default)]
pub struct SessionBuilder {
    /// Optional root namespace exposed via a dynamic
    /// `LanceCatalogProviderList`.
    root: Option<NamespaceLevel>,
    /// Explicit catalogs to register by name.
    catalogs: Vec<(String, NamespaceLevel)>,
    /// Optional DataFusion session configuration.
    config: Option<SessionConfig>,
    /// Optional default catalog name.
    /// It will override the default catalog name in [`SessionBuilder::config`] if set
    default_catalog: Option<String>,
    /// Optional default catalog provider.
    default_catalog_provider: Option<Arc<dyn CatalogProvider>>,
    /// Optional default schema name.
    /// It will override the default schema name in [`SessionBuilder::config`] if set
    default_schema: Option<String>,
    /// Optional default schema provider.
    default_schema_provider: Option<Arc<dyn SchemaProvider>>,
}

impl SessionBuilder {
    /// Create a new builder with no namespaces or configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach a root `LanceNamespace` that is exposed as a dynamic
    /// catalog list via `LanceCatalogProviderList`.
    pub fn with_root(mut self, ns: NamespaceLevel) -> Self {
        self.root = Some(ns);
        self
    }

    /// Register an additional catalog backed by the given namespace.
    ///
    /// The catalog is identified by `name` and can later be combined
    /// with schemas via `SessionBuilder::add_schema` using the same
    /// namespace.
    pub fn add_catalog(mut self, name: &str, ns: NamespaceLevel) -> Self {
        self.catalogs.push((name.to_string(), ns));
        self
    }

    /// Provide an explicit `SessionConfig` for the underlying
    /// `SessionContext`.
    pub fn with_config(mut self, config: SessionConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Override the default catalog name used by the session.
    pub fn with_default_catalog(
        mut self,
        name: &str,
        catalog_provider: Option<Arc<dyn CatalogProvider>>,
    ) -> Self {
        self.default_catalog = Some(name.to_string());
        self.default_catalog_provider = catalog_provider;
        self
    }

    /// Override the default schema name used by the session.
    pub fn with_default_schema(
        mut self,
        name: &str,
        schema_provider: Option<Arc<dyn SchemaProvider>>,
    ) -> Self {
        self.default_schema = Some(name.to_string());
        self.default_schema_provider = schema_provider;
        self
    }

    /// Build a `SessionContext` with all configured namespaces.
    pub async fn build(self) -> Result<SessionContext> {
        self.check_params_valid()?;
        let config = self.config.unwrap_or_default();
        let options = config.options();
        let default_catalog = self
            .default_catalog
            .unwrap_or_else(|| options.catalog.default_catalog.clone());
        let default_schema = self
            .default_schema
            .unwrap_or_else(|| options.catalog.default_schema.clone());

        let ctx = SessionContext::new_with_config(
            config
                .with_default_catalog_and_schema(default_catalog.as_str(), default_schema.as_str()),
        );

        if let Some(root) = self.root {
            let catalog_list = Arc::new(LanceCatalogProviderList::try_new(root).await?);
            ctx.register_catalog_list(catalog_list);
        }

        for (catalog_name, namespace) in self.catalogs {
            ctx.register_catalog(
                catalog_name,
                Arc::new(LanceCatalogProvider::try_new(namespace).await?),
            );
        }
        if let Some(catalog_provider) = self.default_catalog_provider {
            if let Some(schema_provider) = self.default_schema_provider {
                catalog_provider.register_schema(default_schema.as_str(), schema_provider)?;
            }
            ctx.register_catalog(default_catalog.as_str(), catalog_provider);
        }

        Ok(ctx)
    }

    fn check_params_valid(&self) -> Result<()> {
        if let (None, Some(schema)) = (&self.default_catalog, &self.default_schema) {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "Default SchemaProvider {} must be used together with a default CatalogProvider",
                schema
            )));
        }
        Ok(())
    }
}

pub trait LanceUrlTableExt {
    fn enable_lance_url_table(self) -> Self;
}

impl LanceUrlTableExt for SessionContext {
    /// Enables querying local files and URIs as DataFusion tables, with extended support for
    /// Lance datasets (`.lance`).
    ///
    /// This method mirrors the behavior of DataFusion's native
    /// [`SessionContext::enable_url_table`], with the following differences:
    ///
    /// - It wraps the existing `catalog_list` in a [`DynamicFileCatalog`].
    /// - It installs a [`LanceUrlTableFactory`] *before* DataFusion's default
    ///   [`DynamicListTableFactory`] in the URL resolution chain.
    /// - As a result, `.lance` paths are resolved by Lance first, while other formats
    ///   such as CSV, Parquet and JSON continue to be handled by `DynamicListTableFactory`.
    /// - It wires the current [`SessionState`] into both factories via [`SessionStore`], so
    ///   they can access session configuration and runtime state when resolving URLs.
    ///
    /// After calling this method, you can use URL literals in SQL, for example:
    ///
    /// ```sql
    /// SELECT * FROM 'tests/data/example.csv';
    /// SELECT * FROM 'tests/data/example.parquet';
    /// SELECT * FROM 'tests/data/example.json';
    /// SELECT * FROM 'tests/data/example.lance';
    /// ```
    ///
    /// # Note
    ///
    /// This method is intended to be the Lance-aware counterpart of
    /// [`SessionContext::enable_url_table`]. If you call `enable_url_table` *after*
    /// calling this method, the URL table configuration set up here (including `.lance`
    /// support and factory ordering) may be overwritten by DataFusion's default wiring.
    /// In that case, `.lance` URLs may no longer be resolved by `LanceUrlTableFactory`
    /// as intended.
    ///
    /// For security-sensitive deployments, treat this method the same way as
    /// `enable_url_table`: only enable it in environments where direct access to
    /// the underlying file system or object store from SQL is acceptable.
    fn enable_lance_url_table(self) -> Self {
        // Enable URL table support by wrapping the current catalog list with a
        // DynamicFileCatalog that uses both DataFusion's DynamicListTableFactory
        // (for Parquet/CSV/JSON/etc) and a LanceUrlTableFactory (for .lance URLs).
        let current_catalog_list = Arc::clone(self.state().catalog_list());

        let default_url_factory = Arc::new(DynamicListTableFactory::new(SessionStore::new()));
        let lance_url_factory = Arc::new(LanceUrlTableFactory::new(SessionStore::new()));
        let url_factory = Arc::new(MultiUrlTableFactory::new(vec![
            lance_url_factory.clone() as Arc<dyn UrlTableFactory>,
            default_url_factory.clone() as Arc<dyn UrlTableFactory>,
        ]));

        let catalog_list = Arc::new(DynamicFileCatalog::new(current_catalog_list, url_factory));

        let session_id = self.session_id();
        let ctx: SessionContext = self
            .into_state_builder()
            .with_session_id(session_id)
            .with_catalog_list(catalog_list)
            .build()
            .into();

        // Register the new SessionState with each SessionStore so that
        // URL table factories can access the runtime session as needed.
        default_url_factory
            .session_store()
            .with_state(ctx.state_weak_ref());
        lance_url_factory
            .session_store()
            .with_state(ctx.state_weak_ref());

        ctx
    }
}

#[cfg(test)]
mod tests {
    use super::SessionBuilder;
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch};
    use datafusion::catalog::memory::{MemoryCatalogProvider, MemorySchemaProvider};
    use datafusion::catalog::SchemaProvider;
    use datafusion::common::record_batch;
    use datafusion::datasource::MemTable;
    use datafusion::error::Result;

    #[tokio::test]
    async fn default_catalog_and_schema_are_used_for_sql_queries() -> Result<()> {
        // Construct a simple in-memory orders table using the same style as tests/sql.rs.
        let batch = record_batch!(
            ("order_id", Int32, vec![101, 102, 103]),
            ("customer_id", Int32, vec![1, 2, 3]),
            ("amount", Int32, vec![100, 200, 300])
        )?;
        let schema = batch.schema();
        let table = Arc::new(MemTable::try_new(schema, vec![vec![batch]])?);

        // Create DataFusion's in-memory schema and catalog providers.
        let sales_schema = Arc::new(MemorySchemaProvider::new());
        let retail_catalog = Arc::new(MemoryCatalogProvider::new());
        sales_schema.register_table("orders".to_string(), table)?;

        // Build a SessionContext that uses the memory catalog/schema as defaults.
        let ctx = SessionBuilder::new()
            .with_default_catalog("retail", Some(retail_catalog))
            .with_default_schema("sales", Some(sales_schema))
            .build()
            .await?;

        let extract_count = |batches: &[RecordBatch]| -> i64 {
            let batch = &batches[0];
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("COUNT should return Int64Array");
            assert_eq!(array.len(), 1);
            array.value(0)
        };

        // Query using explicit schema name.
        let df_with_schema = ctx.sql("SELECT COUNT(*) AS c FROM sales.orders").await?;
        let batches_with_schema = df_with_schema.collect().await?;

        // Query relying on default catalog and schema.
        let df_without_schema = ctx.sql("SELECT COUNT(*) AS c FROM orders").await?;
        let batches_without_schema = df_without_schema.collect().await?;

        let count_with_schema = extract_count(&batches_with_schema);
        let count_without_schema = extract_count(&batches_without_schema);

        assert_eq!(count_with_schema, 3);
        assert_eq!(count_without_schema, 3);
        assert_eq!(count_with_schema, count_without_schema);

        Ok(())
    }
}
