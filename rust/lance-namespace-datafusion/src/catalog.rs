// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! DataFusion catalog providers backed by Lance namespaces.

use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use dashmap::DashMap;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider};
use datafusion::error::Result;

use crate::namespace::Namespace;
use crate::schema::LanceSchemaProvider;

/// A dynamic [`CatalogProviderList`] that maps Lance namespaces to catalogs.
///
/// Top-level namespaces (e.g. `ListNamespacesRequest { id: Some(vec![]) }`)
/// are exposed as catalog names. Child namespaces under a given catalog are
/// exposed as schemas via [`LanceCatalogProvider`].
#[derive(Debug, Clone)]
pub struct LanceCatalogProviderList {
    /// Root Lance namespace used to resolve catalogs / schemas / tables.
    #[allow(dead_code)]
    namespace: Namespace,
    /// Catalogs that have been loaded from the root namespace.
    catalogs: DashMap<String, Arc<dyn CatalogProvider>>,
}

impl LanceCatalogProviderList {
    pub async fn try_new(namespace: Namespace) -> Result<Self> {
        let catalogs = DashMap::new();
        for child_namespace in namespace.children().await? {
            let catalog_name = child_namespace.name().to_string();
            let catalog_provider = Arc::new(LanceCatalogProvider::try_new(child_namespace).await?);
            catalogs.insert(catalog_name, catalog_provider as Arc<dyn CatalogProvider>);
        }

        Ok(Self {
            namespace,
            catalogs,
        })
    }
}

impl CatalogProviderList for LanceCatalogProviderList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Adds a new catalog to this catalog list.
    /// If a catalog of the same name existed before, it is replaced in the list and returned.
    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs.insert(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        self.catalogs
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs
            .get(name)
            .map(|entry| Arc::clone(entry.value()))
    }
}

/// Dynamic [`CatalogProvider`] that views child namespaces under a single
/// top-level namespace as schemas.
#[derive(Debug, Clone)]
pub struct LanceCatalogProvider {
    #[allow(dead_code)]
    namespace: Namespace,
    schemas: DashMap<String, Arc<dyn SchemaProvider>>,
}

impl LanceCatalogProvider {
    pub async fn try_new(namespace: Namespace) -> Result<Self> {
        let schemas = DashMap::new();
        for child_namespace in namespace.children().await? {
            let schema_name = child_namespace.name().to_string();
            let schema_provider = Arc::new(LanceSchemaProvider::try_new(child_namespace).await?);
            schemas.insert(schema_name, schema_provider as Arc<dyn SchemaProvider>);
        }

        Ok(Self { namespace, schemas })
    }
}

impl CatalogProvider for LanceCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect()
    }

    fn schema(&self, schema_name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas
            .get(schema_name)
            .map(|entry| Arc::clone(entry.value()))
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        Ok(self.schemas.insert(name.to_string(), schema))
    }
}
