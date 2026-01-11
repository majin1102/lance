// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Builder for constructing DataFusion `SessionContext` instances
//! backed by Lance namespaces.
//!
//! This API is intentionally minimal and returns the native
//! `SessionContext` without adding an extra wrapper. It wires up
//! dynamic catalog and schema providers so that Lance namespaces can
//! be accessed via `catalog.schema.table` names.

use std::sync::Arc;

use datafusion::error::Result;
use datafusion::execution::context::{SessionConfig, SessionContext};

use crate::catalog::LanceCatalogProviderList;
use crate::namespace::Namespace;
use crate::LanceCatalogProvider;

/// Builder for configuring a `SessionContext` with Lance namespaces.
#[derive(Clone, Debug, Default)]
pub struct SessionBuilder {
    /// Optional root namespace exposed via a dynamic
    /// `LanceCatalogProviderList`.
    root: Option<Namespace>,
    /// Explicit catalogs to register by name.
    catalogs: Vec<(String, Namespace)>,
    /// Optional DataFusion session configuration.
    config: Option<SessionConfig>,
}

impl SessionBuilder {
    /// Create a new builder with no namespaces or configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach a root `LanceNamespace` that is exposed as a dynamic
    /// catalog list via `LanceCatalogProviderList`.
    pub fn with_root(mut self, ns: Namespace) -> Self {
        self.root = Some(ns);
        self
    }

    /// Register an additional catalog backed by the given namespace.
    ///
    /// The catalog is identified by `name` and can later be combined
    /// with schemas via `SessionBuilder::add_schema` using the same
    /// namespace.
    pub fn add_catalog(mut self, name: &str, ns: Namespace) -> Self {
        self.catalogs.push((name.to_string(), ns));
        self
    }

    /// Provide an explicit `SessionConfig` for the underlying
    /// `SessionContext`.
    pub fn with_config(mut self, config: SessionConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Build a `SessionContext` with all configured namespaces.
    pub async fn build(self) -> Result<SessionContext> {
        let ctx = if let Some(config) = self.config {
            SessionContext::new_with_config(config)
        } else {
            SessionContext::new()
        };

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

        Ok(ctx)
    }
}
