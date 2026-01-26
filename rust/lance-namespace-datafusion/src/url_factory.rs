// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! URL table factories for integrating Lance with DataFusion's DynamicFileCatalog.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::UrlTableFactory;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DataFusionResult;
use datafusion_session::SessionStore;
use lance::datafusion::LanceTableProvider;
use lance::dataset::Dataset;

use crate::error::to_datafusion_error;

/// UrlTableFactory that dispatches to multiple inner factories in order.
#[derive(Debug)]
pub struct MultiUrlTableFactory {
    factories: Vec<Arc<dyn UrlTableFactory>>,
}

impl MultiUrlTableFactory {
    /// Create a new MultiUrlTableFactory from a list of factories.
    ///
    /// Factories are tried in order; the first one that returns a
    /// non-None table provider wins.
    pub fn new(factories: Vec<Arc<dyn UrlTableFactory>>) -> Self {
        Self { factories }
    }
}

#[async_trait]
impl UrlTableFactory for MultiUrlTableFactory {
    async fn try_new(&self, url: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        for factory in &self.factories {
            if let Some(provider) = factory.try_new(url).await? {
                return Ok(Some(provider));
            }
        }

        Ok(None)
    }
}

/// UrlTableFactory implementation that opens Lance datasets for `.lance` URLs.
///
/// This factory only handles URLs that end with the `.lance` extension. All
/// other URLs are ignored and left for other factories (such as
/// DynamicListTableFactory) to handle.
#[derive(Debug)]
pub struct LanceUrlTableFactory {
    /// SessionStore shared with other UrlTableFactory implementations.
    ///
    /// This mirrors DataFusion's DynamicListTableFactory wiring so that
    /// all factories participate in the same session lifecycle. The
    /// Lance implementation does not currently use the session state
    /// directly but keeps the store for future extensions.
    session_store: SessionStore,
}

impl LanceUrlTableFactory {
    /// Create a new LanceUrlTableFactory bound to the given SessionStore.
    pub fn new(session_store: SessionStore) -> Self {
        Self { session_store }
    }

    /// Access the underlying SessionStore.
    pub fn session_store(&self) -> &SessionStore {
        &self.session_store
    }
}

#[async_trait]
impl UrlTableFactory for LanceUrlTableFactory {
    async fn try_new(&self, url: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        // Only handle Lance URLs; let other factories handle the rest.
        if !url.ends_with(".lance") {
            return Ok(None);
        }

        let dataset = Dataset::open(url).await.map_err(to_datafusion_error)?;
        let provider = LanceTableProvider::new(Arc::new(dataset), false, false);

        Ok(Some(Arc::new(provider)))
    }
}
