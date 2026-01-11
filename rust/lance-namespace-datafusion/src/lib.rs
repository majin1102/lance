// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Lance integration between namespaces and Apache DataFusion catalogs.
//!
//! This crate provides adapters to expose Lance namespaces as
//! DataFusion `CatalogProviderList`, `CatalogProvider`, and
//! `SchemaProvider` implementations. It intentionally focuses on
//! read-only catalog and schema mapping.

pub mod catalog;
pub mod error;
pub mod namespace;
pub mod schema;
pub mod session_builder;

pub use catalog::{LanceCatalogProvider, LanceCatalogProviderList};
pub use namespace::Namespace;
pub use schema::LanceSchemaProvider;
pub use session_builder::SessionBuilder;
