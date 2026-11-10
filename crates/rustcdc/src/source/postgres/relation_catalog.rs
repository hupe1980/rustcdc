use std::sync::Arc;

use async_trait::async_trait;
use tokio_postgres::Client;

use crate::core::{Error, Result};
use crate::source::schema_catalog::CatalogColumn;

use super::pg_error::PgError;

/// What a `RELATION` message cannot say about a table, asked of the server.
///
/// pgoutput carries each column's type OID and modifier but not the type's name, its
/// nullability or the primary key. A trait so the decode path is testable without a server.
#[async_trait]
pub(super) trait RelationCatalog: Send + Sync {
    /// `format_type(oid, typmod)` for each pair, in order.
    async fn type_names(&self, types: &[(u32, i32)]) -> Result<Vec<String>>;

    /// The table's columns (for nullability) and primary key, as the catalogue holds them now.
    async fn columns_and_key(
        &self,
        schema: &str,
        table: &str,
    ) -> Result<(Vec<CatalogColumn>, Vec<String>)>;
}

/// The catalogue read over the connection's ordinary SQL client.
pub(super) struct ClientRelationCatalog(pub(super) Arc<Client>);

#[async_trait]
impl RelationCatalog for ClientRelationCatalog {
    async fn type_names(&self, types: &[(u32, i32)]) -> Result<Vec<String>> {
        let oids: Vec<u32> = types.iter().map(|(oid, _)| *oid).collect();
        let modifiers: Vec<i32> = types.iter().map(|(_, typmod)| *typmod).collect();
        let rows = self
            .0
            .query(
                "SELECT pg_catalog.format_type(t.oid, t.typmod)
                 FROM unnest($1::oid[], $2::int4[]) WITH ORDINALITY AS t(oid, typmod, ord)
                 ORDER BY t.ord",
                &[&oids, &modifiers],
            )
            .await
            .map_err(PgError::from)
            .map_err(|error| {
                Error::SourceError(format!(
                    "failed resolving declared types of a pgoutput RELATION: {error}"
                ))
            })?;
        Ok(rows.iter().map(|row| row.get::<usize, String>(0)).collect())
    }

    async fn columns_and_key(
        &self,
        schema: &str,
        table: &str,
    ) -> Result<(Vec<CatalogColumn>, Vec<String>)> {
        let columns = super::query::query_table_column_types(&self.0, schema, table).await?;
        let (key, _) =
            super::query::query_primary_key_columns_and_types(&self.0, schema, table).await?;
        Ok((columns, key))
    }
}
