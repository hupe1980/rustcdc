//! Which [`RowShape`] a MySQL/MariaDB stream row was captured under, if any.
//!
//! The declared types come from `information_schema`, because the binlog table map carries
//! a protocol type code (`MYSQL_TYPE_NEWDECIMAL`, not `decimal(12,4)`). But the catalogue is
//! read *now*, and a row may be from before an `ALTER` — after a resume, or while the stream
//! catches up. So the catalogue's shape is stamped on a row only when it agrees with the
//! row's own table map on everything the table map carries: column count, names, each
//! column's binlog type family, nullability and primary key (`binlog_row_metadata = FULL`
//! supplies names and key). While they disagree, the row carries no shape.
//!
//! *Stated limit:* a change within one type family — `varchar(64)` to `varchar(128)`, `int`
//! to `bigint` — is invisible to the table-map comparison, so a row captured before such a
//! change and read after it carries the newer declaration.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use mysql_common::{binlog::events::TableMapEvent, constants::ColumnType};

use crate::RowShape;
use crate::core::Result;

/// Coarse type classes the binlog table map and a declared type can both be mapped onto.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum TypeFamily {
    Integer,
    Decimal,
    Float,
    Double,
    Year,
    Date,
    Time,
    DateTime,
    Timestamp,
    /// `char`, `varchar`, `binary`, `varbinary`.
    String,
    /// Every `text` and `blob` variant, and MariaDB's `json` (a `longtext` alias).
    Blob,
    Enum,
    Set,
    Bit,
    Json,
    Geometry,
    Vector,
    /// Anything not classified above. Never agrees with anything, itself included.
    Other,
}

impl TypeFamily {
    fn of_binlog(column_type: ColumnType) -> Self {
        use ColumnType::*;
        match column_type {
            MYSQL_TYPE_TINY | MYSQL_TYPE_SHORT | MYSQL_TYPE_INT24 | MYSQL_TYPE_LONG
            | MYSQL_TYPE_LONGLONG => Self::Integer,
            MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => Self::Decimal,
            MYSQL_TYPE_FLOAT => Self::Float,
            MYSQL_TYPE_DOUBLE => Self::Double,
            MYSQL_TYPE_YEAR => Self::Year,
            MYSQL_TYPE_DATE | MYSQL_TYPE_NEWDATE => Self::Date,
            MYSQL_TYPE_TIME | MYSQL_TYPE_TIME2 => Self::Time,
            MYSQL_TYPE_DATETIME | MYSQL_TYPE_DATETIME2 => Self::DateTime,
            MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2 => Self::Timestamp,
            MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING | MYSQL_TYPE_STRING => Self::String,
            MYSQL_TYPE_TINY_BLOB
            | MYSQL_TYPE_MEDIUM_BLOB
            | MYSQL_TYPE_LONG_BLOB
            | MYSQL_TYPE_BLOB => Self::Blob,
            MYSQL_TYPE_ENUM => Self::Enum,
            MYSQL_TYPE_SET => Self::Set,
            MYSQL_TYPE_BIT => Self::Bit,
            MYSQL_TYPE_JSON => Self::Json,
            MYSQL_TYPE_GEOMETRY => Self::Geometry,
            MYSQL_TYPE_VECTOR => Self::Vector,
            _ => Self::Other,
        }
    }

    /// The family of an `information_schema.COLUMNS.COLUMN_TYPE` value.
    fn of_declared(column_type: &str) -> Self {
        let base = column_type
            .trim()
            .split(|c: char| c == '(' || c.is_whitespace())
            .next()
            .unwrap_or_default()
            .to_ascii_lowercase();
        match base.as_str() {
            "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" | "bool"
            | "boolean" => Self::Integer,
            "decimal" | "numeric" | "dec" | "fixed" => Self::Decimal,
            "float" => Self::Float,
            "double" | "real" => Self::Double,
            "year" => Self::Year,
            "date" => Self::Date,
            "time" => Self::Time,
            "datetime" => Self::DateTime,
            "timestamp" => Self::Timestamp,
            "char" | "varchar" | "binary" | "varbinary" => Self::String,
            "tinytext" | "text" | "mediumtext" | "longtext" | "tinyblob" | "blob"
            | "mediumblob" | "longblob" => Self::Blob,
            "enum" => Self::Enum,
            "set" => Self::Set,
            "bit" => Self::Bit,
            "json" => Self::Json,
            "geometry" | "point" | "linestring" | "polygon" | "multipoint" | "multilinestring"
            | "multipolygon" | "geometrycollection" | "geomcollection" => Self::Geometry,
            "vector" => Self::Vector,
            _ => Self::Other,
        }
    }
}

/// One column as a row's binlog table map describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TableMapColumn {
    /// `None` when the table map carries no names (`binlog_row_metadata` below `FULL`).
    pub(super) name: Option<String>,
    pub(super) family: TypeFamily,
    pub(super) nullable: bool,
}

/// What a row's binlog table map says about its table, shared by the rows of one rows event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TableMapColumns {
    pub(super) columns: Vec<TableMapColumn>,
    /// Key column names in key order; empty when the table has none. Meaningful only when
    /// the names are present, which is what says the metadata is `FULL`.
    pub(super) primary_key: Vec<String>,
}

impl TableMapColumns {
    pub(super) fn from_table_map(table_map: &TableMapEvent<'_>) -> Self {
        use mysql_common::binlog::events::OptionalMetadataField;

        let count = usize::try_from(table_map.columns_count()).unwrap_or(usize::MAX);
        let mut names: Vec<String> = Vec::new();
        let mut key_indexes: Vec<u64> = Vec::new();
        for field in table_map.iter_optional_meta() {
            let Ok(field) = field else { continue };
            match field {
                OptionalMetadataField::ColumnName(column_names) => {
                    names = column_names
                        .iter_names()
                        .filter_map(|name| name.ok().map(|name| name.name().into_owned()))
                        .collect();
                }
                OptionalMetadataField::SimplePrimaryKey(keys) => {
                    key_indexes.extend(keys.iter_indexes().filter_map(|index| index.ok()));
                }
                OptionalMetadataField::PrimaryKeyWithPrefix(keys) => {
                    key_indexes.extend(
                        keys.iter_keys()
                            .filter_map(|key| key.ok().map(|key| key.column_index())),
                    );
                }
                _ => {}
            }
        }
        // A partial name list is no better than none: positions would no longer line up.
        let names_complete = names.len() == count;
        let nulls = table_map.null_bitmask();
        let columns = (0..count)
            .map(|index| TableMapColumn {
                name: names_complete.then(|| names[index].clone()),
                family: match table_map.get_column_type(index) {
                    Ok(Some(column_type)) => TypeFamily::of_binlog(column_type),
                    _ => TypeFamily::Other,
                },
                nullable: nulls.get(index).is_some_and(|bit| *bit),
            })
            .collect();
        let primary_key = if names_complete {
            key_indexes
                .iter()
                .filter_map(|index| names.get(usize::try_from(*index).ok()?).cloned())
                .collect()
        } else {
            Vec::new()
        };
        Self {
            columns,
            primary_key,
        }
    }
}

/// Whether `shape` describes the table `table_map` was written under, as far as the table
/// map can tell.
pub(super) fn agrees(shape: &RowShape, table_map: &TableMapColumns) -> bool {
    let (Some(columns), Some(primary_key)) = (shape.columns(), shape.primary_key()) else {
        return false;
    };
    columns.len() == table_map.columns.len()
        && primary_key == table_map.primary_key.as_slice()
        && columns
            .iter()
            .zip(&table_map.columns)
            .all(|(declared, mapped)| {
                let family = TypeFamily::of_declared(&declared.data_type);
                mapped.name.as_deref() == Some(declared.name.as_str())
                    && family != TypeFamily::Other
                    && family == mapped.family
                    && declared.nullable == mapped.nullable
            })
}

/// Reads one table's shape from `information_schema`.
#[async_trait]
pub(super) trait MysqlCatalogReader: Send + Sync {
    /// `Ok(None)` when the catalogue has no such table.
    async fn read_table(&self, database: &str, table: &str) -> Result<Option<Arc<RowShape>>>;
}

/// The live reader, over the connector's own connections — pooled, or one connection per
/// read under `AwsIamToken`, whose credentials expire.
pub(super) struct LiveMysqlCatalogReader {
    pub(super) connections: super::connections::MysqlConnections,
}

#[async_trait]
impl MysqlCatalogReader for LiveMysqlCatalogReader {
    async fn read_table(&self, database: &str, table: &str) -> Result<Option<Arc<RowShape>>> {
        let mut connection = self.connections.get_conn().await?;
        let (columns, keys) =
            super::query::query_table_catalog(&mut connection, database, table).await?;
        Ok(crate::source::schema_catalog::row_shape_from_catalog(
            database, table, &columns, &keys,
        ))
    }
}

#[derive(Debug, Default)]
struct TableShapeState {
    /// The catalogue's shape for the table, `None` when it has none or it is not yet read.
    catalogue: Option<Arc<RowShape>>,
    /// A read is owed: the table has never been read, or a DDL statement named it.
    due: bool,
    /// The table map last compared against `catalogue`, and whether it agreed.
    last: Option<(Arc<TableMapColumns>, bool)>,
}

/// Per-table catalogue shapes for a stream, re-read when they may have gone stale.
pub(super) struct StreamShapes {
    tables: HashMap<(String, String), TableShapeState>,
    reader: Option<Box<dyn MysqlCatalogReader>>,
    /// Tables already read in the current batch, so a failing or disagreeing read costs one
    /// query per table per batch rather than one per row.
    read_this_batch: HashSet<(String, String)>,
}

impl StreamShapes {
    /// Seed from the catalogue read at stream start; the start read is the re-read a resume
    /// owes, so these entries are not due.
    pub(super) fn new(
        catalog_columns: &crate::source::schema_catalog::CatalogSchemas,
        catalog_primary_keys: &HashMap<(String, String), Vec<String>>,
    ) -> Self {
        let tables = catalog_columns
            .iter()
            .map(|(key, columns)| {
                let keys = catalog_primary_keys.get(key).cloned().unwrap_or_default();
                (
                    key.clone(),
                    TableShapeState {
                        catalogue: crate::source::schema_catalog::row_shape_from_catalog(
                            &key.0, &key.1, columns, &keys,
                        ),
                        due: false,
                        last: None,
                    },
                )
            })
            .collect();
        Self {
            tables,
            reader: None,
            read_this_batch: HashSet::new(),
        }
    }

    pub(super) fn set_reader(&mut self, reader: Box<dyn MysqlCatalogReader>) {
        self.reader = Some(reader);
    }

    pub(super) fn start_batch(&mut self) {
        self.read_this_batch.clear();
    }

    /// A DDL statement named `table`: whatever is held for it may be stale.
    ///
    /// Matched on the table name in every schema, case-insensitively: an unqualified
    /// statement parses under a placeholder schema, not the session's database.
    pub(super) fn invalidate(&mut self, schema: &str, table: &str) {
        for ((held_schema, held_table), state) in &mut self.tables {
            if held_table.eq_ignore_ascii_case(table)
                || (held_schema == schema && held_table == table)
            {
                state.catalogue = None;
                state.due = true;
                state.last = None;
            }
        }
        // A read earlier in this batch predates the statement; the rows after it need a new one.
        self.read_this_batch.retain(|(held_schema, held_table)| {
            !(held_table.eq_ignore_ascii_case(table)
                || (held_schema == schema && held_table == table))
        });
    }

    /// Read the table's catalogue shape if this batch has not; `false` when no read happened
    /// or it failed.
    async fn read(&mut self, key: &(String, String)) -> bool {
        let Some(reader) = self.reader.as_ref() else {
            return false;
        };
        if !self.read_this_batch.insert(key.clone()) {
            return false;
        }
        match reader.read_table(&key.0, &key.1).await {
            Ok(shape) => {
                let state = self.tables.entry(key.clone()).or_default();
                state.catalogue = shape;
                state.due = false;
                state.last = None;
                true
            }
            Err(error) => {
                // Not fatal: the row is already taken from the binlog, so failing the poll
                // would lose it. The row carries no shape, and the read is retried next batch.
                tracing::warn!(
                    target: "rustcdc::source::mysql",
                    database = %key.0,
                    table = %key.1,
                    error = %error,
                    "could not re-read a table's catalogue; its rows carry no shape until a \
                     read succeeds",
                );
                false
            }
        }
    }

    /// The shape to stamp on a row written under `table_map`, if the catalogue agrees.
    pub(super) async fn shape_for(
        &mut self,
        schema: &str,
        table: &str,
        table_map: &Arc<TableMapColumns>,
    ) -> Option<Arc<RowShape>> {
        let key = (schema.to_string(), table.to_string());
        let state = self
            .tables
            .entry(key.clone())
            .or_insert_with(|| TableShapeState {
                due: true,
                ..TableShapeState::default()
            });
        if !state.due
            && let Some((last, agreed)) = &state.last
            && (Arc::ptr_eq(last, table_map) || **last == **table_map)
        {
            return agreed.then(|| state.catalogue.clone()).flatten();
        }

        let mut fresh = false;
        if state.due {
            fresh = self.read(&key).await;
            if !fresh {
                return None;
            }
        }
        let state = self.tables.get(&key)?;
        let mut agreed = state
            .catalogue
            .as_deref()
            .is_some_and(|shape| agrees(shape, table_map));
        // A table map the held shape disagrees with may follow a DDL statement this connector
        // does not parse (a `RENAME`, an unrecognised `ALTER` form). One read per distinct
        // table map settles it.
        if !agreed && !fresh && self.read(&key).await {
            agreed = self
                .tables
                .get(&key)?
                .catalogue
                .as_deref()
                .is_some_and(|shape| agrees(shape, table_map));
        }
        let state = self.tables.get_mut(&key)?;
        state.last = Some((Arc::clone(table_map), agreed));
        agreed.then(|| state.catalogue.clone()).flatten()
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::ShapeColumn;
    use std::sync::Mutex;

    pub(in crate::source::mysql) fn table_map(
        columns: &[(&str, TypeFamily, bool)],
        primary_key: &[&str],
    ) -> Arc<TableMapColumns> {
        Arc::new(TableMapColumns {
            columns: columns
                .iter()
                .map(|(name, family, nullable)| TableMapColumn {
                    name: Some((*name).to_string()),
                    family: *family,
                    nullable: *nullable,
                })
                .collect(),
            primary_key: primary_key.iter().map(|key| (*key).to_string()).collect(),
        })
    }

    fn users() -> RowShape {
        RowShape::new(
            "app",
            "users",
            vec![
                ShapeColumn::new("id", "bigint unsigned", false),
                ShapeColumn::new("email", "varchar(320)", true),
            ],
            vec!["id".into()],
        )
        .unwrap()
    }

    fn users_map() -> Arc<TableMapColumns> {
        table_map(
            &[
                ("id", TypeFamily::Integer, false),
                ("email", TypeFamily::String, true),
            ],
            &["id"],
        )
    }

    #[test]
    fn a_shape_that_agrees_with_the_table_map_is_stamped() {
        assert!(agrees(&users(), &users_map()));
    }

    /// Each thing the table map carries is compared; a disagreement on any one of them means
    /// the row was written under a different shape than the catalogue now declares.
    #[test]
    fn a_shape_that_disagrees_with_the_table_map_is_not_stamped() {
        let cases = [
            (
                "renamed column",
                table_map(
                    &[
                        ("id", TypeFamily::Integer, false),
                        ("mail", TypeFamily::String, true),
                    ],
                    &["id"],
                ),
            ),
            (
                "nullability",
                table_map(
                    &[
                        ("id", TypeFamily::Integer, false),
                        ("email", TypeFamily::String, false),
                    ],
                    &["id"],
                ),
            ),
            (
                "primary key",
                table_map(
                    &[
                        ("id", TypeFamily::Integer, false),
                        ("email", TypeFamily::String, true),
                    ],
                    &["email"],
                ),
            ),
            (
                "type family",
                table_map(
                    &[
                        ("id", TypeFamily::Integer, false),
                        ("email", TypeFamily::Blob, true),
                    ],
                    &["id"],
                ),
            ),
            (
                "column count",
                table_map(&[("id", TypeFamily::Integer, false)], &["id"]),
            ),
            (
                "no names in the table map",
                Arc::new(TableMapColumns {
                    columns: vec![
                        TableMapColumn {
                            name: None,
                            family: TypeFamily::Integer,
                            nullable: false,
                        },
                        TableMapColumn {
                            name: None,
                            family: TypeFamily::String,
                            nullable: true,
                        },
                    ],
                    primary_key: Vec::new(),
                }),
            ),
        ];
        for (case, map) in cases {
            assert!(!agrees(&users(), &map), "{case}");
        }
    }

    #[test]
    fn declared_types_map_onto_the_binlog_families() {
        for (declared, family) in [
            ("tinyint(1)", TypeFamily::Integer),
            ("int unsigned", TypeFamily::Integer),
            ("decimal(12,4)", TypeFamily::Decimal),
            ("double", TypeFamily::Double),
            ("varchar(64)", TypeFamily::String),
            ("char(2)", TypeFamily::String),
            ("longtext", TypeFamily::Blob),
            ("enum('a','b')", TypeFamily::Enum),
            ("set('a','b')", TypeFamily::Set),
            ("datetime(6)", TypeFamily::DateTime),
            ("timestamp", TypeFamily::Timestamp),
            ("json", TypeFamily::Json),
            ("bit(8)", TypeFamily::Bit),
            ("inet6", TypeFamily::Other),
        ] {
            assert_eq!(TypeFamily::of_declared(declared), family, "{declared}");
        }
    }

    /// A catalogue reader whose answer the test controls, counting reads.
    pub(in crate::source::mysql) struct FakeCatalogue {
        pub(in crate::source::mysql) shapes: Arc<Mutex<HashMap<(String, String), RowShape>>>,
        pub(in crate::source::mysql) reads: Arc<Mutex<u32>>,
    }

    #[async_trait]
    impl MysqlCatalogReader for FakeCatalogue {
        async fn read_table(&self, database: &str, table: &str) -> Result<Option<Arc<RowShape>>> {
            *self.reads.lock().unwrap() += 1;
            Ok(self
                .shapes
                .lock()
                .unwrap()
                .get(&(database.to_string(), table.to_string()))
                .cloned()
                .map(Arc::new))
        }
    }

    #[tokio::test]
    async fn one_read_per_distinct_disagreeing_table_map() {
        let reads = Arc::new(Mutex::new(0));
        let mut shapes = StreamShapes::new(&HashMap::new(), &HashMap::new());
        shapes.set_reader(Box::new(FakeCatalogue {
            shapes: Arc::new(Mutex::new(HashMap::from([(
                ("app".to_string(), "users".to_string()),
                users(),
            )]))),
            reads: Arc::clone(&reads),
        }));

        shapes.start_batch();
        let old = table_map(&[("id", TypeFamily::Integer, false)], &["id"]);
        assert!(shapes.shape_for("app", "users", &old).await.is_none());
        assert!(shapes.shape_for("app", "users", &old).await.is_none());
        assert_eq!(*reads.lock().unwrap(), 1, "first sight reads once");

        shapes.start_batch();
        let old_again = table_map(&[("id", TypeFamily::Integer, false)], &["id"]);
        assert!(shapes.shape_for("app", "users", &old_again).await.is_none());
        assert_eq!(
            *reads.lock().unwrap(),
            1,
            "an equal table map is not re-read"
        );

        assert!(
            shapes
                .shape_for("app", "users", &users_map())
                .await
                .is_some()
        );
        assert_eq!(*reads.lock().unwrap(), 1, "the held shape agrees: no read");
    }
}
