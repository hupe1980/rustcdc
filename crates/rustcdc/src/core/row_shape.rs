//! The table shape a row was captured under, carried inline by the row.
//!
//! Column values are text on every connector, so a consumer needs each column's declared type
//! to parse them. A shape travelling in a separate event can reach a consumer after the rows
//! it describes — Kafka orders nothing between two topics, or two keys on one — so the shape
//! travels in the row instead, the way Debezium's JSON converter ships `schema` beside
//! `payload` in every record.

use std::collections::HashSet;

use serde::{Deserialize, Serialize, Serializer, ser::SerializeStruct};

use crate::schema_history::TableSchema;

/// Version tag hashed ahead of a shape's content; see [`RowShape::id`].
pub const ROW_SHAPE_DIGEST_VERSION: &str = "rustcdc/v2/row-shape";

/// One column of a [`RowShape`], in the table's ordinal order.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ShapeColumn {
    /// Column name.
    pub name: String,
    /// The declared type in the source's own syntax, modifier included: `numeric(12,4)`,
    /// `decimal(12,4) unsigned`, `nvarchar(255)`.
    pub data_type: String,
    /// Whether the column accepts `NULL`, as the source's catalogue declares it.
    pub nullable: bool,
}

impl ShapeColumn {
    /// A column with its declared type and nullability.
    pub fn new(name: impl Into<String>, data_type: impl Into<String>, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type: data_type.into(),
            nullable,
        }
    }
}

/// The shape of a table a row was captured under: its columns and primary key, named by a
/// content id.
///
/// Fields are private because [`id`](Self::id) is a digest of the others: a shape whose
/// columns could be edited in place would carry an id that no longer names it.
///
/// A shape is either **complete** (columns and primary key present) or **compact** (only the
/// id, schema and table — the server's `row_shape = "id"` delivery mode, resolved by id).
#[derive(Debug, Clone)]
pub struct RowShape {
    id: String,
    schema: String,
    table: String,
    definition: Option<Definition>,
    /// A transform dropped columns from the shape the connector captured. Not on the wire: it
    /// tells the delivering process that no source ever announced this shape.
    narrowed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Definition {
    columns: Vec<ShapeColumn>,
    primary_key: Vec<String>,
}

impl PartialEq for RowShape {
    fn eq(&self, other: &Self) -> bool {
        // `narrowed` is provenance in this process, not part of the shape.
        self.id == other.id
            && self.schema == other.schema
            && self.table == other.table
            && self.definition == other.definition
    }
}

impl Eq for RowShape {}

impl RowShape {
    /// A complete shape, its id computed from exactly these fields.
    ///
    /// `None` for a table with no columns: compact mode is told apart from a complete shape by
    /// the absence of columns, and a Protobuf `repeated` field cannot carry "absent" and
    /// "empty" separately.
    #[must_use]
    pub fn new(
        schema: impl Into<String>,
        table: impl Into<String>,
        columns: Vec<ShapeColumn>,
        primary_key: Vec<String>,
    ) -> Option<Self> {
        if columns.is_empty() {
            return None;
        }
        let schema = schema.into();
        let table = table.into();
        let id = digest(&schema, &table, &columns, &primary_key);
        Some(Self {
            id,
            schema,
            table,
            definition: Some(Definition {
                columns,
                primary_key,
            }),
            narrowed: false,
        })
    }

    /// The shape a [`TableSchema`] describes, under the given schema and table names.
    ///
    /// Column constraints and the history version are not part of a shape: a consumer decoding
    /// a row needs the name, declared type and nullability of each column, and the key.
    #[must_use]
    pub fn from_table_schema(
        schema: &str,
        table: &str,
        table_schema: &TableSchema,
    ) -> Option<Self> {
        Self::new(
            schema,
            table,
            table_schema
                .columns
                .iter()
                .map(|column| ShapeColumn::new(&column.name, &column.data_type, column.nullable))
                .collect(),
            table_schema.primary_keys.clone(),
        )
    }

    /// The content id: lowercase hex SHA-256 over the version tag and the shape's fields.
    ///
    /// Each string `s` below is written as its UTF-8 byte length (4 bytes, big-endian) followed
    /// by its bytes, and each count as 4 bytes big-endian:
    ///
    /// ```text
    /// "rustcdc/v2/row-shape"            (the bytes, no length prefix)
    /// schema, table
    /// column count, then per column: name, data_type, one byte 1 (nullable) or 0
    /// primary-key count, then each key column name
    /// ```
    ///
    /// Derived from the shape alone, so a table altered from A to B and back to A carries A's
    /// id again, and a consumer can recompute it from a complete shape.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The schema (PostgreSQL schema, MySQL database, SQL Server schema) the shape belongs to.
    #[must_use]
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// The table the shape describes, as the source names it — not renamed by a route.
    #[must_use]
    pub fn table(&self) -> &str {
        &self.table
    }

    /// The columns in ordinal order; `None` for a compact shape.
    #[must_use]
    pub fn columns(&self) -> Option<&[ShapeColumn]> {
        self.definition
            .as_ref()
            .map(|definition| definition.columns.as_slice())
    }

    /// The primary-key column names in key order; `None` for a compact shape, empty for a
    /// table without one.
    #[must_use]
    pub fn primary_key(&self) -> Option<&[String]> {
        self.definition
            .as_ref()
            .map(|definition| definition.primary_key.as_slice())
    }

    /// Whether this shape carries only its id, to be resolved elsewhere.
    #[must_use]
    pub fn is_compact(&self) -> bool {
        self.definition.is_none()
    }

    /// Whether a transform narrowed this shape from the one the connector captured.
    #[must_use]
    pub fn is_narrowed(&self) -> bool {
        self.narrowed
    }

    /// The id recomputed from the columns and key, or `None` for a compact shape.
    ///
    /// Equal to [`id`](Self::id) for every shape this crate builds; a decoded shape that
    /// disagrees was altered after it was built.
    #[must_use]
    pub fn computed_id(&self) -> Option<String> {
        self.definition.as_ref().map(|definition| {
            digest(
                &self.schema,
                &self.table,
                &definition.columns,
                &definition.primary_key,
            )
        })
    }

    /// The same shape without its columns and key.
    #[must_use]
    pub fn compact(&self) -> Self {
        Self {
            id: self.id.clone(),
            schema: self.schema.clone(),
            table: self.table.clone(),
            definition: None,
            narrowed: false,
        }
    }

    /// The shape without `dropped`, with a new id; `None` when nothing honest remains.
    ///
    /// A dropped key column removes the shape rather than narrowing the key: a key without
    /// one of its columns claims a uniqueness the table does not have. A compact shape cannot
    /// be narrowed — its columns are not here to drop from.
    pub(crate) fn without_columns(&self, dropped: &HashSet<String>) -> Option<Self> {
        let definition = self.definition.as_ref()?;
        if definition
            .primary_key
            .iter()
            .any(|key| dropped.contains(key))
        {
            return None;
        }
        let columns = definition
            .columns
            .iter()
            .filter(|column| !dropped.contains(&column.name))
            .cloned()
            .collect();
        let mut narrowed = Self::new(
            self.schema.clone(),
            self.table.clone(),
            columns,
            definition.primary_key.clone(),
        )?;
        narrowed.narrowed = true;
        Some(narrowed)
    }

    /// Whether `id` has the form of a shape id: 64 lowercase hexadecimal characters.
    #[must_use]
    pub fn is_valid_id(id: &str) -> bool {
        id.len() == 64
            && id
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    }
}

fn digest(schema: &str, table: &str, columns: &[ShapeColumn], primary_key: &[String]) -> String {
    use sha2::{Digest, Sha256};

    fn put_len(digest: &mut Sha256, len: usize) {
        // A shape with more than 4 GiB of names is not one a source can produce.
        digest.update(u32::try_from(len).unwrap_or(u32::MAX).to_be_bytes());
    }
    fn put_str(digest: &mut Sha256, value: &str) {
        put_len(digest, value.len());
        digest.update(value.as_bytes());
    }

    let mut hasher = Sha256::new();
    hasher.update(ROW_SHAPE_DIGEST_VERSION.as_bytes());
    put_str(&mut hasher, schema);
    put_str(&mut hasher, table);
    put_len(&mut hasher, columns.len());
    for column in columns {
        put_str(&mut hasher, &column.name);
        put_str(&mut hasher, &column.data_type);
        hasher.update([u8::from(column.nullable)]);
    }
    put_len(&mut hasher, primary_key.len());
    for key in primary_key {
        put_str(&mut hasher, key);
    }
    // RustCrypto 0.11 returns a `hybrid-array::Array`, which does not implement `LowerHex`.
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Wire form of [`RowShape`] — the field list the schema-contract gate compares against
/// `event.proto`, `event.avsc` and `EVENT_JSON_SCHEMA`.
#[derive(Deserialize)]
struct RowShapeWire {
    id: String,
    schema: String,
    table: String,
    #[serde(default)]
    columns: Option<Vec<ShapeColumn>>,
    #[serde(default)]
    primary_key: Option<Vec<String>>,
}

impl RowShapeWire {
    /// Rebuild a shape from decoded parts.
    ///
    /// The columns and key travel together: one without the other is neither a complete
    /// shape nor a compact one, and guessing which was meant would hide a broken producer.
    fn into_shape(self) -> Result<RowShape, String> {
        if !RowShape::is_valid_id(&self.id) {
            return Err(format!(
                "row shape id '{}' is not 64 lowercase hexadecimal characters",
                self.id
            ));
        }
        let definition = match (self.columns, self.primary_key) {
            (Some(columns), Some(primary_key)) if !columns.is_empty() => Some(Definition {
                columns,
                primary_key,
            }),
            (Some(_), Some(_)) => {
                return Err("a complete row shape lists at least one column".into());
            }
            (None, None) => None,
            _ => {
                return Err(
                    "a row shape carries both `columns` and `primary_key`, or neither".into(),
                );
            }
        };
        Ok(RowShape {
            id: self.id,
            schema: self.schema,
            table: self.table,
            definition,
            narrowed: false,
        })
    }
}

impl<'de> Deserialize<'de> for RowShape {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        RowShapeWire::deserialize(deserializer)?
            .into_shape()
            .map_err(serde::de::Error::custom)
    }
}

impl Serialize for RowShape {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let len = if self.definition.is_some() { 5 } else { 3 };
        let mut state = serializer.serialize_struct("RowShape", len)?;
        state.serialize_field("id", &self.id)?;
        state.serialize_field("schema", &self.schema)?;
        state.serialize_field("table", &self.table)?;
        match &self.definition {
            Some(definition) => {
                state.serialize_field("columns", &definition.columns)?;
                state.serialize_field("primary_key", &definition.primary_key)?;
            }
            None => {
                state.skip_field("columns")?;
                state.skip_field("primary_key")?;
            }
        }
        state.end()
    }
}

/// Parts for the codecs that rebuild a shape field by field (Avro, Protobuf).
pub(crate) fn row_shape_from_parts(
    id: String,
    schema: String,
    table: String,
    columns: Option<Vec<ShapeColumn>>,
    primary_key: Option<Vec<String>>,
) -> Result<RowShape, String> {
    RowShapeWire {
        id,
        schema,
        table,
        columns,
        primary_key,
    }
    .into_shape()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn orders() -> RowShape {
        RowShape::new(
            "app",
            "orders",
            vec![
                ShapeColumn::new("id", "bigint", false),
                ShapeColumn::new("amount", "decimal(12,4)", true),
            ],
            vec!["id".into()],
        )
        .expect("a shape with columns")
    }

    /// The id is what the published recipe says it is, computed here independently of
    /// `digest` so a change to either is caught.
    #[test]
    fn a_shape_id_is_recomputable_from_the_shape() {
        use sha2::{Digest, Sha256};

        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"rustcdc/v2/row-shape");
        for part in ["app", "orders"] {
            bytes.extend_from_slice(&(part.len() as u32).to_be_bytes());
            bytes.extend_from_slice(part.as_bytes());
        }
        bytes.extend_from_slice(&2u32.to_be_bytes());
        for (name, data_type, nullable) in [("id", "bigint", 0u8), ("amount", "decimal(12,4)", 1)] {
            for part in [name, data_type] {
                bytes.extend_from_slice(&(part.len() as u32).to_be_bytes());
                bytes.extend_from_slice(part.as_bytes());
            }
            bytes.push(nullable);
        }
        bytes.extend_from_slice(&1u32.to_be_bytes());
        bytes.extend_from_slice(&2u32.to_be_bytes());
        bytes.extend_from_slice(b"id");
        let expected: String = Sha256::digest(&bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();

        let shape = orders();
        assert_eq!(shape.id(), expected);
        assert_eq!(shape.computed_id().as_deref(), Some(expected.as_str()));
        assert!(RowShape::is_valid_id(shape.id()));
    }

    #[test]
    fn every_field_of_the_shape_moves_the_id() {
        let base = orders();
        let variants = [
            RowShape::new(
                "other",
                "orders",
                base.columns().unwrap().to_vec(),
                vec!["id".into()],
            ),
            RowShape::new(
                "app",
                "other",
                base.columns().unwrap().to_vec(),
                vec!["id".into()],
            ),
            RowShape::new(
                "app",
                "orders",
                vec![
                    ShapeColumn::new("id", "bigint", true),
                    ShapeColumn::new("amount", "decimal(12,4)", true),
                ],
                vec!["id".into()],
            ),
            RowShape::new(
                "app",
                "orders",
                vec![
                    ShapeColumn::new("id", "bigint", false),
                    ShapeColumn::new("amount", "decimal(12,2)", true),
                ],
                vec!["id".into()],
            ),
            RowShape::new(
                "app",
                "orders",
                base.columns().unwrap().to_vec(),
                Vec::new(),
            ),
        ];
        for variant in variants {
            assert_ne!(variant.unwrap().id(), base.id());
        }
        // A → B → A: the id names a shape, not a point in history.
        assert_eq!(orders().id(), base.id());
    }

    #[test]
    fn a_complete_and_a_compact_shape_round_trip_through_json() {
        let shape = orders();
        let json = serde_json::to_value(&shape).unwrap();
        assert_eq!(json["columns"][1]["data_type"], "decimal(12,4)");
        assert_eq!(json["primary_key"][0], "id");
        assert_eq!(serde_json::from_value::<RowShape>(json).unwrap(), shape);

        let compact = shape.compact();
        let json = serde_json::to_value(&compact).unwrap();
        assert!(json.get("columns").is_none() && json.get("primary_key").is_none());
        let decoded: RowShape = serde_json::from_value(json).unwrap();
        assert!(decoded.is_compact());
        assert_eq!(decoded.id(), shape.id());
    }

    #[test]
    fn a_shape_with_columns_but_no_key_list_is_rejected() {
        let mut json = serde_json::to_value(orders()).unwrap();
        json.as_object_mut().unwrap().remove("primary_key");
        assert!(serde_json::from_value::<RowShape>(json).is_err());

        let mut json = serde_json::to_value(orders()).unwrap();
        json["id"] = "SHAPE".into();
        assert!(serde_json::from_value::<RowShape>(json).is_err());
    }

    #[test]
    fn narrowing_drops_columns_and_moves_the_id_but_never_drops_a_key_column() {
        let shape = orders();
        let narrowed = shape
            .without_columns(&HashSet::from(["amount".to_string()]))
            .unwrap();
        assert_eq!(narrowed.columns().unwrap().len(), 1);
        assert_ne!(narrowed.id(), shape.id());
        assert!(narrowed.is_narrowed());
        assert_eq!(narrowed.computed_id().as_deref(), Some(narrowed.id()));

        assert!(
            shape
                .without_columns(&HashSet::from(["id".to_string()]))
                .is_none()
        );
    }
}
