//! How a Kafka topic name is derived from an event.
//!
//! A sink's `topic` is either a **literal** name — one sink, one topic, which is what it
//! has always been — or a **template** carrying `${schema}` / `${table}` placeholders,
//! resolved per event so one sink writes the topic-per-table layout Debezium produces by
//! default (`<prefix>.<schema>.<table>`).
//!
//! # Why this is its own module
//!
//! Three things have to agree about a topic name and they run at three different times:
//!
//! * **config load** parses the template and rejects what can never work — an unknown
//!   placeholder, a literal segment carrying characters Kafka forbids, a literal name
//!   over the length limit;
//! * **preflight** renders the template against the tables the configuration already
//!   names and asks the broker whether those topics exist;
//! * **the hot path** renders it per event.
//!
//! Splitting the rules across those three call sites is how they drift, and a drift here
//! is a topic name that passes startup and is rejected by the broker mid-stream. One
//! parser, one renderer, one character policy — used by all three.
//!
//! # What is deliberately not a placeholder
//!
//! `${op}`. Splitting a table's inserts, updates and deletes across topics destroys the
//! per-key ordering the whole pipeline is built to preserve: a consumer replaying the
//! topics would see a delete before the insert it follows. It is not offered rather than
//! offered-with-a-warning, because the warning would be read after the topics existed.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// Kafka's hard ceiling on a topic name, from `Topic.validate` in the broker.
pub const KAFKA_TOPIC_MAX_LENGTH: usize = 249;

/// The character class Kafka accepts in a topic name.
pub const KAFKA_TOPIC_LEGAL_CHARACTERS: &str = "[a-zA-Z0-9._-]";

/// Separator for cache keys. NUL cannot appear in a PostgreSQL, MySQL or SQL Server
/// identifier — the protocols themselves are NUL-terminated — so a key built with it
/// cannot be forged by two different `(schema, table)` pairs.
const CACHE_KEY_SEPARATOR: char = '\0';

/// Is this character legal in a Kafka topic name?
#[inline]
pub fn is_legal_topic_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-'
}

// ─────────────────────────────────────────────────────────────────────────────
// Placeholders
// ─────────────────────────────────────────────────────────────────────────────

/// A value a topic template can interpolate from the event envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Placeholder {
    /// [`Event::schema`](rustcdc::Event::schema) — the PostgreSQL/SQL Server schema, or
    /// the MySQL/MariaDB database.
    Schema,
    /// [`Event::table`](rustcdc::Event::table).
    Table,
}

impl Placeholder {
    /// Every placeholder, in the order the error messages list them.
    pub const ALL: [Placeholder; 2] = [Placeholder::Schema, Placeholder::Table];

    pub fn name(self) -> &'static str {
        match self {
            Placeholder::Schema => "schema",
            Placeholder::Table => "table",
        }
    }

    fn parse(name: &str) -> Option<Self> {
        match name {
            "schema" => Some(Placeholder::Schema),
            "table" => Some(Placeholder::Table),
            _ => None,
        }
    }

    fn known_list() -> String {
        Self::ALL
            .iter()
            .map(|p| format!("${{{}}}", p.name()))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Naming policy
// ─────────────────────────────────────────────────────────────────────────────

/// What to do when a rendered topic name contains a character Kafka forbids.
///
/// PostgreSQL identifiers, quoted, are far more permissive than Kafka topic names: `my
/// table`, `order#items` and `bestellungen_für` are all legal tables and none is a legal
/// topic. Interpolating one produces a name the broker rejects.
///
/// A dot is a separate matter and not one this policy touches: `.` *is* legal in a topic
/// name, so a table called `orders.2026` renders to `cdc.public.orders.2026` and is
/// accepted — it simply reads as a four-segment name. Rewriting it would be a change
/// nobody asked for, and `.` is refused as a [`TopicNamingConfig::replacement`] for the
/// mirror-image reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InvalidCharacterPolicy {
    /// Fail the event. It is permanently undeliverable and attributable to the record,
    /// so it is dead-lettered when `[dlq]` is configured and halts the pipeline when it
    /// is not.
    ///
    /// The default, because the alternative changes a topic name without being asked and
    /// a topic name is a published interface.
    #[default]
    Reject,

    /// Replace each illegal character with [`TopicNamingConfig::replacement`], the way
    /// Debezium does.
    ///
    /// Two tables can collapse onto one topic this way — `my table` and `my_table` both
    /// render to `my_table` — which interleaves two change streams under keys that were
    /// only ever unique per table. [`TopicResolver`] refuses the second table rather
    /// than letting that happen silently; see [`TopicResolveError::Collision`].
    Replace,
}

/// How rendered topic names are made legal.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct TopicNamingConfig {
    /// Policy for characters outside `[a-zA-Z0-9._-]`. Default: `reject`.
    #[serde(default)]
    pub invalid_characters: InvalidCharacterPolicy,

    /// The character `invalid_characters = "replace"` substitutes. Default: `_`.
    #[serde(default = "default_replacement")]
    pub replacement: String,
}

fn default_replacement() -> String {
    "_".to_string()
}

impl Default for TopicNamingConfig {
    fn default() -> Self {
        Self {
            invalid_characters: InvalidCharacterPolicy::default(),
            replacement: default_replacement(),
        }
    }
}

impl TopicNamingConfig {
    pub fn validate(&self, path: &str) -> Result<(), String> {
        let mut chars = self.replacement.chars();
        let (Some(replacement), None) = (chars.next(), chars.next()) else {
            return Err(format!(
                "{path}.replacement must be exactly one character, not {:?}",
                self.replacement
            ));
        };

        if !is_legal_topic_char(replacement) {
            return Err(format!(
                "{path}.replacement {replacement:?} is not legal in a Kafka topic name \
                 ({KAFKA_TOPIC_LEGAL_CHARACTERS})"
            ));
        }

        // `.` is legal in a topic name but it is the separator the template itself uses,
        // so replacing with it moves the segment boundary: `cdc.public.my table` would
        // render as `cdc.public.my.table`, which reads as a four-segment name and is
        // indistinguishable from a table genuinely called `my.table`.
        if replacement == '.' {
            return Err(format!(
                "{path}.replacement must not be \".\": it is the separator between \
                 template segments, so replacing with it makes a sanitised name \
                 indistinguishable from a differently-structured one. Use \"_\" or \"-\"."
            ));
        }

        Ok(())
    }

    fn replacement_char(&self) -> char {
        self.replacement.chars().next().unwrap_or('_')
    }
}

/// The sentence that follows "known placeholders are …" for the two near-misses worth
/// naming.
///
/// `${op}` is the placeholder most likely to be reached for and the one that silently
/// destroys ordering, so its refusal says why rather than leaving the operator to
/// conclude the feature is merely incomplete. `${database}` is what an operator arriving
/// from Debezium's MySQL connector writes; on MySQL and MariaDB the database *is* what
/// `${schema}` carries, so that is a redirection rather than a refusal.
fn unknown_placeholder_hint(name: &str) -> &'static str {
    match name {
        "op" | "operation" => {
            " A topic name is deliberately not split by operation: a table's inserts, \
             updates and deletes would land on separate topics, and a consumer replaying \
             them would see a delete before the insert it follows."
        }
        "db" | "database" => {
            " On MySQL and MariaDB the database is what \"${schema}\" carries, so that \
             is the one to use."
        }
        _ => "",
    }
}

/// Validate a topic name that must be a plain name — no placeholders.
///
/// Every Kafka topic in this configuration that is *not* `sink.kafka.topic` is one of
/// these: the dead-letter topic, the compacted state topic, the admin notification and
/// signal-ingress topics. All four used to be checked for emptiness and nothing else, so
/// a name the broker would reject — a space, a `#`, 300 characters, `..` — loaded
/// cleanly and failed on first use. For the DLQ that is precisely the worst moment: the
/// dead-letter path runs during an incident, and a topic name discovered to be invalid
/// then turns a quarantine into an outage.
///
/// A `${` is called out by name rather than reported as an illegal character, because
/// the natural thing to try after reading about `sink.kafka.topic` is to template one of
/// these too. They are single destinations by design — one dead-letter topic, one state
/// topic — so there is nothing to interpolate.
pub fn validate_literal_topic(path: &str, topic: &str) -> Result<(), String> {
    if topic.trim().is_empty() {
        return Err(format!("{path} must not be empty"));
    }

    if topic.contains("${") {
        return Err(format!(
            "{path} = {topic:?} contains a placeholder, but only sink.kafka.topic is \
             resolved per event. This is a single destination, so there is nothing to \
             interpolate — write the name out."
        ));
    }

    if let Some(bad) = topic.chars().find(|c| !is_legal_topic_char(*c)) {
        return Err(format!(
            "{path} = {topic:?} contains {bad:?}, which is not legal in a Kafka topic \
             name ({KAFKA_TOPIC_LEGAL_CHARACTERS})"
        ));
    }

    if topic == "." || topic == ".." {
        return Err(format!(
            "{path} = {topic:?} is reserved: Kafka rejects \".\" and \"..\" because they \
             collide with directory entries in the log dir"
        ));
    }

    if topic.len() > KAFKA_TOPIC_MAX_LENGTH {
        return Err(format!(
            "{path} = {topic:?} is {} characters; Kafka's limit is {KAFKA_TOPIC_MAX_LENGTH}",
            topic.len()
        ));
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Template
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
enum Segment {
    Literal(String),
    Placeholder(Placeholder),
}

/// A parsed `topic` value: either a literal name or a per-event template.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicTemplate {
    segments: Vec<Segment>,
    /// Exactly what the operator wrote, so errors can quote it back.
    source: String,
}

impl TopicTemplate {
    /// Parse a `topic` value, rejecting anything that can never render.
    ///
    /// `${` always opens a placeholder. A bare `$` is an ordinary literal character —
    /// and an illegal one in a Kafka topic name, so it is caught by the literal-segment
    /// check below rather than needing an escape syntax nobody would remember.
    pub fn parse(source: &str) -> Result<Self, String> {
        let mut segments = Vec::new();
        let mut literal = String::new();
        let mut rest = source;

        while let Some(start) = rest.find("${") {
            literal.push_str(&rest[..start]);
            let after = &rest[start + 2..];
            let Some(end) = after.find('}') else {
                return Err(format!(
                    "topic {source:?} has an unterminated placeholder: \"${{\" at byte \
                     {} is never closed by \"}}\"",
                    source.len() - rest.len() + start
                ));
            };

            let name = &after[..end];
            let Some(placeholder) = Placeholder::parse(name) else {
                return Err(format!(
                    "topic {source:?} uses unknown placeholder \"${{{name}}}\"; known \
                     placeholders are {}.{}",
                    Placeholder::known_list(),
                    unknown_placeholder_hint(name)
                ));
            };

            if !literal.is_empty() {
                segments.push(Segment::Literal(std::mem::take(&mut literal)));
            }
            segments.push(Segment::Placeholder(placeholder));
            rest = &after[end + 1..];
        }

        literal.push_str(rest);
        if !literal.is_empty() {
            segments.push(Segment::Literal(literal));
        }

        let template = Self {
            segments,
            source: source.to_string(),
        };
        template.validate_static()?;
        Ok(template)
    }

    /// Everything decidable without an event.
    fn validate_static(&self) -> Result<(), String> {
        let source = &self.source;

        if source.trim().is_empty() {
            return Err("topic must not be empty".to_string());
        }

        // A literal segment is never sanitised — `invalid_characters = "replace"` exists
        // for *identifiers*, which arrive at runtime, not for a name the operator typed
        // and can fix now.
        for segment in &self.segments {
            let Segment::Literal(text) = segment else {
                continue;
            };
            if let Some(bad) = text.chars().find(|c| !is_legal_topic_char(*c)) {
                return Err(format!(
                    "topic {source:?} contains {bad:?}, which is not legal in a Kafka \
                     topic name ({KAFKA_TOPIC_LEGAL_CHARACTERS}). Placeholders are \
                     written \"${{schema}}\" and \"${{table}}\"."
                ));
            }
        }

        match self.as_literal() {
            // A literal name is fully decidable now, so decide all of it now.
            Some(literal) => {
                if literal == "." || literal == ".." {
                    return Err(format!(
                        "topic {literal:?} is reserved: Kafka rejects \".\" and \"..\" \
                         because they collide with directory entries in the log dir"
                    ));
                }
                if literal.len() > KAFKA_TOPIC_MAX_LENGTH {
                    return Err(format!(
                        "topic {literal:?} is {} characters; Kafka's limit is \
                         {KAFKA_TOPIC_MAX_LENGTH}",
                        literal.len()
                    ));
                }
            }
            // A template's length depends on identifiers that do not exist yet, so only
            // the part that is already fixed can be checked. Rendering can still
            // overflow, and `render` reports that per event.
            None => {
                let fixed: usize = self
                    .segments
                    .iter()
                    .map(|segment| match segment {
                        Segment::Literal(text) => text.len(),
                        Segment::Placeholder(_) => 0,
                    })
                    .sum();
                if fixed > KAFKA_TOPIC_MAX_LENGTH {
                    return Err(format!(
                        "topic {source:?} has {fixed} literal characters before any \
                         placeholder is substituted; Kafka's limit is \
                         {KAFKA_TOPIC_MAX_LENGTH}"
                    ));
                }
            }
        }

        Ok(())
    }

    /// The template text as written.
    pub fn source(&self) -> &str {
        &self.source
    }

    /// The whole name, when it carries no placeholders.
    pub fn as_literal(&self) -> Option<&str> {
        match self.segments.as_slice() {
            [Segment::Literal(text)] => Some(text),
            _ => None,
        }
    }

    /// Does this template need an event to produce a name?
    pub fn is_templated(&self) -> bool {
        self.as_literal().is_none()
    }

    /// Render the template for one event's `(schema, table)`.
    ///
    /// Convenience over [`render_detailed`](Self::render_detailed) for callers that only
    /// want the name.
    pub fn render(
        &self,
        schema: Option<&str>,
        table: &str,
        naming: &TopicNamingConfig,
    ) -> Result<String, TopicRenderError> {
        self.render_detailed(schema, table, naming)
            .map(|rendered| rendered.name)
    }

    /// Render, reporting whether the name had to be altered to become legal.
    ///
    /// The flag is not cosmetic. A template that maps two tables onto one topic *by
    /// construction* — `topic = "cdc.${schema}"`, or a literal name — is doing exactly
    /// what it says, and merging those tables is the operator's stated intent. A
    /// template that maps them together only because sanitisation rewrote an identifier
    /// is a different thing entirely: nobody asked for it and nothing in the config
    /// shows it. [`TopicResolver`] treats only the second as a collision.
    pub fn render_detailed(
        &self,
        schema: Option<&str>,
        table: &str,
        naming: &TopicNamingConfig,
    ) -> Result<Rendered, TopicRenderError> {
        let mut out = String::with_capacity(self.source.len() + table.len());

        for segment in &self.segments {
            match segment {
                Segment::Literal(text) => out.push_str(text),
                Segment::Placeholder(Placeholder::Table) => out.push_str(table),
                Segment::Placeholder(Placeholder::Schema) => {
                    // `Event::schema` is `Option`, and `qualified_table_name` treats an
                    // empty string as absent too. A template that asks for a schema the
                    // event does not carry has no honest name to fall back to: dropping
                    // the segment yields `cdc..orders`, and substituting a placeholder
                    // word merges every schemaless table into one namespace.
                    let schema = schema.filter(|s| !s.is_empty()).ok_or_else(|| {
                        TopicRenderError::MissingSchema {
                            template: self.source.clone(),
                            table: table.to_string(),
                        }
                    })?;
                    out.push_str(schema);
                }
            }
        }

        self.finish(out, table, naming)
    }

    /// Apply the character policy and the broker's own rules to a substituted name.
    fn finish(
        &self,
        rendered: String,
        table: &str,
        naming: &TopicNamingConfig,
    ) -> Result<Rendered, TopicRenderError> {
        let mut sanitised = false;
        let rendered = match naming.invalid_characters {
            InvalidCharacterPolicy::Reject => {
                if let Some(bad) = rendered.chars().find(|c| !is_legal_topic_char(*c)) {
                    return Err(TopicRenderError::IllegalCharacter {
                        template: self.source.clone(),
                        rendered,
                        table: table.to_string(),
                        character: bad,
                    });
                }
                rendered
            }
            InvalidCharacterPolicy::Replace => {
                let replacement = naming.replacement_char();
                if rendered.chars().all(is_legal_topic_char) {
                    rendered
                } else {
                    sanitised = true;
                    rendered
                        .chars()
                        .map(|c| {
                            if is_legal_topic_char(c) {
                                c
                            } else {
                                replacement
                            }
                        })
                        .collect()
                }
            }
        };

        if rendered.is_empty() {
            return Err(TopicRenderError::Empty {
                template: self.source.clone(),
                table: table.to_string(),
            });
        }

        if rendered == "." || rendered == ".." {
            return Err(TopicRenderError::Reserved {
                template: self.source.clone(),
                rendered,
                table: table.to_string(),
            });
        }

        if rendered.len() > KAFKA_TOPIC_MAX_LENGTH {
            return Err(TopicRenderError::TooLong {
                template: self.source.clone(),
                length: rendered.len(),
                table: table.to_string(),
            });
        }

        Ok(Rendered {
            name: rendered,
            sanitised,
        })
    }
}

/// A rendered topic name, and how it got that way.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rendered {
    /// The topic name, legal for Kafka.
    pub name: String,
    /// Whether any character had to be replaced to make it legal.
    pub sanitised: bool,
}

// ─────────────────────────────────────────────────────────────────────────────
// Errors
// ─────────────────────────────────────────────────────────────────────────────

/// A name this event cannot be given.
///
/// Every variant is a property of the event's own identifiers, which is what makes the
/// event dead-letterable rather than the pipeline fatal: quarantining the table with the
/// unrepresentable name lets every other table keep flowing.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TopicRenderError {
    #[error(
        "topic template {template:?} interpolates ${{schema}}, but the event for table \
         {table:?} carries no schema. Either the source does not report one, or this \
         event is a synthetic envelope that has no table behind it — use a topic \
         template without ${{schema}}, or route those tables to a sink that has one."
    )]
    MissingSchema { template: String, table: String },

    #[error(
        "topic template {template:?} rendered {rendered:?} for table {table:?}, which \
         contains {character:?} — not legal in a Kafka topic name \
         ({KAFKA_TOPIC_LEGAL_CHARACTERS}). Set \
         sink.kafka.topic_naming.invalid_characters = \"replace\" to substitute illegal \
         characters, or route this table to a sink with a literal topic."
    )]
    IllegalCharacter {
        template: String,
        rendered: String,
        table: String,
        character: char,
    },

    #[error(
        "topic template {template:?} rendered an empty name for table {table:?}; Kafka \
         topic names must be at least one character"
    )]
    Empty { template: String, table: String },

    #[error(
        "topic template {template:?} rendered {rendered:?} for table {table:?}; Kafka \
         reserves \".\" and \"..\""
    )]
    Reserved {
        template: String,
        rendered: String,
        table: String,
    },

    #[error(
        "topic template {template:?} rendered {length} characters for table {table:?}; \
         Kafka's limit is {KAFKA_TOPIC_MAX_LENGTH}. Truncating would map two tables onto \
         one topic, so the event is rejected instead."
    )]
    TooLong {
        template: String,
        length: usize,
        table: String,
    },
}

/// What [`TopicResolver::resolve`] can refuse.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TopicResolveError {
    #[error(transparent)]
    Render(#[from] TopicRenderError),

    /// Two distinct tables render to one topic name **because sanitisation rewrote at
    /// least one of them**.
    ///
    /// Deliberate merges are not collisions. A literal topic maps every table onto one
    /// name; `topic = "cdc.${schema}"` maps a whole schema onto one. Both are legible in
    /// the configuration and both are what the operator asked for. What is not legible
    /// is `my table` and `my_table` arriving on one topic because
    /// `invalid_characters = "replace"` erased the difference — nothing in the config
    /// says so, and the result interleaves two change streams under keys that are only
    /// unique per table.
    ///
    /// It is *not* the event's fault — the other table is equally responsible — so this
    /// halts rather than dead-letters: quarantining one of the two would drain a healthy
    /// table into the DLQ and leave the naming collision in place.
    #[error(
        "topic naming collision: {left} and {right} both render to topic {topic:?} under \
         template {template:?}, because invalid_characters = \"replace\" rewrote an \
         identifier. Two tables sharing one topic interleaves their change streams under \
         keys that are only unique per table. Give the tables distinguishable names, \
         route one of them to its own sink, or set invalid_characters = \"reject\" so \
         the unrepresentable table is dead-lettered instead of merged."
    )]
    Collision {
        template: String,
        topic: String,
        left: String,
        right: String,
    },
}

// ─────────────────────────────────────────────────────────────────────────────
// Qualified table names, as they appear in configuration
// ─────────────────────────────────────────────────────────────────────────────

/// A `"schema.table"` entry from configuration, split the way an event carries it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct QualifiedTable {
    pub schema: Option<String>,
    pub table: String,
}

impl QualifiedTable {
    /// Split a configured `"schema.table"`, or `"table"` when unqualified.
    ///
    /// Splits on the **first** `.`, matching how the connectors build the qualified name
    /// (`format!("{schema}.{table}")`) and how the routing matcher segments a pattern.
    /// Returns `None` for anything that cannot be a concrete table: an empty entry, or
    /// one carrying glob metacharacters — `table_include_list` takes patterns, and
    /// `cdc.public.*` is not a topic anyone can preflight.
    pub fn parse_concrete(entry: &str) -> Option<Self> {
        let entry = entry.trim();
        if entry.is_empty() || entry.contains(['*', '?']) {
            return None;
        }

        Some(match entry.split_once('.') {
            Some((schema, table)) if !schema.is_empty() && !table.is_empty() => Self {
                schema: Some(schema.to_string()),
                table: table.to_string(),
            },
            _ => Self {
                schema: None,
                table: entry.to_string(),
            },
        })
    }

    pub fn display(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{schema}.{}", self.table),
            None => self.table.clone(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Runtime resolution
// ─────────────────────────────────────────────────────────────────────────────

/// Renders a [`TopicTemplate`] per event, once per distinct table.
///
/// # Why a cache at all
///
/// Not allocation: the sink already allocated a `String` per record for the topic before
/// templates existed, so rendering per event would be no worse. The cache exists for the
/// *checks* — the character scan, the length test, and above all the collision test,
/// which is only meaningful against the set of names already handed out.
///
/// A single-entry fast path sits in front of the map because change events arrive in
/// runs from the same table: a transaction touches one table many times before it
/// touches another. On that path resolution is two string comparisons and an `Arc`
/// clone, with no hashing and no allocation.
#[derive(Debug)]
pub struct TopicResolver {
    template: TopicTemplate,
    naming: TopicNamingConfig,
    /// Every `(schema, table)` resolved so far, keyed by `schema\0table`.
    ///
    /// Bounded by the number of distinct tables the pipeline has seen, and entries are
    /// never evicted: `origins` below has to remember every name handed out, or a
    /// collision could be missed by a table arriving after an eviction. A few tens of
    /// bytes per table is not a budget worth managing.
    cache: HashMap<String, Arc<str>>,
    /// The most recently resolved `(schema, table)` and the topic it produced.
    ///
    /// This is the single-entry fast path. It is kept beside the map rather than in it
    /// because the win is skipping the hash, not skipping the lookup.
    last: Option<(String, String, Arc<str>)>,
    /// Reusable buffer for building a cache key without allocating on lookup.
    key_scratch: String,
    /// Which table first claimed each topic name, and whether sanitisation was involved
    /// in getting it there. See [`TopicResolveError::Collision`] for why the flag decides
    /// whether a shared name is a merge or a mistake.
    origins: HashMap<Arc<str>, Origin>,
}

/// The first claim on a topic name.
#[derive(Debug, Clone)]
struct Origin {
    /// `schema.table`, as the error messages spell it.
    table: String,
    /// Whether the name had to be rewritten to become legal.
    sanitised: bool,
}

impl TopicResolver {
    pub fn new(template: TopicTemplate, naming: TopicNamingConfig) -> Self {
        Self {
            template,
            naming,
            cache: HashMap::new(),
            last: None,
            key_scratch: String::new(),
            origins: HashMap::new(),
        }
    }

    pub fn template(&self) -> &TopicTemplate {
        &self.template
    }

    /// The topic for one event, rendering and validating it the first time its table is
    /// seen and returning the cached name afterwards.
    ///
    /// Three tiers, cheapest first: the single-entry fast path (two string comparisons and
    /// an `Arc` clone — no hashing, no allocation), then the map, then a render. The fast
    /// path is what the access pattern actually looks like: a transaction touches one
    /// table many times before it touches another, so it hits on nearly every event after
    /// the first.
    ///
    /// None of this is really about allocation — before templates existed the sink cloned
    /// a topic `String` for every record, so even rendering afresh each time would not be
    /// a regression. What the cache buys is the *checks*: the character scan, the length
    /// test, and above all the collision test, which is only meaningful against the set of
    /// names already handed out.
    pub fn resolve(
        &mut self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Arc<str>, TopicResolveError> {
        // `None` and `Some("")` render identically, so they share one key here too.
        let schema_key = schema.unwrap_or("");

        if let Some((last_schema, last_table, topic)) = &self.last
            && last_schema.as_str() == schema_key
            && last_table.as_str() == table
        {
            return Ok(Arc::clone(topic));
        }

        self.key_scratch.clear();
        self.key_scratch.push_str(schema_key);
        self.key_scratch.push(CACHE_KEY_SEPARATOR);
        self.key_scratch.push_str(table);

        let topic = match self.cache.get(self.key_scratch.as_str()) {
            Some(topic) => Arc::clone(topic),
            None => self.insert(schema, table)?,
        };

        // Only a successful resolve arms the fast path: a table whose name is rejected must
        // be rejected again on its next event, not answered from a cache of one.
        self.last = Some((
            schema_key.to_string(),
            table.to_string(),
            Arc::clone(&topic),
        ));
        Ok(topic)
    }

    /// Render, check for a collision, and record the result. Cold path.
    fn insert(&mut self, schema: Option<&str>, table: &str) -> Result<Arc<str>, TopicResolveError> {
        let rendered = self.template.render_detailed(schema, table, &self.naming)?;
        let topic: Arc<str> = Arc::from(rendered.name.as_str());
        let qualified = qualified(schema, table);

        match self.origins.get(&topic) {
            // A name two tables reach without either being rewritten is the template
            // doing exactly what it says — a literal topic, or one that interpolates
            // only `${schema}`. Merging there is the configuration, not an accident.
            Some(origin)
                if origin.table != qualified && (origin.sanitised || rendered.sanitised) =>
            {
                return Err(TopicResolveError::Collision {
                    template: self.template.source.clone(),
                    topic: rendered.name,
                    left: origin.table.clone(),
                    right: qualified,
                });
            }
            Some(_) => {}
            None => {
                self.origins.insert(
                    Arc::clone(&topic),
                    Origin {
                        table: qualified,
                        sanitised: rendered.sanitised,
                    },
                );
            }
        }

        let mut key = String::with_capacity(self.key_scratch.len());
        key.push_str(&self.key_scratch);
        self.cache.insert(key, Arc::clone(&topic));

        Ok(topic)
    }

    /// Render `tables` up front, dropping the ones that cannot be named.
    ///
    /// Used by preflight, which wants every topic it can check and a reason for each one
    /// it cannot — a table whose name needs sanitising under `reject` is a genuine
    /// startup warning, not a silent omission. The resolver keeps the results, so a
    /// table preflighted here costs nothing again on the hot path.
    pub fn warm(&mut self, tables: &[QualifiedTable]) -> (Vec<String>, Vec<String>) {
        let mut resolved = Vec::new();
        let mut skipped = Vec::new();

        for entry in tables {
            match self.resolve(entry.schema.as_deref(), &entry.table) {
                Ok(topic) => resolved.push(topic.to_string()),
                Err(error) => skipped.push(format!("{}: {error}", entry.display())),
            }
        }

        resolved.sort_unstable();
        resolved.dedup();
        (resolved, skipped)
    }
}

/// Format a `(schema, table)` the way the error messages and logs do.
pub fn qualified(schema: Option<&str>, table: &str) -> String {
    let mut out = String::with_capacity(table.len() + 16);
    if let Some(schema) = schema.filter(|s| !s.is_empty()) {
        let _ = write!(out, "{schema}.");
    }
    out.push_str(table);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reject() -> TopicNamingConfig {
        TopicNamingConfig::default()
    }

    fn replace() -> TopicNamingConfig {
        TopicNamingConfig {
            invalid_characters: InvalidCharacterPolicy::Replace,
            replacement: "_".to_string(),
        }
    }

    #[test]
    fn a_literal_topic_parses_to_one_segment_and_reports_itself_as_literal() {
        let template = TopicTemplate::parse("cdc.events").expect("literal");
        assert_eq!(template.as_literal(), Some("cdc.events"));
        assert!(!template.is_templated());
    }

    #[test]
    fn the_debezium_layout_renders_prefix_schema_table() {
        let template = TopicTemplate::parse("cdc.${schema}.${table}").expect("template");
        assert!(template.is_templated());
        assert_eq!(
            template
                .render(Some("public"), "orders", &reject())
                .unwrap(),
            "cdc.public.orders"
        );
    }

    #[test]
    fn a_template_may_use_one_placeholder_twice_and_in_any_position() {
        let template = TopicTemplate::parse("${table}.cdc.${table}").expect("template");
        assert_eq!(
            template.render(None, "orders", &reject()).unwrap(),
            "orders.cdc.orders"
        );
    }

    #[test]
    fn an_unknown_placeholder_is_rejected_at_parse_with_the_known_set_named() {
        let error = TopicTemplate::parse("cdc.${tenant}.${table}").expect_err("unknown");
        assert!(error.contains("${tenant}"), "{error}");
        assert!(error.contains("${schema}"), "{error}");
        assert!(error.contains("${table}"), "{error}");
    }

    /// `${database}` is what an operator arriving from Debezium's MySQL connector writes,
    /// and on MySQL the database is exactly what `${schema}` carries. Saying so is worth
    /// more than listing the known set again.
    #[test]
    fn the_database_placeholder_is_redirected_to_schema() {
        for name in ["db", "database"] {
            let error =
                TopicTemplate::parse(&format!("cdc.${{{name}}}.${{table}}")).expect_err("unknown");
            assert!(error.contains("MySQL"), "{error}");
            assert!(error.contains("${schema}"), "{error}");
        }
    }

    /// `${op}` is the one placeholder an operator is most likely to reach for and the
    /// one that silently destroys ordering, so its rejection says why.
    #[test]
    fn the_op_placeholder_is_rejected_with_a_pointer_to_the_reason() {
        let error = TopicTemplate::parse("cdc.${op}").expect_err("op");
        assert!(error.contains("${op}"), "{error}");
        assert!(error.contains("not split by operation"), "{error}");
        assert!(error.contains("delete before the insert"), "{error}");
    }

    /// An ordinary typo gets the known set and nothing else — the ordering lecture
    /// belongs only to the placeholder it is about.
    #[test]
    fn an_unrelated_typo_does_not_get_the_ordering_lecture() {
        let error = TopicTemplate::parse("cdc.${tabel}").expect_err("typo");
        assert!(!error.contains("not split by operation"), "{error}");
    }

    #[test]
    fn an_unterminated_placeholder_is_rejected_rather_than_treated_as_literal() {
        let error = TopicTemplate::parse("cdc.${table").expect_err("unterminated");
        assert!(error.contains("unterminated"), "{error}");
    }

    #[test]
    fn a_literal_segment_with_an_illegal_character_is_rejected_at_parse() {
        let error = TopicTemplate::parse("cdc events.${table}").expect_err("space");
        assert!(error.contains("not legal"), "{error}");
    }

    /// A bare `$` needs no escape syntax: it is illegal in a topic name anyway, so the
    /// literal-segment check catches it and says so.
    #[test]
    fn a_bare_dollar_is_rejected_as_an_illegal_literal_character() {
        let error = TopicTemplate::parse("cdc$events").expect_err("dollar");
        assert!(error.contains("not legal"), "{error}");
    }

    #[test]
    fn an_empty_topic_is_rejected() {
        assert!(TopicTemplate::parse("").is_err());
        assert!(TopicTemplate::parse("   ").is_err());
    }

    #[test]
    fn a_literal_topic_over_the_kafka_limit_is_rejected_at_parse() {
        let long = "a".repeat(KAFKA_TOPIC_MAX_LENGTH + 1);
        let error = TopicTemplate::parse(&long).expect_err("too long");
        assert!(error.contains("249"), "{error}");
    }

    #[test]
    fn the_reserved_dot_names_are_rejected_at_parse() {
        assert!(TopicTemplate::parse(".").is_err());
        assert!(TopicTemplate::parse("..").is_err());
    }

    #[test]
    fn a_template_whose_literal_part_already_overflows_is_rejected_at_parse() {
        let source = format!("{}.${{table}}", "a".repeat(KAFKA_TOPIC_MAX_LENGTH));
        let error = TopicTemplate::parse(&source).expect_err("too long");
        assert!(error.contains("literal characters"), "{error}");
    }

    /// The gap the draft proposal missed: `Event::schema` is `Option`, and dropping the
    /// segment would produce `cdc..orders` — a legal Kafka name with an empty segment,
    /// which is worse than an error because it looks deliberate.
    #[test]
    fn a_schema_placeholder_with_no_schema_is_an_error_not_an_empty_segment() {
        let template = TopicTemplate::parse("cdc.${schema}.${table}").expect("template");
        let error = template
            .render(None, "orders", &reject())
            .expect_err("no schema");
        assert!(matches!(error, TopicRenderError::MissingSchema { .. }));
        // An empty string is how `qualified_table_name` already spells "absent".
        assert!(template.render(Some(""), "orders", &reject()).is_err());
    }

    #[test]
    fn reject_is_the_default_policy_for_an_identifier_kafka_cannot_name() {
        let template = TopicTemplate::parse("cdc.${schema}.${table}").expect("template");
        let error = template
            .render(Some("public"), "my table", &reject())
            .expect_err("space");
        match error {
            TopicRenderError::IllegalCharacter { character, .. } => assert_eq!(character, ' '),
            other => panic!("expected IllegalCharacter, got {other:?}"),
        }
    }

    /// A dot is legal in a topic name. A table containing one renders unchanged under
    /// both policies — rewriting it would be a transformation nobody asked for.
    #[test]
    fn a_dotted_table_name_renders_unchanged_because_a_dot_is_legal() {
        let template = TopicTemplate::parse("cdc.${schema}.${table}").expect("template");
        for naming in [reject(), replace()] {
            assert_eq!(
                template
                    .render(Some("public"), "orders.2026", &naming)
                    .unwrap(),
                "cdc.public.orders.2026"
            );
        }
    }

    #[test]
    fn replace_substitutes_every_illegal_character_including_non_ascii() {
        let template = TopicTemplate::parse("cdc.${schema}.${table}").expect("template");
        assert_eq!(
            template
                .render(Some("public"), "my table", &replace())
                .unwrap(),
            "cdc.public.my_table"
        );
        assert_eq!(
            template
                .render(Some("public"), "bestellungen_für", &replace())
                .unwrap(),
            "cdc.public.bestellungen_f_r"
        );
    }

    #[test]
    fn a_rendered_name_over_the_limit_is_rejected_rather_than_truncated() {
        let template = TopicTemplate::parse("cdc.${table}").expect("template");
        let table = "a".repeat(KAFKA_TOPIC_MAX_LENGTH);
        let error = template
            .render(None, &table, &reject())
            .expect_err("too long");
        assert!(matches!(error, TopicRenderError::TooLong { .. }));
    }

    #[test]
    fn a_rendered_reserved_name_is_rejected() {
        let template = TopicTemplate::parse("${table}").expect("template");
        assert!(matches!(
            template.render(None, "..", &reject()),
            Err(TopicRenderError::Reserved { .. })
        ));
    }

    #[test]
    fn an_all_illegal_identifier_under_reject_is_an_illegal_character_error() {
        let template = TopicTemplate::parse("${table}").expect("template");
        assert!(matches!(
            template.render(None, "  ", &reject()),
            Err(TopicRenderError::IllegalCharacter { .. })
        ));
    }

    // ── Literal topics elsewhere in the config ───────────────────────────────

    #[test]
    fn a_literal_topic_field_accepts_an_ordinary_name() {
        assert!(validate_literal_topic("dlq.topic", "cdc.dlq").is_ok());
        assert!(validate_literal_topic("dlq.topic", "__rustcdc_state").is_ok());
    }

    #[test]
    fn a_literal_topic_field_rejects_what_the_broker_would_reject() {
        for bad in ["", "   ", "cdc dlq", "cdc#dlq", ".", ".."] {
            assert!(
                validate_literal_topic("dlq.topic", bad).is_err(),
                "{bad:?} should be rejected"
            );
        }
        let long = "a".repeat(KAFKA_TOPIC_MAX_LENGTH + 1);
        assert!(validate_literal_topic("dlq.topic", &long).is_err());
    }

    /// The obvious thing to try after reading about `sink.kafka.topic`.
    #[test]
    fn a_literal_topic_field_says_why_a_placeholder_does_not_belong_there() {
        let error = validate_literal_topic("dlq.topic", "cdc.${schema}.dlq")
            .expect_err("placeholders are not resolved here");
        assert!(error.contains("sink.kafka.topic"), "{error}");
    }

    // ── Naming config ────────────────────────────────────────────────────────

    #[test]
    fn the_replacement_must_be_one_legal_character_and_never_a_dot() {
        assert!(TopicNamingConfig::default().validate("x").is_ok());
        for bad in ["", "__", " ", "."] {
            let cfg = TopicNamingConfig {
                invalid_characters: InvalidCharacterPolicy::Replace,
                replacement: bad.to_string(),
            };
            assert!(cfg.validate("x").is_err(), "{bad:?} should be rejected");
        }
        let dash = TopicNamingConfig {
            invalid_characters: InvalidCharacterPolicy::Replace,
            replacement: "-".to_string(),
        };
        assert!(dash.validate("x").is_ok());
    }

    // ── Resolver ─────────────────────────────────────────────────────────────

    #[test]
    fn the_resolver_returns_the_same_arc_for_a_repeated_table() {
        let mut resolver = TopicResolver::new(
            TopicTemplate::parse("cdc.${schema}.${table}").unwrap(),
            reject(),
        );
        let first = resolver.resolve(Some("public"), "orders").unwrap();
        let second = resolver.resolve(Some("public"), "orders").unwrap();
        assert_eq!(&*first, "cdc.public.orders");
        assert!(Arc::ptr_eq(&first, &second));
    }

    /// A rejected table must be rejected again on its next event.
    ///
    /// The single-entry fast path is armed only by a *successful* resolve. Arming it on
    /// the way out of `resolve` regardless would cache nothing — the error is not an
    /// `Arc<str>` — but arming it before the render would hand the previous table's topic
    /// to a table whose own name was refused, publishing one table's rows under another
    /// table's name.
    #[test]
    fn a_rejected_table_does_not_answer_from_the_fast_path() {
        let mut resolver = TopicResolver::new(
            TopicTemplate::parse("cdc.${schema}.${table}").unwrap(),
            reject(),
        );
        resolver
            .resolve(Some("public"), "orders")
            .expect("a legal table arms the fast path");
        resolver
            .resolve(Some("public"), "order items")
            .expect_err("a space is not a legal topic character");
        resolver
            .resolve(Some("public"), "order items")
            .expect_err("and it must still be refused on the next event");
    }

    /// The cache key spans both halves: two schemas holding a same-named table are the
    /// case it must not conflate.
    #[test]
    fn the_cache_does_not_conflate_two_schemas_with_the_same_table_name() {
        let mut resolver = TopicResolver::new(
            TopicTemplate::parse("cdc.${schema}.${table}").unwrap(),
            reject(),
        );
        assert_eq!(
            &*resolver.resolve(Some("a"), "orders").unwrap(),
            "cdc.a.orders"
        );
        assert_eq!(
            &*resolver.resolve(Some("b"), "orders").unwrap(),
            "cdc.b.orders"
        );
        assert_eq!(
            &*resolver.resolve(Some("a"), "orders").unwrap(),
            "cdc.a.orders"
        );
    }

    /// The cache key must not let `(a, b.c)` and `(a.b, c)` share an entry.
    #[test]
    fn the_cache_key_separates_a_dotted_schema_from_a_dotted_table() {
        let mut resolver = TopicResolver::new(
            TopicTemplate::parse("${schema}-${table}").unwrap(),
            reject(),
        );
        assert_eq!(&*resolver.resolve(Some("a"), "b.c").unwrap(), "a-b.c");
        assert_eq!(&*resolver.resolve(Some("a.b"), "c").unwrap(), "a.b-c");
    }

    #[test]
    fn a_literal_template_resolves_to_the_same_name_for_every_table() {
        let mut resolver =
            TopicResolver::new(TopicTemplate::parse("cdc.events").unwrap(), reject());
        assert_eq!(&*resolver.resolve(Some("a"), "x").unwrap(), "cdc.events");
        assert_eq!(&*resolver.resolve(Some("b"), "y").unwrap(), "cdc.events");
    }

    /// Sanitisation is what makes two tables collapse onto one topic. Detecting it is
    /// the difference between a documented transformation and silent data corruption.
    #[test]
    fn two_tables_sanitising_onto_one_topic_are_refused_rather_than_merged() {
        let mut resolver = TopicResolver::new(
            TopicTemplate::parse("cdc.${schema}.${table}").unwrap(),
            replace(),
        );
        assert_eq!(
            &*resolver.resolve(Some("public"), "my_table").unwrap(),
            "cdc.public.my_table"
        );
        let error = resolver
            .resolve(Some("public"), "my table")
            .expect_err("collision");
        match error {
            TopicResolveError::Collision {
                topic, left, right, ..
            } => {
                assert_eq!(topic, "cdc.public.my_table");
                assert_eq!(left, "public.my_table");
                assert_eq!(right, "public.my table");
            }
            other => panic!("expected Collision, got {other:?}"),
        }
    }

    /// A template that merges tables *by construction* is the operator saying so. Only
    /// a merge that sanitisation caused is unasked-for.
    #[test]
    fn a_template_that_deliberately_merges_a_schemas_tables_is_not_a_collision() {
        let mut resolver =
            TopicResolver::new(TopicTemplate::parse("cdc.${schema}").unwrap(), replace());
        assert_eq!(
            &*resolver.resolve(Some("public"), "orders").unwrap(),
            "cdc.public"
        );
        assert_eq!(
            &*resolver.resolve(Some("public"), "customers").unwrap(),
            "cdc.public"
        );
    }

    /// The collision must be caught whichever of the two tables arrives first.
    #[test]
    fn a_collision_is_caught_when_the_sanitised_table_arrives_first() {
        let mut resolver = TopicResolver::new(
            TopicTemplate::parse("cdc.${schema}.${table}").unwrap(),
            replace(),
        );
        assert_eq!(
            &*resolver.resolve(Some("public"), "my table").unwrap(),
            "cdc.public.my_table"
        );
        assert!(matches!(
            resolver.resolve(Some("public"), "my_table"),
            Err(TopicResolveError::Collision { .. })
        ));
    }

    #[test]
    fn a_literal_topic_is_never_a_collision_however_many_tables_use_it() {
        let mut resolver =
            TopicResolver::new(TopicTemplate::parse("cdc.events").unwrap(), replace());
        assert!(resolver.resolve(Some("a"), "x").is_ok());
        assert!(resolver.resolve(Some("b"), "y").is_ok());
    }

    #[test]
    fn warm_returns_the_topics_it_could_render_and_a_reason_for_each_one_it_could_not() {
        let mut resolver = TopicResolver::new(
            TopicTemplate::parse("cdc.${schema}.${table}").unwrap(),
            reject(),
        );
        let tables = vec![
            QualifiedTable::parse_concrete("public.orders").unwrap(),
            QualifiedTable::parse_concrete("public.customers").unwrap(),
            // Duplicate: preflight should describe each topic once.
            QualifiedTable::parse_concrete("public.orders").unwrap(),
            // No schema, and the template needs one.
            QualifiedTable::parse_concrete("orders").unwrap(),
        ];
        let (resolved, skipped) = resolver.warm(&tables);
        assert_eq!(resolved, vec!["cdc.public.customers", "cdc.public.orders"]);
        assert_eq!(skipped.len(), 1);
        assert!(skipped[0].starts_with("orders:"), "{:?}", skipped[0]);
    }

    // ── Qualified table parsing ──────────────────────────────────────────────

    #[test]
    fn a_configured_table_splits_on_the_first_dot() {
        let parsed = QualifiedTable::parse_concrete("public.orders").unwrap();
        assert_eq!(parsed.schema.as_deref(), Some("public"));
        assert_eq!(parsed.table, "orders");

        let nested = QualifiedTable::parse_concrete("public.a.b").unwrap();
        assert_eq!(nested.schema.as_deref(), Some("public"));
        assert_eq!(nested.table, "a.b");
    }

    #[test]
    fn an_unqualified_entry_parses_with_no_schema() {
        let parsed = QualifiedTable::parse_concrete("orders").unwrap();
        assert_eq!(parsed.schema, None);
        assert_eq!(parsed.table, "orders");
    }

    /// `table_include_list` takes glob patterns. Rendering `cdc.public.*` and asking the
    /// broker to describe it would fail preflight on a topic that was never meant to
    /// exist — the gap that made the naive version of this check unsound.
    #[test]
    fn a_glob_pattern_is_not_a_concrete_table() {
        assert_eq!(QualifiedTable::parse_concrete("public.*"), None);
        assert_eq!(QualifiedTable::parse_concrete("public.tmp_?"), None);
        assert_eq!(QualifiedTable::parse_concrete("*"), None);
        assert_eq!(QualifiedTable::parse_concrete(""), None);
        assert_eq!(QualifiedTable::parse_concrete("   "), None);
    }

    #[test]
    fn qualified_renders_the_same_shape_as_the_event_helper() {
        assert_eq!(qualified(Some("public"), "orders"), "public.orders");
        assert_eq!(qualified(None, "orders"), "orders");
        assert_eq!(qualified(Some(""), "orders"), "orders");
    }
}
