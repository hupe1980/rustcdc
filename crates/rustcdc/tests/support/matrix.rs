//! The versions the container suites run against, read from `matrix.txt` beside this file.
//!
//! A suite asks for an engine and gets a tag; it never names a version itself, so moving the
//! matrix is one edit (the policy gate holds the suites to that). `RUSTCDC_TEST_<ENGINE>` runs any
//! suite against another tag without an edit: `RUSTCDC_TEST_POSTGRES=17-alpine`.
#![allow(dead_code)]

const MATRIX: &str = include_str!("matrix.txt");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Engine {
    Postgres,
    Mysql,
    Mariadb,
    SqlServer,
    Kafka,
    Redpanda,
}

impl Engine {
    fn key(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::Mysql => "mysql",
            Self::Mariadb => "mariadb",
            Self::SqlServer => "sqlserver",
            Self::Kafka => "kafka",
            Self::Redpanda => "redpanda",
        }
    }

    fn override_var(self) -> String {
        format!("RUSTCDC_TEST_{}", self.key().to_ascii_uppercase())
    }

    fn line(self) -> Vec<&'static str> {
        MATRIX
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .map(|line| line.split_whitespace().collect::<Vec<_>>())
            .find(|fields| fields.first() == Some(&self.key()))
            .unwrap_or_else(|| panic!("{} has no line in tests/support/matrix.txt", self.key()))
    }
}

/// The tag the full suites run on.
pub fn full(engine: Engine) -> &'static str {
    match std::env::var(engine.override_var()) {
        // Once per test process; the image API wants a `&str` that outlives the container.
        Ok(tag) => Box::leak(tag.into_boxed_str()),
        Err(_) => engine.line()[1],
    }
}

/// Every tag the version-matrix suites run on; only the override when one is set.
pub fn tested(engine: Engine) -> Vec<&'static str> {
    match std::env::var(engine.override_var()) {
        Ok(_) => vec![full(engine)],
        Err(_) => engine.line()[2..].to_vec(),
    }
}
