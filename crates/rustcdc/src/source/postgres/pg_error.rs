//! How a PostgreSQL client error reads once it is flattened into a message.
//!
//! `tokio_postgres::Error` displays a server error as the literal `db error`: the SQLSTATE,
//! the message, the detail and the hint all sit behind `as_db_error()`. Formatting it with
//! `{error}` therefore hands an operator `failed fetching snapshot rows: db error` while the
//! cause — `54023: cannot pass more than 100 arguments to a function` — is visible only in
//! the database's own log, which on a managed database they may not be able to read.

use std::fmt;

use crate::core::render_error_chain;

/// A `tokio_postgres::Error` whose `Display` carries what the server said.
///
/// Inserted as `.map_err(PgError::from)` ahead of the closure that builds the crate's
/// error, so the message text at each site is unchanged and only `{error}` gains content.
#[derive(Debug)]
pub(crate) struct PgError(tokio_postgres::Error);

impl From<tokio_postgres::Error> for PgError {
    fn from(error: tokio_postgres::Error) -> Self {
        Self(error)
    }
}

impl fmt::Display for PgError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Some(db) = self.0.as_db_error() else {
            // Not a server error — a closed connection, an I/O failure, a TLS handshake.
            // Its cause is behind `source()` too, so render the chain.
            return f.write_str(&render_error_chain(&self.0));
        };
        write!(f, "{}: {}", db.code().code(), db.message())?;
        if let Some(detail) = db.detail() {
            write!(f, " (detail: {detail})")?;
        }
        if let Some(hint) = db.hint() {
            write!(f, " (hint: {hint})")?;
        }
        Ok(())
    }
}
