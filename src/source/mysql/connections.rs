//! How the MySQL connector obtains connections, and why that is a choice.
//!
//! `mysql_async::Opts` is immutable and the driver exposes no per-connection credential
//! hook, so a `Pool` authenticates **every** connection it ever opens with the password
//! resolved at the moment the pool was built. For a static password that is correct and
//! efficient. For a short-lived one it is a time bomb: an AWS RDS IAM token is valid for
//! fifteen minutes, and the pool goes on opening connections long after that — following a
//! server-side `wait_timeout`, a transient error, or a demand spike — each authenticating
//! with an expired token.
//!
//! The failure is invisible at startup, because the token is only checked when a connection
//! is *established*: existing connections keep working, and the first sign is an
//! "access denied" some time later that reads like an intermittent credentials problem.
//!
//! [`MysqlConnections`] makes the trade explicit. Pool by default; when the operator
//! declares the credential short-lived with
//! [`DatabaseAuthMode::AwsIamToken`](crate::source::DatabaseAuthMode::AwsIamToken), open a
//! fresh connection per request with the secret re-resolved each time.
//!
//! # Why giving up the pool is affordable here
//!
//! Pooling pays for itself under many small request/response round trips. A CDC connector's
//! shape is the opposite: one long-lived binlog connection, a handful of metadata queries at
//! startup, one query per snapshot chunk (ten thousand rows by default), and one heartbeat
//! per interval. The handshake cost lands on a request rate low enough that it does not
//! matter — and it only lands at all on the path an operator explicitly opted into.

use std::sync::Arc;

use mysql_async::{Conn, Opts, Pool as MySqlPool};

use crate::core::{Error, Result};

use super::MysqlSourceConfig;

/// A source of MySQL connections: pooled, or freshly authenticated per connection.
///
/// Cloneable and cheap to clone, so it drops into the places a `Pool` used to live.
#[derive(Clone)]
pub(super) struct MysqlConnections {
    inner: Arc<Mode>,
}

enum Mode {
    /// Credentials fixed when the pool was built. The default.
    Pooled(MySqlPool),
    /// Options — and therefore the secret — rebuilt for every connection.
    PerConnection(Box<MysqlSourceConfig>),
}

impl std::fmt::Debug for MysqlConnections {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MysqlConnections")
            .field("mode", &self.mode_name())
            .finish()
    }
}

impl MysqlConnections {
    /// Pool connections, reusing the credential resolved when `pool` was built.
    pub(super) fn pooled(pool: MySqlPool) -> Self {
        Self {
            inner: Arc::new(Mode::Pooled(pool)),
        }
    }

    /// Open a freshly authenticated connection per request.
    pub(super) fn per_connection(config: MysqlSourceConfig) -> Self {
        Self {
            inner: Arc::new(Mode::PerConnection(Box::new(config))),
        }
    }

    /// Choose a mode from the configuration.
    ///
    /// `AwsIamToken` is the operator declaring the credential short-lived, which is exactly
    /// the condition a pool cannot serve. A *deferred* secret alone is not enough to switch:
    /// a static password fetched from Vault is deferred and never expires, and silently
    /// dropping pooling for it would trade throughput for nothing.
    pub(super) fn for_config(config: &MysqlSourceConfig, pool: MySqlPool) -> Self {
        if matches!(
            config.auth_mode,
            crate::source::DatabaseAuthMode::AwsIamToken
        ) {
            // The pool built for the connectivity probe is discarded rather than kept: its
            // credential is already the one that will expire, and holding it would leave a
            // second, stale path to the server.
            Self::per_connection(config.clone())
        } else {
            Self::pooled(pool)
        }
    }

    /// `"pooled"` or `"per-connection"`, for logs and diagnostics.
    pub(super) fn mode_name(&self) -> &'static str {
        match self.inner.as_ref() {
            Mode::Pooled(_) => "pooled",
            Mode::PerConnection(_) => "per-connection",
        }
    }

    /// Whether every connection re-resolves the credential.
    pub(super) fn refreshes_credentials(&self) -> bool {
        matches!(self.inner.as_ref(), Mode::PerConnection(_))
    }

    /// Options for the next connection, re-resolving the secret when that is the mode.
    ///
    /// Separate from [`Self::get_conn`] so the part that decides *what credential is used*
    /// can be tested without a server — which is the whole point of the distinction.
    pub(super) fn opts_for_next_connection(&self) -> Result<Option<Opts>> {
        match self.inner.as_ref() {
            Mode::Pooled(_) => Ok(None),
            Mode::PerConnection(config) => config.build_pool_opts().map(Some),
        }
    }

    /// Acquire a connection.
    ///
    /// Routed through [`Self::opts_for_next_connection`] rather than repeating the decision,
    /// so the credential path the tests exercise is the one production runs.
    pub(super) async fn get_conn(&self) -> Result<Conn> {
        if let Some(opts) = self.opts_for_next_connection()? {
            return Conn::new(opts).await.map_err(|error| {
                Error::SourceError(format!(
                    "failed to open a mysql connection: {error}. Credentials are re-resolved \
                     for every connection because auth_mode = AwsIamToken, so an \
                     authentication failure here means the token callback returned something \
                     the server rejected — not a stale cached one."
                ))
            });
        }

        let Mode::Pooled(pool) = self.inner.as_ref() else {
            return Err(Error::StateError(
                "mysql connection mode is inconsistent: per-connection mode yielded no \
                 options to connect with"
                    .into(),
            ));
        };
        pool.get_conn().await.map_err(|error| {
            Error::SourceError(format!("failed to acquire mysql connection: {error}"))
        })
    }

    /// Close pooled connections. A no-op in per-connection mode, which holds none.
    pub(super) async fn disconnect(self) -> Result<()> {
        match Arc::try_unwrap(self.inner) {
            Ok(Mode::Pooled(pool)) => pool.disconnect().await.map_err(|error| {
                Error::SourceError(format!("failed to close the mysql pool: {error}"))
            }),
            Ok(Mode::PerConnection(_)) => Ok(()),
            // Another clone is still live — the pool closes when the last one drops, which
            // is `mysql_async`'s own contract for a `Pool`.
            Err(_) => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use super::*;
    use crate::core::SecretString;
    use crate::core::TransportConfig;
    use crate::source::DatabaseAuthMode;

    fn config(auth_mode: DatabaseAuthMode, password: SecretString) -> MysqlSourceConfig {
        MysqlSourceConfig {
            host: "localhost".into(),
            port: 3306,
            user: "cdc".into(),
            password,
            database: "app".into(),
            auth_mode,
            transport: TransportConfig::tls(),
            ..Default::default()
        }
    }

    #[test]
    fn an_iam_token_config_opens_a_fresh_connection_per_request() {
        // The defect this exists for: a pool resolves the password once and then
        // authenticates every later connection with it. An RDS IAM token is valid for
        // fifteen minutes, so "once" is the wrong number of times.
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&calls);
        let password = SecretString::from_callback("rds-iam", move || {
            counter.fetch_add(1, Ordering::Relaxed);
            Ok("token".into())
        });

        let connections =
            MysqlConnections::per_connection(config(DatabaseAuthMode::AwsIamToken, password));
        assert!(connections.refreshes_credentials());
        assert_eq!(connections.mode_name(), "per-connection");

        for _ in 0..3 {
            connections
                .opts_for_next_connection()
                .expect("options build")
                .expect("per-connection mode always builds options");
        }
        assert_eq!(
            calls.load(Ordering::Relaxed),
            3,
            "every connection must mint its own token, not reuse the first"
        );
    }

    #[test]
    fn a_static_password_keeps_the_pool() {
        // A deferred secret is not automatically short-lived — a fixed password in Vault is
        // deferred and never expires. Dropping pooling for it would cost throughput and buy
        // nothing, so the switch keys on the operator's explicit declaration instead.
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&calls);
        let password = SecretString::from_callback("vault", move || {
            counter.fetch_add(1, Ordering::Relaxed);
            Ok("static".into())
        });
        let config = config(DatabaseAuthMode::Password, password);

        let opts = config.build_pool_opts().expect("options build");
        let connections = MysqlConnections::for_config(&config, MySqlPool::new(opts));

        assert!(!connections.refreshes_credentials());
        assert_eq!(connections.mode_name(), "pooled");
        assert!(
            connections
                .opts_for_next_connection()
                .expect("no error")
                .is_none(),
            "a pooled source does not rebuild options; the pool owns them"
        );
    }

    #[test]
    fn the_mode_is_chosen_by_auth_mode_not_by_secret_shape() {
        let inline = config(DatabaseAuthMode::AwsIamToken, SecretString::new("token"));
        let opts = inline.build_pool_opts().expect("options build");
        assert!(
            MysqlConnections::for_config(&inline, MySqlPool::new(opts)).refreshes_credentials(),
            "AwsIamToken means short-lived whatever shape the secret has today"
        );
    }
}
