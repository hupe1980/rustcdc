use crate::{
    cli::{InitArgs, InitProfile},
    error::AppError,
};

pub async fn execute(args: InitArgs) -> Result<(), AppError> {
    if args.output.exists() && !args.force {
        return Err(AppError::Other(format!(
            "config file already exists at {}. Re-run with --force to replace it.",
            args.output.display()
        )));
    }

    if let Some(parent) = args.output.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }

    // The state directory is created here, not left to the first `run`.
    //
    // The loader validates that the parent of `admin.notification_log_file` exists, and
    // the prod template puts that file inside the state directory — so without this the
    // emitted config fails `validate-config`, which is the very next step this command
    // tells the operator to run.
    std::fs::create_dir_all(&args.state_dir)?;

    let template = render_template(&args);
    std::fs::write(&args.output, template)?;

    let (_profile, next_steps) = match &args.profile {
        InitProfile::Dev => (
            "dev",
            "1. set source credentials and local sink choice\n2. switch to --profile prod before deploying outside loopback",
        ),
        InitProfile::Prod => (
            "prod",
            "1. set source/sink credentials and trusted endpoints\n2. provision admin TLS cert/key + tokens",
        ),
    };

    println!(
        "Created secure starter config at {}\n\nNext steps:\n{}\n3. run: cdc --config-file {} validate-config\n4. run: cdc --config-file {} run",
        args.output.display(),
        next_steps,
        args.output.display(),
        args.output.display()
    );

    Ok(())
}

fn render_template(args: &InitArgs) -> String {
    match &args.profile {
        InitProfile::Dev => render_dev_template(args),
        InitProfile::Prod => render_prod_template(args),
    }
}

fn render_dev_template(args: &InitArgs) -> String {
    format!(
        r#"api_version = "v1"

[source.postgres]
host = "localhost"
port = 5432
user = "cdc_user"
password = {{ env = "CDC_SOURCE__POSTGRES__PASSWORD" }}
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
# Dev convenience: create the replication slot on first connect. Keep false in
# production — a missing slot there means data loss, and silently recreating it
# would resume from "now" and skip everything in between.
create_replication_slot_if_missing = true
conn_timeout_secs = 10
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "plaintext"

[sink]
type = "stdout"

[state]
dir = "{state_dir}"

[runtime]
max_buffer_size = 1000
max_poll_wait_ms = 100
max_event_bytes = 1048576
sink_flush_interval_events = 100
transform_error_policy = "halt"
post_commit_source_confirm_policy = "fail_fast"

[runtime.source_connection_retry]
enabled = true
max_retries = 5
initial_delay_ms = 300
max_delay_ms = 10000

[admin]
bind = "{admin_bind}"
probe_auth_mode = "allow_unauthenticated_loopback"

[observability]
service_name = "cdc-server-dev"
"#,
        state_dir = args.state_dir.display(),
        admin_bind = args.admin_bind
    )
}

fn render_prod_template(args: &InitArgs) -> String {
    format!(
        r#"api_version = "v1"

[source.postgres]
host = "db.internal.example"
port = 5432
user = "cdc_user"
password = {{ env = "CDC_SOURCE__POSTGRES__PASSWORD" }}
database = "mydb"
replication_slot_name = "cdc_slot"
publication_name = "cdc_pub"
# Production posture: the slot is provisioned out of band. A slot that vanishes
# mid-life is a data-loss event; recreating it silently would skip every change
# since the last confirmed LSN, which looks exactly like healthy operation.
create_replication_slot_if_missing = false
# PostgreSQL 17+: create failover-enabled slots (synchronized to standbys) so
# capture survives primary promotion. Requires cluster-side sync configuration;
# see the rustcdc PostgresSourceConfig::failover_slot docs.
failover_slot = false
conn_timeout_secs = 10
# `START_REPLICATION ... LOGICAL` over the streaming replication protocol — the
# server pushes WAL as it is written. Needs the REPLICATION role attribute and a
# direct connection. Switch to "sql_peek" only if the environment cannot grant
# either; it re-decodes from the slot's restart_lsn on every poll.
wal_transport = "streaming_replication"
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

# mode = "tls" now *requires* TLS: a server with `ssl = off` fails the connection
# rather than silently downgrading it to plaintext.
[source.postgres.transport]
mode = "tls"

# Declared with no tables: nothing is backfilled at startup, but the
# incremental-snapshot driver is installed, which is what makes
# `POST /signals` with action_type = "execute_snapshot" work against a running
# pipeline. Remove this section if on-demand snapshots are not wanted.
[incremental_snapshot]
tables = []

[sink]
type = "stdout"

[state]
dir = "{state_dir}"

[runtime]
max_buffer_size = 1000
max_poll_wait_ms = 100
max_event_bytes = 1048576
sink_flush_interval_events = 100
transform_error_policy = "halt"
post_commit_source_confirm_policy = "fail_fast"

[runtime.source_connection_retry]
enabled = true
max_retries = 5
initial_delay_ms = 300
max_delay_ms = 10000

[admin]
bind = "{admin_bind}"
read_token_env = "CDC_ADMIN_READ_TOKEN"
write_token_env = "CDC_ADMIN_WRITE_TOKEN"
# Required whenever write-capable signalling is enabled, and the template used to
# omit it — so `init --profile prod` produced a config that failed the
# `validate-config` step this command prints as its own next step. A write signal
# whose outcome is only visible in the admin process's memory is not an audit trail;
# swap in `[admin.notification_kafka]` if the durable channel should be a topic.
notification_log_file = "{state_dir}/admin-notifications.jsonl"

[admin.tls]
cert_file = "/etc/cdc/admin/tls.crt"
key_file = "/etc/cdc/admin/tls.key"
require_client_cert = true
client_ca_file = "/etc/cdc/admin/clients-ca.crt"

[observability]
service_name = "cdc-server"
"#,
        state_dir = args.state_dir.display(),
        admin_bind = args.admin_bind
    )
}

#[cfg(test)]
mod tests {
    use super::render_template;
    use crate::cli::{InitArgs, InitProfile};
    use std::path::PathBuf;

    fn args_for(profile: InitProfile) -> InitArgs {
        InitArgs {
            output: PathBuf::from("cdc.toml"),
            force: false,
            profile,
            state_dir: PathBuf::from("/var/lib/cdc/state"),
            admin_bind: "127.0.0.1:8080".to_string(),
        }
    }

    #[test]
    fn dev_template_validates_local_bootstrap_expectations() {
        let rendered = render_template(&args_for(InitProfile::Dev));
        assert!(rendered.contains("host = \"localhost\""));
        assert!(rendered.contains("mode = \"plaintext\""));
        assert!(!rendered.contains("[admin.tls]"));
        assert!(!rendered.contains("read_token_env = \"CDC_ADMIN_READ_TOKEN\""));
    }

    #[test]
    fn prod_template_enables_tls_and_admin_auth() {
        let rendered = render_template(&args_for(InitProfile::Prod));
        assert!(rendered.contains("mode = \"tls\""));
        assert!(rendered.contains("read_token_env = \"CDC_ADMIN_READ_TOKEN\""));
        assert!(rendered.contains("[admin.tls]"));
    }

    /// Both starter configs must survive the real loader.
    ///
    /// These are the first configuration an operator ever runs, and the printed next step
    /// is `validate-config` — so a template that does not load turns the very first
    /// command into an error message. Nothing checked it: the assertions above are all
    /// substring matches, which pass equally well against a file the loader rejects. It
    /// found one: the prod template enabled write-capable admin signalling without the
    /// notification channel the loader requires alongside it.
    ///
    /// The prod template's admin TLS paths are rewritten to files that exist, because
    /// provisioning that certificate is step 2 of the printed next steps — it is the
    /// operator's to supply, and everything *else* in the template has to be right.
    #[test]
    fn both_templates_load_through_the_real_loader() {
        // Only referenced by the prod template, but set for both so the test does not
        // depend on which one names it.
        std::env::set_var("CDC_SOURCE__POSTGRES__PASSWORD", "template-test-secret");

        for profile in [InitProfile::Dev, InitProfile::Prod] {
            let dir = tempfile::tempdir().expect("tempdir");
            let mut args = args_for(profile);
            args.state_dir = dir.path().join("state");
            // `execute` creates this; the render-only path here has to as well.
            std::fs::create_dir_all(&args.state_dir).expect("state dir");

            let path = dir.path().join("cdc.toml");
            std::fs::write(
                &path,
                with_provisioned_admin_tls(&render_template(&args), dir.path()),
            )
            .expect("write template");

            crate::config::load(&path)
                .unwrap_or_else(|e| panic!("the starter template must load: {e}"));
        }
    }

    /// The prod template ships on-demand snapshots switched on.
    ///
    /// A declared-but-empty `[incremental_snapshot]` backfills nothing at startup and is
    /// the only way `execute_snapshot` is reachable later — an operator who needs to
    /// backfill a newly published table should not have to restart the pipeline to earn
    /// the capability.
    #[test]
    fn the_prod_template_enables_on_demand_snapshots() {
        std::env::set_var("CDC_SOURCE__POSTGRES__PASSWORD", "template-test-secret");
        let dir = tempfile::tempdir().expect("tempdir");
        let mut args = args_for(InitProfile::Prod);
        args.state_dir = dir.path().join("state");
        std::fs::create_dir_all(&args.state_dir).expect("state dir");

        let path = dir.path().join("cdc.toml");
        std::fs::write(
            &path,
            with_provisioned_admin_tls(&render_template(&args), dir.path()),
        )
        .expect("write template");

        let config = crate::config::load(&path).expect("prod template loads");
        let incremental = config
            .incremental_snapshot
            .expect("the section must be declared so execute_snapshot is reachable");
        assert!(
            !incremental.backfills_at_startup(),
            "the template must not silently backfill tables nobody listed"
        );
    }

    /// Stand in for the operator's step 2: point `[admin.tls]` at files that exist.
    ///
    /// The loader only checks that they are files at load time — the certificate itself is
    /// parsed when the listener starts — so empty placeholders are enough to get past the
    /// one prerequisite the template cannot satisfy on its own.
    fn with_provisioned_admin_tls(rendered: &str, dir: &std::path::Path) -> String {
        let mut out = rendered.to_string();
        for name in ["tls.crt", "tls.key", "clients-ca.crt"] {
            let path = dir.join(name);
            std::fs::write(&path, b"").expect("placeholder admin TLS file");
            out = out.replace(
                &format!("/etc/cdc/admin/{name}"),
                &path.display().to_string(),
            );
        }
        out
    }
}
