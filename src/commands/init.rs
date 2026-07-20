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
stream_poll_interval_ms = 100
max_events_per_poll = 1000
table_include_list = []
table_exclude_list = []

[source.postgres.transport]
mode = "tls"

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
}
