use cdc_rs::{
    checkpoint::{Checkpoint, PostgresCheckpoint, PostgresOffset},
    core::Result,
};
use sqlx::postgres::PgPoolOptions;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

#[tokio::test]
async fn postgres_checkpoint_save_load_and_upsert_cycle() -> Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping postgres checkpoint integration test (set CDC_RS_RUN_DOCKER_TESTS=1)");
        return Ok(());
    }

    let container = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "cdc")
        .start()
        .await
        .map_err(|error| cdc_rs::Error::CheckpointError(error.to_string()))?;

    let host = container
        .get_host()
        .await
        .map_err(|error| cdc_rs::Error::CheckpointError(error.to_string()))?;
    let port = container
        .get_host_port_ipv4(5432.tcp())
        .await
        .map_err(|error| cdc_rs::Error::CheckpointError(error.to_string()))?;

    let database_url = format!("postgres://postgres:postgres@{host}:{port}/cdc");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .map_err(|error| cdc_rs::Error::CheckpointError(error.to_string()))?;

    let mut checkpoint = PostgresCheckpoint::new(pool.clone(), "cdc_checkpoints", "postgres")?;

    let offset = PostgresOffset {
        lsn: 128,
        slot_name: "slot-a".into(),
    };
    checkpoint.save(&offset, 11).await?;

    let loaded = checkpoint.load().await?.expect("checkpoint exists");
    assert_eq!(loaded.source_type(), "postgres");
    assert_eq!(checkpoint.get_committed_count().await?, 11);

    let mut joins = Vec::new();
    for count in 0..8_u64 {
        let pool = pool.clone();
        joins.push(tokio::spawn(async move {
            let mut writer = PostgresCheckpoint::new(pool, "cdc_checkpoints", "postgres").unwrap();
            let offset = PostgresOffset {
                lsn: 200 + count,
                slot_name: "slot-a".into(),
            };
            writer.save(&offset, count).await
        }));
    }

    for join in joins {
        join.await
            .map_err(|error| cdc_rs::Error::CheckpointError(error.to_string()))??;
    }

    checkpoint
        .save(
            &PostgresOffset {
                lsn: 4096,
                slot_name: "slot-a".into(),
            },
            777,
        )
        .await?;
    assert_eq!(checkpoint.get_committed_count().await?, 777);

    let mut namespaced = PostgresCheckpoint::new(pool, "cdc_checkpoints_alt", "postgres_alt")?;
    namespaced
        .save(
            &PostgresOffset {
                lsn: 512,
                slot_name: "slot-b".into(),
            },
            9,
        )
        .await?;
    assert_eq!(namespaced.get_committed_count().await?, 9);

    Ok(())
}
