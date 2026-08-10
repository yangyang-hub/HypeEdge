//! Postgres connection pool, migrations, and shared SQL helpers.

use std::time::Duration;

use sqlx::PgPool as Pool;
use sqlx::postgres::PgPoolOptions;

/// A pooled Postgres connection with the sqlx migrations applied.
pub struct Postgres {
    pub pool: Pool,
}

/// Whether a migration error is "object already exists" (schema pre-existing).
fn is_already_exists(e: &sqlx::migrate::MigrateError) -> bool {
    let msg = e.to_string().to_lowercase();
    msg.contains("already exists")
        || msg.contains("duplicate key")
        || msg.contains("already in use")
}

impl Postgres {
    /// Connect, apply migrations, and return a ready pool.
    pub async fn connect(database_url: &str, pool_size: u32) -> Result<Postgres, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(pool_size)
            .acquire_timeout(Duration::from_secs(10))
            .connect(database_url)
            .await?;
        // sqlx::migrate! runs `crates/storage/migrations/*.sql` in order. A
        // pre-existing schema (e.g. a database created before sqlx took over)
        // has the tables but no `_sqlx_migrations` rows; the first migration
        // then fails with "already exists". Treat that as "schema already
        // applied" rather than a hard failure so the app boots against an
        // existing database.
        match sqlx::migrate!("./migrations").run(&pool).await {
            Ok(_) => {}
            Err(e) if is_already_exists(&e) => {
                tracing::warn!(error = %e, "migration_already_applied_using_existing_schema");
            }
            Err(e) => return Err(e.into()),
        }
        Ok(Postgres { pool })
    }

    /// Connect without running migrations (for tests that manage schema).
    pub async fn connect_unmigrated(
        database_url: &str,
        pool_size: u32,
    ) -> Result<Postgres, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(pool_size)
            .acquire_timeout(Duration::from_secs(10))
            .connect(database_url)
            .await?;
        Ok(Postgres { pool })
    }
}
