//! Postgres connection pool, migrations, and shared SQL helpers.

use std::time::Duration;

use sqlx::PgPool as Pool;
use sqlx::postgres::PgPoolOptions;

/// A pooled Postgres connection with the sqlx migrations applied.
pub struct Postgres {
    pub pool: Pool,
}

impl Postgres {
    /// Connect, apply migrations, and return a ready pool.
    pub async fn connect(database_url: &str, pool_size: u32) -> Result<Postgres, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(pool_size)
            .acquire_timeout(Duration::from_secs(10))
            .connect(database_url)
            .await?;
        // sqlx::migrate! runs `crates/storage/migrations/*.sql` in order.
        sqlx::migrate!("./migrations").run(&pool).await?;
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
