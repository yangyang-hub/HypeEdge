//! Parity check: load the real `configs/dev.yaml` through the Rust loader and
//! verify the deserialized settings match the values the Python backend reads
//! from the same file. This is the strongest config-parity signal available
//! without running Python.

use std::path::PathBuf;

use hypeedge_config::settings::AppSettings;

fn repo_configs_dir() -> PathBuf {
    // CARGO_MANIFEST_DIR = <repo>/crates/config
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("configs")
}

#[test]
fn dev_yaml_deserializes_matching_python_defaults() {
    let env = "dev";
    let yaml =
        hypeedge_config::load_yaml_config(env, Some(&repo_configs_dir())).expect("dev.yaml loads");
    let mut merged = yaml;
    use serde_yaml::Value;
    hypeedge_config::loader::set_path(&mut merged, &["environment"], &Value::String(env.into()));
    let s: AppSettings =
        serde_yaml::from_value(merged).expect("dev.yaml deserializes into AppSettings");

    // Values that dev.yaml explicitly sets.
    assert_eq!(s.environment, "dev");
    assert_eq!(s.log_level, "DEBUG");

    // Feature flags: dev.yaml has the full V2 chain on.
    assert!(
        s.features.durable_ledger_v2,
        "dev enables durable_ledger_v2"
    );
    assert!(s.features.execution_v2);
    assert!(s.features.user_stream_v2);
    assert!(s.features.reconciliation_v2);
    assert!(s.features.strategy_runner_v2);
    assert!(s.features.api_v1);
    assert!(s.features.v2_trading_enabled());
    assert!(
        !s.features.funding_arb_execution_enabled,
        "dev keeps funding-arb execution off"
    );

    // Market data symbols.
    assert_eq!(s.market_data.coins, vec!["BTC", "ETH", "SOL"]);

    // Postgres default port/host shape.
    assert!(
        s.postgres.url.contains("localhost:5432"),
        "dev postgres url: {}",
        s.postgres.url
    );
}

#[test]
fn settings_validate_passes_for_real_dev_yaml() {
    let env = "dev";
    let yaml =
        hypeedge_config::load_yaml_config(env, Some(&repo_configs_dir())).expect("dev.yaml loads");
    let mut merged = yaml;
    use serde_yaml::Value;
    hypeedge_config::loader::set_path(&mut merged, &["environment"], &Value::String(env.into()));
    let s: AppSettings = serde_yaml::from_value(merged).expect("dev.yaml deserializes");
    s.validate()
        .expect("dev.yaml passes all cross-field validators");
}

/// Load `testnet.yaml` — the shape used by the live testnet deployment.
#[test]
fn testnet_yaml_deserializes() {
    let env = "testnet";
    let yaml = hypeedge_config::load_yaml_config(env, Some(&repo_configs_dir()))
        .expect("testnet.yaml loads");
    let mut merged = yaml;
    use serde_yaml::Value;
    hypeedge_config::loader::set_path(&mut merged, &["environment"], &Value::String(env.into()));
    let s: AppSettings = serde_yaml::from_value(merged).expect("testnet.yaml deserializes");
    assert_eq!(s.environment, "testnet");
    assert!(
        s.features.funding_arb_execution_enabled,
        "testnet enables funding-arb execution"
    );
    s.validate()
        .expect("testnet.yaml passes all cross-field validators");
}
