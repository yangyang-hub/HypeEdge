//! Regression (B11): `load_settings(None)` must resolve the environment from
//! the process `HYPE_ENV` / `.env` layer instead of defaulting to `dev`.

use std::path::PathBuf;
use std::sync::Mutex;

use hypeedge_config::loader::load_settings;

/// Serialize env mutation across tests in this binary (env vars are global).
static ENV_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn load_settings_none_honors_process_hype_env() {
    let _guard = ENV_LOCK.lock().unwrap();
    // Point the loader at the repo `configs/` dir so `testnet.yaml` resolves.
    let cfg_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("configs");
    unsafe {
        std::env::set_var("HYPE_CONFIGS_DIR", &cfg_dir);
        std::env::set_var("HYPE_ENV", "testnet");
        std::env::remove_var("HYPE_ENVIRONMENT");
    }

    let settings =
        load_settings(None).expect("load_settings(None) should resolve testnet from HYPE_ENV");
    assert_eq!(settings.environment, "testnet");

    unsafe {
        std::env::remove_var("HYPE_ENV");
        std::env::remove_var("HYPE_CONFIGS_DIR");
    }
}

#[test]
fn load_settings_some_overrides_env() {
    let _guard = ENV_LOCK.lock().unwrap();
    let cfg_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("configs");
    unsafe {
        std::env::set_var("HYPE_CONFIGS_DIR", &cfg_dir);
        std::env::set_var("HYPE_ENV", "testnet");
        std::env::remove_var("HYPE_ENVIRONMENT");
    }

    // An explicit argument wins over the environment.
    let settings = load_settings(Some("dev")).expect("explicit dev loads");
    assert_eq!(settings.environment, "dev");

    unsafe {
        std::env::remove_var("HYPE_ENV");
        std::env::remove_var("HYPE_CONFIGS_DIR");
    }
}
