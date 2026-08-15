//! Layered configuration loader mirroring `src/hypeedge/config/loader.py`.
//!
//! Precedence (highest wins), except exchange API/WS URLs which are always
//! forced from the selected environment:
//!   1. Process environment variables (`HYPE_*`)
//!   2. `.env` file
//!   3. `configs/{env}.yaml`
//!   4. Defaults in the settings structs
//!
//! Mainnet fails closed: secrets must come from the process environment and
//! the Postgres URL must require TLS with a non-default password.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde_yaml::Value;
use url::Url;

use crate::settings::{AppSettings, ConfigError};

const MAINNET_API_URL: &str = "https://api.hyperliquid.xyz";
const MAINNET_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
const TESTNET_API_URL: &str = "https://api.hyperliquid-testnet.xyz";
const TESTNET_WS_URL: &str = "wss://api.hyperliquid-testnet.xyz/ws";

const MAINNET_REQUIRED_ENV_VARS: &[&str] = &[
    "HYPE_EXCHANGE__ACCOUNT_ADDRESS",
    "HYPE_EXCHANGE__AGENT_PRIVATE_KEY",
    "HYPE_POSTGRES__URL",
];
const MAINNET_API_TOKEN_ENV_VARS: &[&str] = &[
    "HYPE_API__AUTH_TOKEN",
    "HYPE_API__VIEWER_TOKEN",
    "HYPE_API__OPERATOR_TOKEN",
    "HYPE_API__ADMIN_TOKEN",
];
const WEAK_POSTGRES_PASSWORDS: &[&str] = &[
    "",
    "changeme",
    "change-me",
    "hypeedge",
    "password",
    "postgres",
];

/// Default location of the environment YAML files, resolved relative to the
/// current working directory. Override with `HYPE_CONFIGS_DIR`.
pub fn default_configs_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("HYPE_CONFIGS_DIR") {
        return PathBuf::from(dir);
    }
    PathBuf::from("configs")
}

/// Select the environment before the full settings are loaded:
/// explicit argument > process `HYPE_ENV`/`HYPE_ENVIRONMENT` > `.env` > "dev".
pub fn select_environment(explicit: Option<&str>) -> String {
    if let Some(e) = explicit {
        return e.to_string();
    }
    if let Some(e) = process_env_var("HYPE_ENV") {
        return e;
    }
    if let Some(e) = process_env_var("HYPE_ENVIRONMENT") {
        return e;
    }
    let dotenv = load_dotenv_map();
    if let Some(e) = dotenv.get("HYPE_ENV").filter(|s| !s.is_empty()) {
        return e.clone();
    }
    if let Some(e) = dotenv.get("HYPE_ENVIRONMENT").filter(|s| !s.is_empty()) {
        return e.clone();
    }
    "dev".to_string()
}

/// Load the `configs/{env}.yaml` file as a raw mapping, or `{}` if missing.
pub fn load_yaml_config(
    environment: &str,
    configs_dir: Option<&Path>,
) -> Result<Value, ConfigError> {
    let default_dir = default_configs_dir();
    let dir = configs_dir.unwrap_or(&default_dir);
    let path = dir.join(format!("{environment}.yaml"));
    let content = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Value::Mapping(Default::default()));
        }
        Err(e) => return Err(ConfigError::Io(e.to_string())),
    };
    let value: Value =
        serde_yaml::from_str(&content).map_err(|e| ConfigError::Io(e.to_string()))?;
    Ok(value)
}

/// Load settings from YAML + environment variables.
pub fn load_settings(explicit_environment: Option<&str>) -> Result<AppSettings, ConfigError> {
    let selected = select_environment(explicit_environment);
    if !matches!(selected.as_str(), "dev" | "testnet" | "mainnet") {
        return Err(ConfigError::UnsupportedEnvironment(format!(
            "unsupported HYPE_ENV={selected:?}; expected one of dev, testnet, mainnet"
        )));
    }

    let yaml = load_yaml_config(&selected, None)?;
    // Base the top-level `environment` on the selection, then overlay env vars.
    let mut merged = yaml;
    set_path(
        &mut merged,
        &["environment"],
        &Value::String(selected.clone()),
    );

    // Process env over .env over YAML.
    let dotenv = load_dotenv_map();
    let mut all_env: HashMap<String, String> = dotenv;
    for (k, v) in std::env::vars() {
        all_env.insert(k, v);
    }
    apply_env_overlay(&mut merged, &all_env);

    let settings: AppSettings =
        serde_yaml::from_value(merged).map_err(|e| ConfigError::Io(e.to_string()))?;

    if settings.environment != selected {
        return Err(ConfigError::EnvironmentMismatch(
            "HYPE_ENV and HYPE_ENVIRONMENT must not select different environments".into(),
        ));
    }

    let settings = apply_exchange_urls(settings)?;
    settings.validate()?;
    if settings.is_mainnet() {
        validate_mainnet_environment(&settings)?;
    }
    Ok(settings)
}

/// Force the official Hyperliquid endpoints from the selected environment.
fn apply_exchange_urls(mut settings: AppSettings) -> Result<AppSettings, ConfigError> {
    let (api, ws) = exchange_urls_for_environment(&settings.environment)?;
    settings.exchange.api_url = api.to_string();
    settings.exchange.ws_url = ws.to_string();
    Ok(settings)
}

fn exchange_urls_for_environment(
    environment: &str,
) -> Result<(&'static str, &'static str), ConfigError> {
    match environment {
        "mainnet" => Ok((MAINNET_API_URL, MAINNET_WS_URL)),
        "dev" | "testnet" => Ok((TESTNET_API_URL, TESTNET_WS_URL)),
        other => Err(ConfigError::UnsupportedEnvironment(format!(
            "unsupported HYPE_ENV={other:?}; expected one of dev, testnet, mainnet"
        ))),
    }
}

/// Fail closed unless mainnet secrets came from explicit environment variables.
fn validate_mainnet_environment(settings: &AppSettings) -> Result<(), ConfigError> {
    let mut missing = Vec::new();
    for name in MAINNET_REQUIRED_ENV_VARS {
        if process_env_var(name).is_none() {
            missing.push(name.to_string());
        }
    }
    let has_any_token = MAINNET_API_TOKEN_ENV_VARS
        .iter()
        .any(|name| process_env_var(name).is_some());
    if !has_any_token {
        missing.push("one of HYPE_API__AUTH/VIEWER/OPERATOR/ADMIN_TOKEN".to_string());
    }
    if !missing.is_empty() {
        return Err(ConfigError::MainnetSecretsMissing(missing.join(", ")));
    }

    let admin_tokens = [
        settings.api.auth_token.as_str(),
        settings.api.admin_token.as_str(),
    ];
    if !admin_tokens.iter().any(|t| t.len() >= 32) {
        return Err(ConfigError::MainnetApiTokenInvalid(
            "an admin HYPE_API token must contain at least 32 characters on mainnet".into(),
        ));
    }

    let parsed = Url::parse(&settings.postgres.url)
        .map_err(|_| ConfigError::MainnetPostgresInvalid("not a valid URL".into()))?;
    let scheme_ok = parsed.scheme().starts_with("postgresql");
    let has_host = parsed.host_str().is_some();
    let password = parsed.password().unwrap_or("").to_lowercase();
    let password_ok = !WEAK_POSTGRES_PASSWORDS.contains(&password.as_str());
    if !scheme_ok || !has_host || !password_ok {
        return Err(ConfigError::MainnetPostgresInvalid(
            "HYPE_POSTGRES__URL must be a valid mainnet URL with a non-default password".into(),
        ));
    }
    let mut ssl = parsed
        .query_pairs()
        .filter(|(k, _)| *k == "ssl" || *k == "sslmode")
        .map(|(_, v)| v.to_string())
        .collect::<Vec<_>>();
    ssl.sort();
    let ssl_mode = ssl.last().map(String::as_str).unwrap_or("");
    if !matches!(ssl_mode, "require" | "verify-ca" | "verify-full") {
        return Err(ConfigError::MainnetPostgresInvalid(
            "mainnet HYPE_POSTGRES__URL must require TLS with ssl=require, verify-ca, or verify-full".into(),
        ));
    }

    // H-CF1: real money requires the kill switch armed. A mainnet deployment
    // with `kill_switch_enabled=false` cannot panic-stop trading, so it is
    // refused at load time.
    if !settings.risk.kill_switch_enabled {
        return Err(ConfigError::MainnetKillSwitchDisabled);
    }
    Ok(())
}

/// Read a process environment variable, treating empty as unset.
fn process_env_var(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(v) if !v.trim().is_empty() => Some(v.trim().to_string()),
        _ => None,
    }
}

/// Load `.env` from the current directory into a map (dotenvy semantics).
fn load_dotenv_map() -> HashMap<String, String> {
    let mut map = HashMap::new();
    if let Ok(content) = std::fs::read_to_string(".env") {
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (k, v) = match line.split_once('=') {
                Some(kv) => kv,
                None => continue,
            };
            let k = k.trim().trim_start_matches("export ").trim();
            let v = v.trim().trim_matches('"').trim_matches('\'');
            if !k.is_empty() {
                map.insert(k.to_string(), v.to_string());
            }
        }
    }
    map
}

/// Apply `HYPE_*` env vars onto a nested YAML mapping. Nested sections use the
/// `__` delimiter (e.g. `HYPE_EXCHANGE__API_URL` -> `exchange.api_url`);
/// top-level fields use `HYPE_FIELD`. `HYPE_ENV` is an alias only and is
/// ignored here.
///
/// Only variables whose path resolves to a real settings field are injected
/// (M-CF1): unrelated `HYPE_*` variables in the operator's environment (loader
/// controls such as `HYPE_CONFIGS_DIR`, CI secrets, tooling vars) must not be
/// smuggled into the YAML root, where `deny_unknown_fields` would otherwise
/// fail the load (or worse, silently shadow a typo'd setting).
fn apply_env_overlay(root: &mut Value, env: &HashMap<String, String>) {
    let known = known_overlay_paths();
    for (key, value) in env {
        let Some(rest) = key.strip_prefix("HYPE_") else {
            continue;
        };
        if rest.is_empty() || rest == "ENV" {
            continue;
        }
        let path: Vec<&str> = rest.split("__").collect();
        let canonical = path
            .iter()
            .map(|s| s.to_lowercase())
            .collect::<Vec<_>>()
            .join("__");
        if !known.contains(&canonical) {
            continue; // not a settings field: never inject into the YAML root
        }
        set_path(root, &path, &parse_env_scalar(value));
    }
}

/// The set of canonical `section__field` paths that exist on [`AppSettings`],
/// derived once from its serialized defaults. `apply_env_overlay` uses this
/// as the allow-list for `HYPE_*` injection.
fn known_overlay_paths() -> &'static std::collections::HashSet<String> {
    use std::sync::OnceLock;
    static KNOWN: OnceLock<std::collections::HashSet<String>> = OnceLock::new();
    KNOWN.get_or_init(|| {
        let defaults = serde_yaml::to_value(AppSettings::default())
            .expect("AppSettings defaults must serialize");
        let mut paths = std::collections::HashSet::new();
        let mut prefix = Vec::new();
        collect_leaf_paths(&defaults, &mut prefix, &mut paths);
        paths
    })
}

/// Walk a YAML mapping, recording every leaf path as a `__`-joined lowercase
/// string (e.g. `risk__max_leverage`).
fn collect_leaf_paths(
    value: &Value,
    prefix: &mut Vec<String>,
    out: &mut std::collections::HashSet<String>,
) {
    if let Value::Mapping(map) = value {
        for (k, v) in map {
            let key = k.as_str().unwrap_or_default().to_lowercase();
            prefix.push(key);
            if matches!(v, Value::Mapping(_)) {
                collect_leaf_paths(v, prefix, out);
            } else {
                out.insert(prefix.join("__"));
            }
            prefix.pop();
        }
    }
}

/// Set a value at a nested path, creating intermediate mappings. Public for
/// the parity test harness; callers should prefer [`load_settings`].
pub fn set_path(root: &mut Value, path: &[&str], value: &Value) {
    let mut current = root;
    for (i, segment) in path.iter().enumerate() {
        let key = segment.to_lowercase();
        if i == path.len() - 1 {
            if let Value::Mapping(map) = current {
                map.insert(Value::String(key), value.clone());
            }
        } else if let Value::Mapping(map) = current {
            let entry = map
                .entry(Value::String(key))
                .or_insert_with(|| Value::Mapping(Default::default()));
            current = entry;
        }
    }
}

/// Parse an env-var scalar: JSON for lists/maps, otherwise number/bool/string.
fn parse_env_scalar(value: &str) -> Value {
    let trimmed = value.trim();
    if (trimmed.starts_with('[') || trimmed.starts_with('{'))
        && let Ok(json) = serde_json::from_str::<serde_json::Value>(trimmed)
    {
        return json_to_yaml(&json);
    }
    if let Ok(i) = trimmed.parse::<i64>() {
        return Value::Number(i.into());
    }
    if let Ok(f) = trimmed.parse::<f64>() {
        return Value::Number(serde_yaml::Number::from(f));
    }
    match trimmed {
        "true" => return Value::Bool(true),
        "false" => return Value::Bool(false),
        _ => {}
    }
    Value::String(value.to_string())
}

fn json_to_yaml(json: &serde_json::Value) -> Value {
    match json {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Number(i.into())
            } else if let Some(f) = n.as_f64() {
                Value::Number(serde_yaml::Number::from(f))
            } else {
                Value::String(n.to_string())
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Array(items) => {
            Value::Sequence(items.iter().map(json_to_yaml).collect())
        }
        serde_json::Value::Object(map) => {
            let mut m = serde_yaml::Mapping::new();
            for (k, v) in map {
                m.insert(Value::String(k.clone()), json_to_yaml(v));
            }
            Value::Mapping(m)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serialize env mutation across tests in this binary (env vars are
    /// global; `mainnet_requires_kill_switch_enabled` mutates them).
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn defaults_apply_without_env_or_yaml() {
        // Point configs dir at an empty temp dir so no YAML interferes.
        let tmp = std::env::temp_dir().join("hypeedge_config_empty");
        let _ = std::fs::create_dir_all(&tmp);
        let yaml = load_yaml_config("dev", Some(&tmp)).unwrap();
        assert!(yaml.as_mapping().unwrap().is_empty());
        let mut merged = yaml;
        set_path(&mut merged, &["environment"], &Value::String("dev".into()));
        let s: AppSettings = serde_yaml::from_value(merged).unwrap();
        assert_eq!(s.environment, "dev");
        assert_eq!(s.market_data.coins, vec!["BTC", "ETH", "SOL"]);
        assert_eq!(s.risk.max_leverage, 5);
        assert_eq!(s.api.port, 37001);
        assert!(s.exchange.api_url.contains("testnet"));
        assert!(!s.exchange.is_configured());
        assert!(!s.features.v2_trading_enabled());
        // The default Postgres URL must use the plain `postgresql://` scheme
        // (sqlx); the Python-era `postgresql+asyncpg://` dialect prefix is
        // not a sqlx scheme and must not reappear in the default.
        assert!(
            s.postgres.url.starts_with("postgresql://"),
            "default postgres url must use the sqlx scheme: {}",
            s.postgres.url
        );
        assert!(!s.postgres.url.contains("asyncpg"));
    }

    #[test]
    fn env_overlay_nested_and_list() {
        let mut root = Value::Mapping(Default::default());
        let mut env = HashMap::new();
        env.insert("HYPE_MARKET_DATA__COINS".into(), "[\"BTC\",\"ETH\"]".into());
        env.insert("HYPE_RISK__MAX_LEVERAGE".into(), "3".into());
        env.insert("HYPE_LOG_LEVEL".into(), "DEBUG".into());
        apply_env_overlay(&mut root, &env);
        let s: AppSettings = serde_yaml::from_value(root).unwrap();
        assert_eq!(s.market_data.coins, vec!["BTC", "ETH"]);
        assert_eq!(s.risk.max_leverage, 3);
        assert_eq!(s.log_level, "DEBUG");
    }

    #[test]
    fn validation_rejects_bad_thresholds() {
        let mut s = AppSettings::default();
        s.action_budget.address_cancel_only_threshold = 5000;
        s.action_budget.address_critical_threshold = 1500;
        s.action_budget.address_conserve_threshold = 3000;
        assert!(s.validate().is_err());
    }

    #[test]
    fn feature_cutover_chain() {
        let mut s = AppSettings::default();
        s.features.execution_v2 = true; // without durable_ledger_v2
        assert!(s.validate().is_err());
        let mut s2 = AppSettings::default();
        s2.features.durable_ledger_v2 = true;
        s2.features.execution_v2 = true;
        s2.features.user_stream_v2 = true;
        s2.features.reconciliation_v2 = true;
        s2.features.strategy_runner_v2 = true;
        assert!(s2.features.v2_trading_enabled());
        assert!(s2.validate().is_ok());
    }

    #[test]
    fn env_overlay_ignores_unrelated_hype_vars() {
        // M-CF1: only env vars that resolve to real settings fields are
        // injected; loader controls and typo'd settings are ignored so they
        // cannot be smuggled into the YAML root.
        let mut root = Value::Mapping(Default::default());
        let mut env = HashMap::new();
        env.insert("HYPE_MARKET_DATA__COINS".into(), "[\"BTC\"]".into());
        env.insert("HYPE_RISK__MAX_LEVARAGE".into(), "5".into()); // typo
        env.insert("HYPE_CONFIGS_DIR".into(), "/tmp/nope".into()); // loader control
        env.insert("HYPE_UNRELATED_SECRET".into(), "s3cr3t".into());
        apply_env_overlay(&mut root, &env);
        let s: AppSettings = serde_yaml::from_value(root).unwrap();
        assert_eq!(s.market_data.coins, vec!["BTC"]);
        assert_eq!(s.risk.max_leverage, 5); // default, typo not applied
        let map = serde_yaml::to_value(&s).unwrap();
        let debug = format!("{map:?}");
        assert!(
            !debug.contains("MAX_LEVARAGE") && !debug.contains("max_levarage"),
            "typo'd key must not appear: {debug}"
        );
    }

    #[test]
    fn mainnet_requires_kill_switch_enabled() {
        // H-CF1: mainnet with risk.kill_switch_enabled=false must be refused,
        // even when every mainnet secret is present.
        let _guard = ENV_LOCK.lock().unwrap();
        let cfg_dir = std::env::temp_dir().join("hypeedge_config_kill_switch");
        let _ = std::fs::create_dir_all(&cfg_dir);
        unsafe {
            std::env::set_var("HYPE_CONFIGS_DIR", &cfg_dir);
            std::env::set_var(
                "HYPE_EXCHANGE__ACCOUNT_ADDRESS",
                "0x1234567890abcdef1234567890abcdef12345678",
            );
            std::env::set_var(
                "HYPE_EXCHANGE__AGENT_PRIVATE_KEY",
                "0x0000000000000000000000000000000000000000000000000000000000000001",
            );
            std::env::set_var(
                "HYPE_POSTGRES__URL",
                "postgresql://hypeedge:strongpassword123@db.example.com:5432/hypeedge?sslmode=require",
            );
            std::env::set_var(
                "HYPE_API__ADMIN_TOKEN",
                "a-very-long-admin-token-that-is-at-least-32-characters-long",
            );
            std::env::set_var("HYPE_RISK__KILL_SWITCH_ENABLED", "false");
        }
        let err = load_settings(Some("mainnet")).expect_err("kill switch disabled must fail");
        assert!(
            matches!(err, ConfigError::MainnetKillSwitchDisabled),
            "expected MainnetKillSwitchDisabled, got: {err}"
        );
        // With the kill switch armed, mainnet loads (valid mainnet defaults).
        unsafe {
            std::env::set_var("HYPE_RISK__KILL_SWITCH_ENABLED", "true");
        }
        let settings = load_settings(Some("mainnet")).expect("kill switch enabled loads");
        assert!(settings.risk.kill_switch_enabled);
        unsafe {
            std::env::remove_var("HYPE_RISK__KILL_SWITCH_ENABLED");
            std::env::remove_var("HYPE_API__ADMIN_TOKEN");
            std::env::remove_var("HYPE_POSTGRES__URL");
            std::env::remove_var("HYPE_EXCHANGE__AGENT_PRIVATE_KEY");
            std::env::remove_var("HYPE_EXCHANGE__ACCOUNT_ADDRESS");
            std::env::remove_var("HYPE_CONFIGS_DIR");
        }
    }

    #[test]
    fn funding_arb_restricted_to_live_envs() {
        let mut s = AppSettings::default();
        s.features.durable_ledger_v2 = true;
        s.features.execution_v2 = true;
        s.features.user_stream_v2 = true;
        s.features.reconciliation_v2 = true;
        s.features.strategy_runner_v2 = true;
        s.features.funding_arb_execution_enabled = true;
        assert!(s.validate().is_err()); // dev: observation/control plane only
        // M-FA6: mainnet is hard-disabled for funding-arb live execution even
        // when every other mainnet gate would pass.
        s.environment = "mainnet".into();
        let err = s.validate().unwrap_err();
        assert!(
            err.to_string().contains("testnet"),
            "mainnet must be refused with a testnet-only message: {err}"
        );
        s.environment = "testnet".into();
        assert!(s.validate().is_ok());
        // With the flag off, every environment validates.
        s.features.funding_arb_execution_enabled = false;
        s.environment = "dev".into();
        assert!(s.validate().is_ok());
        s.environment = "mainnet".into();
        assert!(s.validate().is_ok());
    }
}
