//! Strategy type registry, port of `src/hypeedge/strategy/registry.py` + `plugin.py`.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use hypeedge_domain::enums::MarketMakerLifecycle;

/// A persisted strategy instance definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategyInstanceDefinition {
    pub strategy_id: String,
    pub strategy_type: String,
    pub sub_account: String,
    pub symbol: String,
    pub desired_state: MarketMakerLifecycle,
    pub desired_config_revision: u64,
    pub revision: u64,
}

/// An immutable config snapshot for a strategy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategyConfigSnapshot {
    pub strategy_id: String,
    pub revision: u64,
    pub values: serde_json::Value,
}

/// The context handed to a strategy factory.
#[derive(Debug, Clone)]
pub struct StrategyBuildContext {
    pub instance: StrategyInstanceDefinition,
    pub config: StrategyConfigSnapshot,
}

/// The runtime handle the supervisor drives.
#[async_trait]
pub trait StrategyRuntimeHandle: Send + Sync {
    async fn start(&self) -> Result<(), String>;
    async fn set_mode(&self, mode: MarketMakerLifecycle) -> Result<(), String>;
    async fn apply_config(&self, config: &StrategyConfigSnapshot) -> Result<(), String>;
    async fn stop(&self) -> Result<(), String>;
}

pub type StrategyFactory =
    Arc<dyn Fn(&StrategyBuildContext) -> Arc<dyn StrategyRuntimeHandle> + Send + Sync>;

/// Declared capabilities of a strategy type (mirrors `StrategyTypeCapabilities`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategyTypeCapabilities {
    pub creatable: bool,
    pub desired_states: Vec<MarketMakerLifecycle>,
    pub actions: Vec<String>,
    pub supports_shadow: bool,
    pub supports_drain: bool,
    pub workspace: Option<String>,
}

pub fn market_maker_capabilities() -> StrategyTypeCapabilities {
    StrategyTypeCapabilities {
        creatable: true,
        desired_states: vec![
            MarketMakerLifecycle::Stopped,
            MarketMakerLifecycle::Shadow,
            MarketMakerLifecycle::Running,
            MarketMakerLifecycle::Paused,
        ],
        actions: vec![
            "start".into(),
            "pause".into(),
            "resume".into(),
            "drain".into(),
            "stop".into(),
        ],
        supports_shadow: true,
        supports_drain: true,
        workspace: Some("market-making".into()),
    }
}

pub fn trend_follow_capabilities() -> StrategyTypeCapabilities {
    StrategyTypeCapabilities {
        creatable: true,
        desired_states: vec![
            MarketMakerLifecycle::Stopped,
            MarketMakerLifecycle::Running,
            MarketMakerLifecycle::Paused,
        ],
        actions: vec![
            "start".into(),
            "stop".into(),
            "pause".into(),
            "resume".into(),
        ],
        supports_shadow: false,
        supports_drain: false,
        workspace: None,
    }
}

pub fn funding_arb_capabilities() -> StrategyTypeCapabilities {
    StrategyTypeCapabilities {
        creatable: true,
        desired_states: vec![
            MarketMakerLifecycle::Stopped,
            MarketMakerLifecycle::Running,
            MarketMakerLifecycle::Paused,
        ],
        actions: vec![
            "start".into(),
            "stop".into(),
            "pause".into(),
            "resume".into(),
        ],
        supports_shadow: false,
        supports_drain: false,
        workspace: Some("funding-arb".into()),
    }
}

/// A registered strategy type plugin.
pub struct StrategyTypePlugin {
    pub strategy_type: String,
    pub capabilities: StrategyTypeCapabilities,
    pub factory: StrategyFactory,
}

/// The strategy registry (keyed by normalized `strategy_type`).
pub struct StrategyRegistry {
    factories: HashMap<String, StrategyFactory>,
    plugins: HashMap<String, Arc<StrategyTypePlugin>>,
}

impl Default for StrategyRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl StrategyRegistry {
    pub fn new() -> Self {
        Self {
            factories: HashMap::new(),
            plugins: HashMap::new(),
        }
    }

    fn normalize(t: &str) -> String {
        t.trim().to_lowercase()
    }

    pub fn register(&mut self, strategy_type: &str, factory: StrategyFactory) {
        self.factories
            .insert(Self::normalize(strategy_type), factory);
    }

    /// Register a full plugin (capabilities + factory).
    pub fn register_plugin(&mut self, plugin: StrategyTypePlugin) {
        let key = Self::normalize(&plugin.strategy_type);
        let plugin = Arc::new(plugin);
        let factory = plugin.factory.clone();
        self.factories.insert(key.clone(), factory);
        self.plugins.insert(key, plugin);
    }

    pub fn unregister(&mut self, strategy_type: &str) {
        let key = Self::normalize(strategy_type);
        self.factories.remove(&key);
        self.plugins.remove(&key);
    }

    pub fn create(
        &self,
        context: &StrategyBuildContext,
    ) -> Result<Arc<dyn StrategyRuntimeHandle>, String> {
        let factory = self
            .factories
            .get(&Self::normalize(&context.instance.strategy_type))
            .ok_or_else(|| format!("unknown strategy type: {}", context.instance.strategy_type))?;
        Ok(factory(context))
    }

    pub fn get_plugin(&self, strategy_type: &str) -> Option<Arc<StrategyTypePlugin>> {
        self.plugins.get(&Self::normalize(strategy_type)).cloned()
    }

    pub fn capabilities(&self, strategy_type: &str) -> Option<StrategyTypeCapabilities> {
        self.get_plugin(strategy_type)
            .map(|p| p.capabilities.clone())
    }

    pub fn strategy_types(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.factories.keys().cloned().collect();
        keys.sort();
        keys
    }

    pub fn contains(&self, strategy_type: &str) -> bool {
        self.factories.contains_key(&Self::normalize(strategy_type))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_keys_case_insensitive() {
        let mut reg = StrategyRegistry::new();
        let factory: StrategyFactory = Arc::new(|_| {
            struct Noop;
            #[async_trait]
            impl StrategyRuntimeHandle for Noop {
                async fn start(&self) -> Result<(), String> {
                    Ok(())
                }
                async fn set_mode(&self, _: MarketMakerLifecycle) -> Result<(), String> {
                    Ok(())
                }
                async fn apply_config(&self, _: &StrategyConfigSnapshot) -> Result<(), String> {
                    Ok(())
                }
                async fn stop(&self) -> Result<(), String> {
                    Ok(())
                }
            }
            Arc::new(Noop)
        });
        reg.register("Trend_Follow", factory);
        assert!(reg.contains("trend_follow"));
        assert_eq!(reg.strategy_types(), vec!["trend_follow"]);
    }

    #[test]
    fn capabilities_lookup() {
        let mut reg = StrategyRegistry::new();
        reg.register_plugin(StrategyTypePlugin {
            strategy_type: "market_maker".to_string(),
            capabilities: market_maker_capabilities(),
            factory: Arc::new(|_| {
                struct Noop;
                #[async_trait]
                impl StrategyRuntimeHandle for Noop {
                    async fn start(&self) -> Result<(), String> {
                        Ok(())
                    }
                    async fn set_mode(&self, _: MarketMakerLifecycle) -> Result<(), String> {
                        Ok(())
                    }
                    async fn apply_config(&self, _: &StrategyConfigSnapshot) -> Result<(), String> {
                        Ok(())
                    }
                    async fn stop(&self) -> Result<(), String> {
                        Ok(())
                    }
                }
                Arc::new(Noop)
            }),
        });
        let caps = reg.capabilities("market_maker").unwrap();
        assert!(caps.supports_shadow);
        assert!(caps.supports_drain);
        assert_eq!(caps.workspace.as_deref(), Some("market-making"));
    }

    #[test]
    fn capability_tables_match_python() {
        assert_eq!(
            market_maker_capabilities().desired_states,
            vec![
                MarketMakerLifecycle::Stopped,
                MarketMakerLifecycle::Shadow,
                MarketMakerLifecycle::Running,
                MarketMakerLifecycle::Paused
            ]
        );
        assert!(!trend_follow_capabilities().supports_shadow);
        assert_eq!(
            funding_arb_capabilities().workspace.as_deref(),
            Some("funding-arb")
        );
    }
}
