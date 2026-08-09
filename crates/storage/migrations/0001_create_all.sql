-- Generated from SQLAlchemy models (Phase 1 storage rewrite).;
-- All FKs are applied after table creation to avoid cycles.;
;

CREATE TABLE account_state (
	id BIGINT GENERATED ALWAYS AS IDENTITY, 
	sub_account TEXT, 
	equity NUMERIC(38, 18) NOT NULL, 
	available_balance NUMERIC(38, 18) NOT NULL, 
	total_margin_used NUMERIC(38, 18) NOT NULL, 
	total_unrealized_pnl NUMERIC(38, 18) NOT NULL, 
	peak_equity NUMERIC(38, 18) NOT NULL, 
	action_credits_remaining BIGINT, 
	exchange_updated_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	reconciled_at TIMESTAMP WITH TIME ZONE, 
	revision BIGINT DEFAULT '0' NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id)
);

CREATE TABLE action_budget_allocations (
	id BIGINT GENERATED ALWAYS AS IDENTITY, 
	quota_owner_address TEXT NOT NULL, 
	strategy_id TEXT NOT NULL, 
	symbol TEXT NOT NULL, 
	soft_allocation BIGINT NOT NULL, 
	hard_allocation BIGINT NOT NULL, 
	status TEXT DEFAULT 'active' NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	released_at TIMESTAMP WITH TIME ZONE, 
	PRIMARY KEY (id), 
	CONSTRAINT ck_action_budget_allocations_status CHECK (status IN ('active', 'released')), 
	CONSTRAINT ck_action_budget_allocations_soft CHECK (soft_allocation >= 0), 
	CONSTRAINT ck_action_budget_allocations_hard CHECK (hard_allocation >= soft_allocation), 
	CONSTRAINT ck_action_budget_allocations_time CHECK (released_at IS NULL OR released_at >= created_at)
);

CREATE TABLE action_budget_events (
	id BIGINT GENERATED ALWAYS AS IDENTITY, 
	quota_owner_address TEXT NOT NULL, 
	strategy_id TEXT, 
	command_item_id BIGINT, 
	event_type TEXT NOT NULL, 
	estimated_delta BIGINT DEFAULT '0' NOT NULL, 
	remote_before BIGINT, 
	remote_after BIGINT, 
	details JSONB DEFAULT '{}' NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT ck_action_budget_events_before CHECK (remote_before IS NULL OR remote_before >= 0), 
	CONSTRAINT ck_action_budget_events_after CHECK (remote_after IS NULL OR remote_after >= 0)
);

CREATE TABLE action_budget_scopes (
	quota_owner_address TEXT NOT NULL, 
	remote_cap BIGINT NOT NULL, 
	remote_used BIGINT NOT NULL, 
	remote_remaining BIGINT NOT NULL, 
	shadow_used BIGINT DEFAULT '0' NOT NULL, 
	emergency_reserve BIGINT NOT NULL, 
	mode TEXT DEFAULT 'cancel_only' NOT NULL, 
	observed_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	revision BIGINT DEFAULT '0' NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (quota_owner_address), 
	CONSTRAINT ck_action_budget_scopes_mode CHECK (mode IN ('normal', 'conserve', 'critical', 'cancel_only', 'exhausted')), 
	CONSTRAINT ck_action_budget_scopes_nonnegative CHECK (remote_cap >= 0 AND remote_used >= 0 AND remote_remaining >= 0 AND shadow_used >= 0), 
	CONSTRAINT ck_action_budget_scopes_emergency_reserve CHECK (emergency_reserve >= 0), 
	CONSTRAINT ck_action_budget_scopes_used_cap CHECK (remote_used <= remote_cap), 
	CONSTRAINT ck_action_budget_scopes_balance CHECK (remote_remaining = remote_cap - remote_used), 
	CONSTRAINT ck_action_budget_scopes_reserve_cap CHECK (emergency_reserve <= remote_cap), 
	CONSTRAINT ck_action_budget_scopes_address CHECK (quota_owner_address ~ '^0x[0-9a-f]{40}$'), 
	CONSTRAINT ck_action_budget_scopes_revision CHECK (revision >= 0)
);

CREATE TABLE api_audit (
	id BIGINT GENERATED ALWAYS AS IDENTITY, 
	audit_id UUID NOT NULL, 
	request_id UUID NOT NULL, 
	actor_type TEXT NOT NULL, 
	actor_id TEXT NOT NULL, 
	role TEXT NOT NULL, 
	action TEXT NOT NULL, 
	resource_type TEXT, 
	resource_id TEXT, 
	outcome TEXT NOT NULL, 
	reason TEXT, 
	ip_address INET, 
	user_agent TEXT, 
	details JSONB DEFAULT '{}' NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	UNIQUE (audit_id)
);

CREATE TABLE exchange_sync_cursors (
	id BIGINT GENERATED ALWAYS AS IDENTITY, 
	source TEXT NOT NULL, 
	sub_account TEXT NOT NULL, 
	stream TEXT NOT NULL, 
	last_exchange_timestamp_ms BIGINT DEFAULT '0' NOT NULL, 
	last_external_event_id TEXT, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_exchange_sync_cursor_scope UNIQUE (source, sub_account, stream)
);

CREATE TABLE execution_actions (
	id BIGINT GENERATED ALWAYS AS IDENTITY, 
	command_item_id BIGINT NOT NULL, 
	attempt INTEGER NOT NULL, 
	action_type TEXT NOT NULL, 
	request_hash TEXT NOT NULL, 
	sent_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	responded_at TIMESTAMP WITH TIME ZONE, 
	outcome TEXT NOT NULL, 
	response_code TEXT, 
	estimated_credit_cost BIGINT NOT NULL, 
	reconciled_credit_cost BIGINT, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT ck_execution_actions_action CHECK (action_type IN ('place', 'cancel', 'modify')), 
	CONSTRAINT ck_execution_actions_outcome CHECK (outcome IN ('succeeded', 'rejected', 'timeout', 'unknown', 'transport_error')), 
	CONSTRAINT uq_execution_actions_attempt UNIQUE (command_item_id, attempt), 
	CONSTRAINT ck_execution_actions_attempt CHECK (attempt > 0), 
	CONSTRAINT ck_execution_actions_request_hash CHECK (length(request_hash) = 64), 
	CONSTRAINT ck_execution_actions_estimated_cost CHECK (estimated_credit_cost >= 0), 
	CONSTRAINT ck_execution_actions_reconciled_cost CHECK (reconciled_credit_cost IS NULL OR reconciled_credit_cost >= 0), 
	CONSTRAINT ck_execution_actions_time CHECK (responded_at IS NULL OR responded_at >= sent_at)
);

CREATE TABLE execution_command_items (
	id BIGINT GENERATED ALWAYS AS IDENTITY, 
	command_id UUID NOT NULL, 
	plan_item_id BIGINT, 
	ordinal INTEGER NOT NULL, 
	action_type TEXT NOT NULL, 
	source_order_id UUID, 
	target_order_id UUID, 
	status TEXT DEFAULT 'pending' NOT NULL, 
	resolution TEXT, 
	attempt_count INTEGER DEFAULT '0' NOT NULL, 
	available_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	locked_at TIMESTAMP WITH TIME ZONE, 
	locked_by TEXT, 
	completed_at TIMESTAMP WITH TIME ZONE, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT ck_execution_command_items_action CHECK (action_type IN ('place', 'cancel', 'modify')), 
	CONSTRAINT ck_execution_command_items_status CHECK (status IN ('pending', 'processing', 'succeeded', 'failed', 'unknown', 'cancelled', 'superseded', 'expired', 'blocked')), 
	CONSTRAINT uq_execution_command_items_ordinal UNIQUE (command_id, ordinal), 
	CONSTRAINT uq_execution_command_items_id_command UNIQUE (id, command_id), 
	CONSTRAINT ck_execution_command_items_ordinal CHECK (ordinal >= 0), 
	CONSTRAINT ck_execution_command_items_attempts CHECK (attempt_count >= 0)
);

CREATE TABLE execution_commands (
	id BIGINT GENERATED ALWAYS AS IDENTITY, 
	command_id UUID NOT NULL, 
	order_id UUID, 
	command_type TEXT NOT NULL, 
	actor_type TEXT NOT NULL, 
	actor_id TEXT NOT NULL, 
	idempotency_key TEXT NOT NULL, 
	priority INTEGER DEFAULT '100' NOT NULL, 
	status TEXT DEFAULT 'pending' NOT NULL, 
	payload JSONB NOT NULL, 
	attempt_count INTEGER DEFAULT '0' NOT NULL, 
	available_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	locked_at TIMESTAMP WITH TIME ZONE, 
	locked_by TEXT, 
	completed_at TIMESTAMP WITH TIME ZONE, 
	last_error_code TEXT, 
	last_error_message TEXT, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT ck_execution_commands_status CHECK (status IN ('pending', 'processing', 'succeeded', 'failed', 'unknown', 'cancelled')), 
	CONSTRAINT uq_execution_commands_actor_idempotency UNIQUE (actor_id, idempotency_key), 
	CONSTRAINT ck_execution_commands_attempt_count CHECK (attempt_count >= 0), 
	UNIQUE (command_id)
);

CREATE TABLE fills (
	id BIGINT GENERATED ALWAYS AS IDENTITY, 
	fill_id UUID NOT NULL, 
	source TEXT DEFAULT 'hyperliquid' NOT NULL, 
	exchange_fill_id TEXT NOT NULL, 
	order_id UUID, 
	cloid TEXT, 
	exchange_oid TEXT NOT NULL, 
	symbol TEXT NOT NULL, 
	side TEXT NOT NULL, 
	price NUMERIC(38, 18) NOT NULL, 
	size NUMERIC(38, 18) NOT NULL, 
	fee NUMERIC(38, 18) DEFAULT '0' NOT NULL, 
	realized_pnl NUMERIC(38, 18) DEFAULT '0' NOT NULL, 
	is_maker BOOLEAN DEFAULT 'false' NOT NULL, 
	is_spot BOOLEAN DEFAULT 'false' NOT NULL, 
	strategy_id TEXT, 
	sub_account TEXT, 
	occurred_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	timestamp TIMESTAMP WITH TIME ZONE NOT NULL, 
	raw_event JSONB DEFAULT '{}' NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_fills_source_exchange_fill UNIQUE (source, exchange_fill_id), 
	CONSTRAINT ck_fills_price_positive CHECK (price > 0), 
	CONSTRAINT ck_fills_size_positive CHECK (size > 0), 
	UNIQUE (fill_id)
);

CREATE TABLE funding_arb_config_versions (
	config_version_id BIGINT NOT NULL, 
	spot_coin VARCHAR(64) NOT NULL, 
	entry_funding_rate NUMERIC(38, 18) NOT NULL, 
	exit_funding_rate NUMERIC(38, 18) NOT NULL, 
	max_notional_usd NUMERIC(38, 18) NOT NULL, 
	hedge_ratio NUMERIC(38, 18) NOT NULL, 
	rebalance_threshold_bps BIGINT NOT NULL, 
	leverage NUMERIC(38, 18) NOT NULL, 
	max_slippage_bps BIGINT DEFAULT '50' NOT NULL, 
	max_basis_bps BIGINT DEFAULT '500' NOT NULL, 
	min_expected_edge_bps NUMERIC(38, 18) DEFAULT '5' NOT NULL, 
	expected_hold_hours BIGINT DEFAULT '8' NOT NULL, 
	round_trip_fee_bps NUMERIC(38, 18) DEFAULT '20' NOT NULL, 
	max_unhedged_seconds BIGINT DEFAULT '15' NOT NULL, 
	PRIMARY KEY (config_version_id), 
	CONSTRAINT ck_fa_config_spot_coin CHECK (length(spot_coin) > 0), 
	CONSTRAINT ck_fa_config_spot_market CHECK (spot_coin ~ '^(@[0-9]+|[A-Za-z0-9_.:-]+/[A-Za-z0-9_.:-]+)$'), 
	CONSTRAINT ck_fa_config_entry_funding CHECK (entry_funding_rate > 0), 
	CONSTRAINT ck_fa_config_exit_funding CHECK (exit_funding_rate >= 0), 
	CONSTRAINT ck_fa_config_rate_hysteresis CHECK (exit_funding_rate < entry_funding_rate), 
	CONSTRAINT ck_fa_config_max_notional CHECK (max_notional_usd > 0), 
	CONSTRAINT ck_fa_config_hedge_ratio CHECK (hedge_ratio > 0 AND hedge_ratio <= 1), 
	CONSTRAINT ck_fa_config_rebalance_bps CHECK (rebalance_threshold_bps > 0), 
	CONSTRAINT ck_fa_config_leverage CHECK (leverage > 0), 
	CONSTRAINT ck_fa_config_max_slippage CHECK (max_slippage_bps BETWEEN 1 AND 500), 
	CONSTRAINT ck_fa_config_max_basis CHECK (max_basis_bps > 0), 
	CONSTRAINT ck_fa_config_min_edge CHECK (min_expected_edge_bps >= 0), 
	CONSTRAINT ck_fa_config_hold_hours CHECK (expected_hold_hours BETWEEN 1 AND 168), 
	CONSTRAINT ck_fa_config_round_trip_fee CHECK (round_trip_fee_bps >= 0), 
	CONSTRAINT ck_fa_config_unhedged_seconds CHECK (max_unhedged_seconds BETWEEN 1 AND 60)
);

CREATE TABLE funding_arb_cycle_events (
	id BIGINT GENERATED ALWAYS AS IDENTITY, 
	event_id UUID NOT NULL, 
	cycle_id UUID NOT NULL, 
	revision BIGINT NOT NULL, 
	event_type TEXT NOT NULL, 
	from_state TEXT, 
	to_state TEXT NOT NULL, 
	payload JSONB DEFAULT '{}' NOT NULL, 
	occurred_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_funding_arb_cycle_events_revision UNIQUE (cycle_id, revision), 
	CONSTRAINT ck_funding_arb_cycle_events_revision CHECK (revision > 0), 
	UNIQUE (event_id)
);

CREATE TABLE funding_arb_cycles (
	cycle_id UUID NOT NULL, 
	strategy_id TEXT NOT NULL, 
	config_version_id BIGINT NOT NULL, 
	config_revision BIGINT NOT NULL, 
	sub_account TEXT NOT NULL, 
	perp_symbol TEXT NOT NULL, 
	spot_symbol TEXT NOT NULL, 
	spot_display TEXT NOT NULL, 
	base_token TEXT NOT NULL, 
	quote_token TEXT NOT NULL, 
	state TEXT NOT NULL, 
	target_perp_size NUMERIC(38, 18) NOT NULL, 
	target_spot_size NUMERIC(38, 18) NOT NULL, 
	perp_open_size NUMERIC(38, 18) DEFAULT '0' NOT NULL, 
	spot_open_size NUMERIC(38, 18) DEFAULT '0' NOT NULL, 
	baseline_spot_size NUMERIC(38, 18) DEFAULT '0' NOT NULL, 
	spot_entry_cloid TEXT, 
	perp_entry_cloid TEXT, 
	compensation_cloid TEXT, 
	perp_exit_cloid TEXT, 
	spot_exit_cloid TEXT, 
	entry_funding_rate NUMERIC(38, 18) NOT NULL, 
	entry_basis_bps NUMERIC(38, 18) NOT NULL, 
	error_code TEXT, 
	error_message TEXT, 
	revision BIGINT DEFAULT '0' NOT NULL, 
	opened_at TIMESTAMP WITH TIME ZONE, 
	closed_at TIMESTAMP WITH TIME ZONE, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (cycle_id), 
	CONSTRAINT ck_funding_arb_cycles_state CHECK (state IN ('entering_spot', 'entering_perp', 'compensating_entry', 'open', 'rebalancing', 'exiting_perp', 'exiting_spot', 'closed', 'faulted')), 
	CONSTRAINT ck_funding_arb_cycles_target_perp CHECK (target_perp_size > 0), 
	CONSTRAINT ck_funding_arb_cycles_target_spot CHECK (target_spot_size > 0), 
	CONSTRAINT ck_funding_arb_cycles_perp_open CHECK (perp_open_size >= 0), 
	CONSTRAINT ck_funding_arb_cycles_spot_open CHECK (spot_open_size >= 0), 
	CONSTRAINT ck_funding_arb_cycles_spot_baseline CHECK (baseline_spot_size >= 0), 
	CONSTRAINT ck_funding_arb_cycles_revision CHECK (revision >= 0)
);

CREATE TABLE funding_payments (
	id BIGINT GENERATED ALWAYS AS IDENTITY, 
	payment_id UUID NOT NULL, 
	source TEXT DEFAULT 'hyperliquid' NOT NULL, 
	external_event_id TEXT NOT NULL, 
	sub_account TEXT NOT NULL, 
	cycle_id UUID, 
	symbol TEXT NOT NULL, 
	amount NUMERIC(38, 18) NOT NULL, 
	funding_rate NUMERIC(38, 18) NOT NULL, 
	position_size NUMERIC(38, 18) NOT NULL, 
	occurred_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	raw_event JSONB DEFAULT '{}' NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_funding_payments_source_event UNIQUE (source, external_event_id), 
	UNIQUE (payment_id)
);

CREATE TABLE inbox_events (
	id BIGINT GENERATED ALWAYS AS IDENTITY, 
	event_id UUID NOT NULL, 
	source TEXT NOT NULL, 
	external_event_id TEXT NOT NULL, 
	event_type TEXT NOT NULL, 
	payload_hash TEXT NOT NULL, 
	payload JSONB NOT NULL, 
	received_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	processed_at TIMESTAMP WITH TIME ZONE, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_inbox_events_source_external UNIQUE (source, external_event_id), 
	UNIQUE (event_id)
);

CREATE TABLE ledger_entries (
	id BIGINT GENERATED ALWAYS AS IDENTITY, 
	entry_id UUID NOT NULL, 
	fill_id UUID NOT NULL, 
	entry_type TEXT NOT NULL, 
	asset TEXT DEFAULT 'USDC' NOT NULL, 
	amount NUMERIC(38, 18) NOT NULL, 
	sub_account TEXT, 
	strategy_id TEXT, 
	occurred_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	metadata JSONB DEFAULT '{}' NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_ledger_entries_fill_type UNIQUE (fill_id, entry_type), 
	UNIQUE (entry_id)
);

CREATE TABLE market_maker_config_versions (
	config_version_id BIGINT NOT NULL, 
	soft_inventory_notional NUMERIC(38, 18) NOT NULL, 
	hard_inventory_notional NUMERIC(38, 18) NOT NULL, 
	emergency_inventory_notional NUMERIC(38, 18) NOT NULL, 
	quote_size NUMERIC(38, 18) NOT NULL, 
	max_depth_participation NUMERIC(38, 18) NOT NULL, 
	inventory_skew_bps NUMERIC(38, 18) NOT NULL, 
	max_inventory_shift_bps NUMERIC(38, 18) NOT NULL, 
	min_half_spread_bps NUMERIC(38, 18) NOT NULL, 
	toxicity_spread_bps NUMERIC(38, 18) NOT NULL, 
	min_expected_pnl_usdc NUMERIC(38, 18) NOT NULL, 
	external_reference_weight NUMERIC(38, 18) NOT NULL, 
	external_max_age_seconds NUMERIC(38, 18) NOT NULL, 
	external_outlier_bps NUMERIC(38, 18) NOT NULL, 
	max_external_shift_ticks NUMERIC(38, 18) NOT NULL, 
	max_total_fair_shift_ticks NUMERIC(38, 18) NOT NULL, 
	latency_risk_multiplier NUMERIC(38, 18) NOT NULL, 
	conservative_latency_seconds NUMERIC(38, 18) NOT NULL, 
	conservative_markout_bps NUMERIC(38, 18) NOT NULL, 
	min_markout_samples BIGINT NOT NULL, 
	min_quote_lifetime_ms BIGINT NOT NULL, 
	refresh_cooldown_ms BIGINT NOT NULL, 
	max_quote_age_ms BIGINT NOT NULL, 
	market_stale_after_ms BIGINT NOT NULL, 
	account_stale_after_ms BIGINT NOT NULL, 
	PRIMARY KEY (config_version_id), 
	CONSTRAINT ck_mm_config_soft_inventory CHECK (soft_inventory_notional > 0), 
	CONSTRAINT ck_mm_config_hard_inventory CHECK (hard_inventory_notional >= soft_inventory_notional), 
	CONSTRAINT ck_mm_config_emergency_inventory CHECK (emergency_inventory_notional >= hard_inventory_notional), 
	CONSTRAINT ck_mm_config_quote_size CHECK (quote_size > 0), 
	CONSTRAINT ck_mm_config_depth CHECK (max_depth_participation > 0 AND max_depth_participation <= 1), 
	CONSTRAINT ck_mm_config_min_lifetime CHECK (min_quote_lifetime_ms >= 0), 
	CONSTRAINT ck_mm_config_cooldown CHECK (refresh_cooldown_ms >= 0), 
	CONSTRAINT ck_mm_config_max_age CHECK (max_quote_age_ms > 0), 
	CONSTRAINT ck_mm_config_market_stale CHECK (market_stale_after_ms > 0), 
	CONSTRAINT ck_mm_config_account_stale CHECK (account_stale_after_ms > 0), 
	CONSTRAINT ck_mm_config_expected_pnl CHECK (min_expected_pnl_usdc >= 0), 
	CONSTRAINT ck_mm_config_external_weight CHECK (external_reference_weight >= 0 AND external_reference_weight <= 1), 
	CONSTRAINT ck_mm_config_external_max_age CHECK (external_max_age_seconds > 0), 
	CONSTRAINT ck_mm_config_external_outlier CHECK (external_outlier_bps > 0), 
	CONSTRAINT ck_mm_config_external_shift CHECK (max_external_shift_ticks >= 0), 
	CONSTRAINT ck_mm_config_total_shift CHECK (max_total_fair_shift_ticks >= 0), 
	CONSTRAINT ck_mm_config_latency_multiplier CHECK (latency_risk_multiplier >= 0), 
	CONSTRAINT ck_mm_config_latency_default CHECK (conservative_latency_seconds >= 0), 
	CONSTRAINT ck_mm_config_markout_default CHECK (conservative_markout_bps >= 0), 
	CONSTRAINT ck_mm_config_markout_samples CHECK (min_markout_samples > 0)
);

CREATE TABLE market_making_sessions (
	id BIGINT GENERATED ALWAYS AS IDENTITY, 
	strategy_id TEXT NOT NULL, 
	config_version_id BIGINT NOT NULL, 
	mode TEXT NOT NULL, 
	started_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	ended_at TIMESTAMP WITH TIME ZONE, 
	stop_reason TEXT, 
	PRIMARY KEY (id), 
	CONSTRAINT ck_market_making_sessions_mode CHECK (mode IN ('shadow', 'testnet', 'mainnet')), 
	CONSTRAINT ck_market_making_sessions_time CHECK (ended_at IS NULL OR ended_at >= started_at), 
	CONSTRAINT uq_market_making_sessions_id_strategy UNIQUE (id, strategy_id)
);

CREATE TABLE order_events (
	id BIGINT GENERATED ALWAYS AS IDENTITY, 
	event_id UUID NOT NULL, 
	order_id UUID NOT NULL, 
	cloid TEXT NOT NULL, 
	revision BIGINT NOT NULL, 
	event_type TEXT NOT NULL, 
	symbol TEXT NOT NULL, 
	side TEXT, 
	size NUMERIC(38, 18), 
	price NUMERIC(38, 18), 
	status TEXT NOT NULL, 
	error_code TEXT, 
	error_message TEXT, 
	strategy_id TEXT, 
	payload JSONB DEFAULT '{}' NOT NULL, 
	extra_data TEXT, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_order_events_order_revision UNIQUE (order_id, revision), 
	CONSTRAINT ck_order_events_revision CHECK (revision >= 0), 
	UNIQUE (event_id)
);

CREATE TABLE orders (
	id BIGINT GENERATED ALWAYS AS IDENTITY, 
	order_id UUID NOT NULL, 
	cloid TEXT NOT NULL, 
	legacy_cloid TEXT, 
	exchange_oid TEXT, 
	command_id UUID, 
	symbol TEXT NOT NULL, 
	side TEXT NOT NULL, 
	order_type TEXT NOT NULL, 
	time_in_force TEXT DEFAULT 'Gtc' NOT NULL, 
	size NUMERIC(38, 18) NOT NULL, 
	price NUMERIC(38, 18), 
	status TEXT DEFAULT 'pending' NOT NULL, 
	strategy_id TEXT, 
	sub_account TEXT, 
	client_id TEXT, 
	reduce_only BOOLEAN DEFAULT 'false' NOT NULL, 
	is_spot BOOLEAN DEFAULT 'false' NOT NULL, 
	risk_reducing BOOLEAN DEFAULT 'false' NOT NULL, 
	max_slippage_bps INTEGER DEFAULT '50' NOT NULL, 
	filled_size NUMERIC(38, 18) DEFAULT '0' NOT NULL, 
	avg_fill_price NUMERIC(38, 18), 
	revision BIGINT DEFAULT '0' NOT NULL, 
	error_code TEXT, 
	error_message TEXT, 
	submitted_at TIMESTAMP WITH TIME ZONE, 
	acknowledged_at TIMESTAMP WITH TIME ZONE, 
	filled_at TIMESTAMP WITH TIME ZONE, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT ck_orders_status CHECK (status IN ('pending', 'submitted', 'submit_unknown', 'acknowledged', 'partial_fill', 'filled', 'cancel_pending', 'cancel_unknown', 'cancelled', 'rejected', 'expired')), 
	CONSTRAINT ck_orders_size_positive CHECK (size > 0), 
	CONSTRAINT ck_orders_filled_size CHECK (filled_size >= 0 AND filled_size <= size), 
	CONSTRAINT ck_orders_price_positive CHECK (price IS NULL OR price > 0), 
	CONSTRAINT ck_orders_cloid_format CHECK (cloid ~ '^0x[0-9a-f]{32}$'), 
	CONSTRAINT ck_orders_max_slippage_bps CHECK (max_slippage_bps BETWEEN 1 AND 500), 
	CONSTRAINT ck_orders_spot_not_reduce_only CHECK (NOT (is_spot AND reduce_only)), 
	UNIQUE (order_id), 
	UNIQUE (cloid)
);

CREATE TABLE outbox_events (
	sequence BIGINT GENERATED ALWAYS AS IDENTITY, 
	event_id UUID NOT NULL, 
	event_type TEXT NOT NULL, 
	schema_version INTEGER DEFAULT '1' NOT NULL, 
	aggregate_type TEXT NOT NULL, 
	aggregate_id TEXT NOT NULL, 
	aggregate_revision BIGINT NOT NULL, 
	correlation_id TEXT, 
	payload JSONB NOT NULL, 
	occurred_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	published_at TIMESTAMP WITH TIME ZONE, 
	claimed_at TIMESTAMP WITH TIME ZONE, 
	claimed_by TEXT, 
	publish_attempts BIGINT DEFAULT '0' NOT NULL, 
	last_publish_error TEXT, 
	PRIMARY KEY (sequence), 
	CONSTRAINT ck_outbox_events_publish_attempts CHECK (publish_attempts >= 0), 
	UNIQUE (event_id)
);

CREATE TABLE pnl (
	id SERIAL NOT NULL, 
	strategy_id VARCHAR(50) NOT NULL, 
	symbol VARCHAR(20) NOT NULL, 
	realized_pnl FLOAT NOT NULL, 
	fees FLOAT NOT NULL, 
	funding FLOAT NOT NULL, 
	trade_count INTEGER NOT NULL, 
	period_start TIMESTAMP WITH TIME ZONE NOT NULL, 
	period_end TIMESTAMP WITH TIME ZONE NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id)
);

CREATE TABLE position_snapshots (
	id SERIAL NOT NULL, 
	symbol VARCHAR(20) NOT NULL, 
	size FLOAT NOT NULL, 
	entry_price FLOAT, 
	mark_price FLOAT, 
	unrealized_pnl FLOAT, 
	leverage INTEGER NOT NULL, 
	strategy_id VARCHAR(50), 
	sub_account VARCHAR(50), 
	snapshot_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id)
);

CREATE TABLE positions (
	id BIGINT GENERATED ALWAYS AS IDENTITY, 
	position_id UUID NOT NULL, 
	sub_account TEXT, 
	symbol TEXT NOT NULL, 
	size NUMERIC(38, 18) DEFAULT '0' NOT NULL, 
	entry_price NUMERIC(38, 18), 
	mark_price NUMERIC(38, 18), 
	unrealized_pnl NUMERIC(38, 18) DEFAULT '0' NOT NULL, 
	realized_pnl NUMERIC(38, 18) DEFAULT '0' NOT NULL, 
	leverage INTEGER DEFAULT '1' NOT NULL, 
	liquidation_price NUMERIC(38, 18), 
	exchange_updated_at TIMESTAMP WITH TIME ZONE, 
	revision BIGINT DEFAULT '0' NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT ck_positions_leverage CHECK (leverage >= 1), 
	UNIQUE (position_id)
);

CREATE TABLE quote_plan_items (
	id BIGINT GENERATED ALWAYS AS IDENTITY, 
	plan_id UUID NOT NULL, 
	ordinal INTEGER NOT NULL, 
	symbol TEXT NOT NULL, 
	side TEXT NOT NULL, 
	level INTEGER DEFAULT '0' NOT NULL, 
	decision TEXT NOT NULL, 
	source_order_id UUID, 
	target_order_id UUID, 
	source_cloid TEXT, 
	target_cloid TEXT, 
	desired_price NUMERIC(38, 18), 
	desired_size NUMERIC(38, 18), 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT ck_quote_plan_items_decision CHECK (decision IN ('place', 'cancel', 'modify', 'blocked_unknown')), 
	CONSTRAINT uq_quote_plan_items_ordinal UNIQUE (plan_id, ordinal), 
	CONSTRAINT ck_quote_plan_items_ordinal CHECK (ordinal >= 0), 
	CONSTRAINT ck_quote_plan_items_level CHECK (level >= 0), 
	CONSTRAINT ck_quote_plan_items_side CHECK (side IN ('buy','sell')), 
	CONSTRAINT ck_quote_plan_items_price CHECK (desired_price IS NULL OR desired_price > 0), 
	CONSTRAINT ck_quote_plan_items_size CHECK (desired_size IS NULL OR desired_size > 0)
);

CREATE TABLE quote_plans (
	plan_id UUID NOT NULL, 
	strategy_id TEXT NOT NULL, 
	session_id BIGINT NOT NULL, 
	config_version_id BIGINT NOT NULL, 
	revision BIGINT NOT NULL, 
	market_version BIGINT NOT NULL, 
	fair_price NUMERIC(38, 18) NOT NULL, 
	reservation_price NUMERIC(38, 18) NOT NULL, 
	inventory_size NUMERIC(38, 18) NOT NULL, 
	budget_mode TEXT NOT NULL, 
	status TEXT DEFAULT 'planned' NOT NULL, 
	valid_until TIMESTAMP WITH TIME ZONE NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (plan_id), 
	CONSTRAINT ck_quote_plans_budget_mode CHECK (budget_mode IN ('normal', 'conserve', 'critical', 'cancel_only', 'exhausted')), 
	CONSTRAINT ck_quote_plans_status CHECK (status IN ('planned', 'dispatching', 'succeeded', 'partial', 'unknown', 'cancelled', 'superseded')), 
	CONSTRAINT uq_quote_plans_revision UNIQUE (strategy_id, session_id, revision), 
	CONSTRAINT uq_quote_plans_id_strategy UNIQUE (plan_id, strategy_id), 
	CONSTRAINT ck_quote_plans_revision CHECK (revision >= 0), 
	CONSTRAINT ck_quote_plans_market_version CHECK (market_version >= 0), 
	CONSTRAINT ck_quote_plans_fair_price CHECK (fair_price > 0), 
	CONSTRAINT ck_quote_plans_reservation_price CHECK (reservation_price > 0), 
	CONSTRAINT ck_quote_plans_valid_until CHECK (valid_until >= created_at)
);

CREATE TABLE quote_slots (
	id BIGINT GENERATED ALWAYS AS IDENTITY, 
	strategy_id TEXT NOT NULL, 
	symbol TEXT NOT NULL, 
	side TEXT NOT NULL, 
	level INTEGER DEFAULT '0' NOT NULL, 
	owner_order_id UUID, 
	owner_plan_id UUID, 
	plan_revision BIGINT DEFAULT '0' NOT NULL, 
	state TEXT DEFAULT 'empty' NOT NULL, 
	revision BIGINT DEFAULT '0' NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT ck_quote_slots_state CHECK (state IN ('empty', 'live', 'inflight', 'unknown', 'orphaned_live', 'recovery_required')), 
	CONSTRAINT uq_quote_slots_key UNIQUE (strategy_id, symbol, side, level), 
	CONSTRAINT ck_quote_slots_level CHECK (level >= 0), 
	CONSTRAINT ck_quote_slots_side CHECK (side IN ('buy','sell')), 
	CONSTRAINT ck_quote_slots_plan_revision CHECK (plan_revision >= 0), 
	CONSTRAINT ck_quote_slots_revision CHECK (revision >= 0)
);

CREATE TABLE reconciliation_diffs (
	id BIGINT GENERATED ALWAYS AS IDENTITY, 
	diff_id UUID NOT NULL, 
	run_id UUID NOT NULL, 
	entity_type TEXT NOT NULL, 
	entity_key TEXT NOT NULL, 
	difference_type TEXT NOT NULL, 
	severity TEXT NOT NULL, 
	local_value JSONB, 
	exchange_value JSONB, 
	resolution TEXT, 
	resolved BOOLEAN DEFAULT 'false' NOT NULL, 
	resolved_at TIMESTAMP WITH TIME ZONE, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	UNIQUE (diff_id)
);

CREATE TABLE reconciliation_runs (
	id BIGINT GENERATED ALWAYS AS IDENTITY, 
	run_id UUID NOT NULL, 
	sub_account TEXT, 
	trigger TEXT NOT NULL, 
	status TEXT DEFAULT 'running' NOT NULL, 
	required_queries JSONB DEFAULT '[]' NOT NULL, 
	completed_queries JSONB DEFAULT '[]' NOT NULL, 
	error_code TEXT, 
	error_message TEXT, 
	started_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	finished_at TIMESTAMP WITH TIME ZONE, 
	PRIMARY KEY (id), 
	CONSTRAINT ck_reconciliation_runs_status CHECK (status IN ('running', 'succeeded', 'failed')), 
	UNIQUE (run_id)
);

CREATE TABLE risk_events (
	id BIGINT GENERATED ALWAYS AS IDENTITY, 
	risk_event_id UUID NOT NULL, 
	command_id UUID NOT NULL, 
	order_id UUID, 
	sub_account TEXT, 
	strategy_id TEXT, 
	passed BOOLEAN NOT NULL, 
	reason_code TEXT, 
	reason TEXT, 
	checked_limits JSONB DEFAULT '[]' NOT NULL, 
	snapshot JSONB DEFAULT '{}' NOT NULL, 
	duration_ms BIGINT NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	UNIQUE (risk_event_id)
);

CREATE TABLE risk_reservations (
	id BIGINT GENERATED ALWAYS AS IDENTITY, 
	reservation_id UUID NOT NULL, 
	command_id UUID NOT NULL, 
	command_item_id BIGINT, 
	risk_owner_type TEXT DEFAULT 'legacy' NOT NULL, 
	risk_owner_key TEXT DEFAULT gen_random_uuid()::text NOT NULL, 
	order_id UUID, 
	sub_account TEXT, 
	strategy_id TEXT, 
	symbol TEXT NOT NULL, 
	side TEXT NOT NULL, 
	reduce_only BOOLEAN DEFAULT 'false' NOT NULL, 
	reserved_size NUMERIC(38, 18) NOT NULL, 
	reserved_notional NUMERIC(38, 18) NOT NULL, 
	status TEXT DEFAULT 'active' NOT NULL, 
	expires_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	released_at TIMESTAMP WITH TIME ZONE, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT ck_risk_reservations_status CHECK (status IN ('active', 'consumed', 'released', 'expired')), 
	CONSTRAINT ck_risk_reservations_owner_type CHECK (risk_owner_type IN ('legacy', 'live_order', 'inflight_place', 'unknown', 'new_quote')), 
	CONSTRAINT ck_risk_reservations_notional CHECK (reserved_notional >= 0), 
	CONSTRAINT ck_risk_reservations_size CHECK (reserved_size >= 0), 
	CONSTRAINT uq_risk_reservations_command_owner UNIQUE (command_id, risk_owner_key), 
	UNIQUE (reservation_id)
);

CREATE TABLE spot_balances (
	id BIGINT GENERATED ALWAYS AS IDENTITY, 
	balance_id UUID NOT NULL, 
	sub_account TEXT, 
	token TEXT NOT NULL, 
	total NUMERIC(38, 18) DEFAULT '0' NOT NULL, 
	hold NUMERIC(38, 18) DEFAULT '0' NOT NULL, 
	entry_ntl NUMERIC(38, 18) DEFAULT '0' NOT NULL, 
	exchange_updated_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	revision BIGINT DEFAULT '0' NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT ck_spot_balances_total CHECK (total >= 0), 
	CONSTRAINT ck_spot_balances_hold CHECK (hold >= 0 AND hold <= total), 
	UNIQUE (balance_id)
);

CREATE TABLE strategy_allocations (
	id BIGINT GENERATED ALWAYS AS IDENTITY, 
	strategy_id TEXT NOT NULL, 
	sub_account TEXT, 
	symbol TEXT NOT NULL, 
	allocated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	revision BIGINT DEFAULT '0' NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT ck_strategy_allocations_revision CHECK (revision >= 0)
);

CREATE TABLE strategy_config_versions (
	id BIGINT GENERATED ALWAYS AS IDENTITY, 
	strategy_id TEXT NOT NULL, 
	version BIGINT NOT NULL, 
	config_hash TEXT NOT NULL, 
	created_by TEXT NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_strategy_config_versions_version UNIQUE (strategy_id, version), 
	CONSTRAINT uq_strategy_config_versions_hash UNIQUE (strategy_id, config_hash), 
	CONSTRAINT uq_strategy_config_versions_id_strategy UNIQUE (id, strategy_id), 
	CONSTRAINT ck_strategy_config_versions_version CHECK (version > 0), 
	CONSTRAINT ck_strategy_config_versions_hash CHECK (length(config_hash) = 64)
);

CREATE TABLE strategy_instances (
	strategy_id TEXT NOT NULL, 
	strategy_type TEXT NOT NULL, 
	sub_account TEXT, 
	symbol TEXT NOT NULL, 
	desired_state TEXT DEFAULT 'stopped' NOT NULL, 
	desired_config_version_id BIGINT, 
	revision BIGINT DEFAULT '0' NOT NULL, 
	metadata JSONB DEFAULT '{}' NOT NULL, 
	archived_at TIMESTAMP WITH TIME ZONE, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (strategy_id), 
	CONSTRAINT ck_strategy_instances_type CHECK (strategy_type IN ('funding_arb', 'trend_follow', 'market_maker', 'legacy')), 
	CONSTRAINT ck_strategy_instances_desired_state CHECK (desired_state IN ('stopped', 'warming', 'shadow', 'running', 'paused', 'draining', 'faulted')), 
	CONSTRAINT ck_strategy_instances_revision CHECK (revision >= 0), 
	CONSTRAINT ck_strategy_instances_archive_time CHECK (archived_at IS NULL OR archived_at >= created_at)
);

CREATE TABLE strategy_runtime_state (
	strategy_id TEXT NOT NULL, 
	actual_state TEXT DEFAULT 'stopped' NOT NULL, 
	effective_config_version_id BIGINT, 
	heartbeat_at TIMESTAMP WITH TIME ZONE, 
	revision BIGINT DEFAULT '0' NOT NULL, 
	reason TEXT, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (strategy_id), 
	CONSTRAINT ck_strategy_runtime_state_actual CHECK (actual_state IN ('stopped', 'warming', 'shadow', 'running', 'paused', 'draining', 'faulted')), 
	CONSTRAINT ck_strategy_runtime_state_revision CHECK (revision >= 0)
);

CREATE TABLE strategy_state_events (
	id BIGINT GENERATED ALWAYS AS IDENTITY, 
	strategy_id TEXT NOT NULL, 
	from_state TEXT, 
	to_state TEXT NOT NULL, 
	desired_config_version_id BIGINT, 
	effective_config_version_id BIGINT, 
	reason TEXT, 
	actor TEXT NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT ck_strategy_state_events_from_state CHECK (from_state IS NULL OR from_state IN ('stopped','warming','shadow','running','paused','draining','faulted')), 
	CONSTRAINT ck_strategy_state_events_to_state CHECK (to_state IN ('stopped','warming','shadow','running','paused','draining','faulted'))
);

CREATE TABLE system_state (
	state_key TEXT DEFAULT 'trading' NOT NULL, 
	state TEXT DEFAULT 'starting' NOT NULL, 
	kill_switch_active BOOLEAN DEFAULT 'false' NOT NULL, 
	reason TEXT, 
	triggered_by TEXT, 
	triggered_at TIMESTAMP WITH TIME ZONE, 
	last_reconciliation_id UUID, 
	revision BIGINT DEFAULT '0' NOT NULL, 
	metadata JSONB DEFAULT '{}' NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (state_key), 
	CONSTRAINT ck_system_state_state CHECK (state IN ('starting', 'reconciling', 'normal', 'reduce_only', 'cancel_only', 'halting', 'halted', 'recovering', 'stopping'))
);

CREATE TABLE trend_follow_config_versions (
	config_version_id BIGINT NOT NULL, 
	fast_ema_period BIGINT NOT NULL, 
	slow_ema_period BIGINT NOT NULL, 
	signal_ema_period BIGINT NOT NULL, 
	momentum_period BIGINT NOT NULL, 
	momentum_threshold NUMERIC(38, 18) NOT NULL, 
	atr_period BIGINT NOT NULL, 
	atr_position_multiplier NUMERIC(38, 18) NOT NULL, 
	atr_stop_multiplier NUMERIC(38, 18) NOT NULL, 
	max_position_pct NUMERIC(38, 18) NOT NULL, 
	risk_per_trade_pct NUMERIC(38, 18) NOT NULL, 
	macd_cross_threshold NUMERIC(38, 18) NOT NULL, 
	PRIMARY KEY (config_version_id), 
	CONSTRAINT ck_tf_config_fast_ema CHECK (fast_ema_period > 0), 
	CONSTRAINT ck_tf_config_slow_ema CHECK (slow_ema_period > 0), 
	CONSTRAINT ck_tf_config_ema_order CHECK (fast_ema_period < slow_ema_period), 
	CONSTRAINT ck_tf_config_signal_ema CHECK (signal_ema_period > 0), 
	CONSTRAINT ck_tf_config_momentum_period CHECK (momentum_period > 0), 
	CONSTRAINT ck_tf_config_atr_period CHECK (atr_period > 0), 
	CONSTRAINT ck_tf_config_atr_pos_mult CHECK (atr_position_multiplier > 0), 
	CONSTRAINT ck_tf_config_atr_stop_mult CHECK (atr_stop_multiplier > 0), 
	CONSTRAINT ck_tf_config_max_pos CHECK (max_position_pct > 0 AND max_position_pct <= 1), 
	CONSTRAINT ck_tf_config_risk CHECK (risk_per_trade_pct > 0 AND risk_per_trade_pct <= 1)
);
CREATE UNIQUE INDEX uq_account_state_scope ON account_state (sub_account) NULLS NOT DISTINCT;
CREATE UNIQUE INDEX uq_action_budget_allocations_active_scope ON action_budget_allocations (quota_owner_address, strategy_id, symbol) WHERE status = 'active';
CREATE INDEX ix_action_budget_allocations_strategy_id ON action_budget_allocations (strategy_id);
CREATE INDEX ix_action_budget_allocations_quota_owner_address ON action_budget_allocations (quota_owner_address);
CREATE INDEX ix_action_budget_events_strategy_id ON action_budget_events (strategy_id);
CREATE INDEX ix_action_budget_events_command_item_id ON action_budget_events (command_item_id);
CREATE INDEX ix_action_budget_events_scope_created ON action_budget_events (quota_owner_address, created_at);
CREATE INDEX ix_action_budget_events_quota_owner_address ON action_budget_events (quota_owner_address);
CREATE INDEX ix_api_audit_request_id ON api_audit (request_id);
CREATE INDEX ix_api_audit_actor_created ON api_audit (actor_id, created_at);
CREATE INDEX ix_execution_actions_sent ON execution_actions (sent_at);
CREATE INDEX ix_execution_actions_command_item_id ON execution_actions (command_item_id);
CREATE INDEX ix_execution_command_items_command_id ON execution_command_items (command_id);
CREATE INDEX ix_execution_command_items_ready ON execution_command_items (status, available_at);
CREATE INDEX ix_execution_command_items_target_order_id ON execution_command_items (target_order_id);
CREATE INDEX ix_execution_command_items_plan_item_id ON execution_command_items (plan_item_id);
CREATE INDEX ix_execution_command_items_source_order_id ON execution_command_items (source_order_id);
CREATE INDEX ix_execution_commands_order_id ON execution_commands (order_id);
CREATE INDEX ix_execution_commands_status_updated ON execution_commands (status, updated_at);
CREATE INDEX ix_execution_commands_ready ON execution_commands (priority, created_at) WHERE status = 'pending';
CREATE INDEX ix_fills_exchange_oid ON fills (exchange_oid);
CREATE INDEX ix_fills_symbol ON fills (symbol);
CREATE INDEX ix_fills_cloid ON fills (cloid);
CREATE INDEX ix_fills_strategy_id ON fills (strategy_id);
CREATE INDEX ix_fills_order_id ON fills (order_id);
CREATE INDEX ix_fills_sub_account ON fills (sub_account);
CREATE INDEX ix_fills_symbol_occurred ON fills (symbol, occurred_at);
CREATE INDEX ix_fills_account_occurred ON fills (sub_account, occurred_at);
CREATE INDEX ix_funding_arb_cycle_events_cycle_id ON funding_arb_cycle_events (cycle_id);
CREATE INDEX ix_funding_arb_cycle_events_cycle_created ON funding_arb_cycle_events (cycle_id, occurred_at);
CREATE INDEX ix_funding_arb_cycles_sub_account ON funding_arb_cycles (sub_account);
CREATE INDEX ix_funding_arb_cycles_spot_exit_cloid ON funding_arb_cycles (spot_exit_cloid);
CREATE INDEX ix_funding_arb_cycles_spot_entry_cloid ON funding_arb_cycles (spot_entry_cloid);
CREATE INDEX ix_funding_arb_cycles_compensation_cloid ON funding_arb_cycles (compensation_cloid);
CREATE INDEX ix_funding_arb_cycles_strategy_id ON funding_arb_cycles (strategy_id);
CREATE INDEX ix_funding_arb_cycles_strategy_created ON funding_arb_cycles (strategy_id, created_at);
CREATE INDEX ix_funding_arb_cycles_config ON funding_arb_cycles (config_version_id);
CREATE INDEX ix_funding_arb_cycles_config_version_id ON funding_arb_cycles (config_version_id);
CREATE UNIQUE INDEX uq_funding_arb_cycles_active_strategy ON funding_arb_cycles (strategy_id) WHERE state <> 'closed';
CREATE INDEX ix_funding_arb_cycles_perp_exit_cloid ON funding_arb_cycles (perp_exit_cloid);
CREATE INDEX ix_funding_arb_cycles_perp_entry_cloid ON funding_arb_cycles (perp_entry_cloid);
CREATE INDEX ix_funding_payments_symbol_occurred ON funding_payments (symbol, occurred_at);
CREATE INDEX ix_funding_payments_sub_account ON funding_payments (sub_account);
CREATE INDEX ix_funding_payments_cycle_id ON funding_payments (cycle_id);
CREATE INDEX ix_funding_payments_symbol ON funding_payments (symbol);
CREATE INDEX ix_inbox_events_received ON inbox_events (received_at);
CREATE INDEX ix_ledger_entries_sub_account ON ledger_entries (sub_account);
CREATE INDEX ix_ledger_entries_account_occurred ON ledger_entries (sub_account, occurred_at);
CREATE INDEX ix_ledger_entries_strategy_id ON ledger_entries (strategy_id);
CREATE INDEX ix_ledger_entries_fill_id ON ledger_entries (fill_id);
CREATE INDEX ix_market_making_sessions_config_version_id ON market_making_sessions (config_version_id);
CREATE INDEX ix_market_making_sessions_strategy_id ON market_making_sessions (strategy_id);
CREATE UNIQUE INDEX uq_market_making_sessions_active_strategy ON market_making_sessions (strategy_id) WHERE ended_at IS NULL;
CREATE INDEX ix_market_making_sessions_strategy_started ON market_making_sessions (strategy_id, started_at);
CREATE INDEX ix_order_events_order_id ON order_events (order_id);
CREATE INDEX ix_order_events_strategy_id ON order_events (strategy_id);
CREATE INDEX ix_order_events_type_created ON order_events (event_type, created_at);
CREATE INDEX ix_order_events_cloid ON order_events (cloid);
CREATE INDEX ix_order_events_order_created ON order_events (order_id, created_at);
CREATE INDEX ix_orders_exchange_oid ON orders (exchange_oid);
CREATE INDEX ix_orders_sub_account ON orders (sub_account);
CREATE INDEX ix_orders_account_status_created ON orders (sub_account, status, created_at);
CREATE INDEX ix_orders_command_id ON orders (command_id);
CREATE INDEX ix_orders_strategy_created ON orders (strategy_id, created_at);
CREATE INDEX ix_orders_strategy_id ON orders (strategy_id);
CREATE INDEX ix_orders_symbol ON orders (symbol);
CREATE INDEX ix_orders_open ON orders (sub_account, symbol) WHERE status IN ('pending', 'submitted', 'submit_unknown', 'acknowledged', 'partial_fill', 'cancel_pending', 'cancel_unknown');
CREATE INDEX ix_outbox_events_unpublished ON outbox_events (sequence) WHERE published_at IS NULL;
CREATE INDEX ix_outbox_events_aggregate ON outbox_events (aggregate_type, aggregate_id, sequence);
CREATE INDEX ix_outbox_events_dispatch ON outbox_events (claimed_at, sequence) WHERE published_at IS NULL;
CREATE INDEX ix_pnl_symbol ON pnl (symbol);
CREATE INDEX ix_pnl_strategy_id ON pnl (strategy_id);
CREATE INDEX ix_position_snapshots_symbol ON position_snapshots (symbol);
CREATE INDEX ix_position_snapshots_strategy_id ON position_snapshots (strategy_id);
CREATE INDEX ix_positions_symbol ON positions (symbol);
CREATE INDEX ix_positions_sub_account ON positions (sub_account);
CREATE INDEX ix_positions_account_updated ON positions (sub_account, updated_at);
CREATE UNIQUE INDEX uq_positions_scope_symbol ON positions (sub_account, symbol) NULLS NOT DISTINCT;
CREATE INDEX ix_quote_plan_items_slot ON quote_plan_items (symbol, side, level);
CREATE INDEX ix_quote_plan_items_target_order_id ON quote_plan_items (target_order_id);
CREATE INDEX ix_quote_plan_items_plan_id ON quote_plan_items (plan_id);
CREATE INDEX ix_quote_plan_items_source_order_id ON quote_plan_items (source_order_id);
CREATE INDEX ix_quote_plans_strategy_id ON quote_plans (strategy_id);
CREATE INDEX ix_quote_plans_config ON quote_plans (config_version_id);
CREATE INDEX ix_quote_plans_session_created ON quote_plans (session_id, created_at);
CREATE INDEX ix_quote_plans_session_id ON quote_plans (session_id);
CREATE INDEX ix_quote_slots_owner_plan_id ON quote_slots (owner_plan_id);
CREATE INDEX ix_quote_slots_strategy_id ON quote_slots (strategy_id);
CREATE INDEX ix_quote_slots_owner_order ON quote_slots (owner_order_id);
CREATE INDEX ix_reconciliation_diffs_run_id ON reconciliation_diffs (run_id);
CREATE INDEX ix_reconciliation_diffs_run_severity ON reconciliation_diffs (run_id, severity);
CREATE INDEX ix_reconciliation_runs_sub_account ON reconciliation_runs (sub_account);
CREATE INDEX ix_reconciliation_runs_scope_started ON reconciliation_runs (sub_account, started_at);
CREATE INDEX ix_risk_events_sub_account ON risk_events (sub_account);
CREATE INDEX ix_risk_events_order_id ON risk_events (order_id);
CREATE INDEX ix_risk_events_account_created ON risk_events (sub_account, created_at);
CREATE INDEX ix_risk_events_strategy_id ON risk_events (strategy_id);
CREATE INDEX ix_risk_events_command_id ON risk_events (command_id);
CREATE INDEX ix_risk_reservations_sub_account ON risk_reservations (sub_account);
CREATE INDEX ix_risk_reservations_strategy_id ON risk_reservations (strategy_id);
CREATE INDEX ix_risk_reservations_order_id ON risk_reservations (order_id);
CREATE INDEX ix_risk_reservations_command_item_id ON risk_reservations (command_item_id);
CREATE INDEX ix_risk_reservations_active ON risk_reservations (sub_account, expires_at) WHERE status = 'active';
CREATE INDEX ix_spot_balances_sub_account ON spot_balances (sub_account);
CREATE UNIQUE INDEX uq_spot_balances_scope_token ON spot_balances (sub_account, token) NULLS NOT DISTINCT;
CREATE INDEX ix_spot_balances_account_updated ON spot_balances (sub_account, updated_at);
CREATE INDEX ix_spot_balances_token ON spot_balances (token);
CREATE UNIQUE INDEX ix_strategy_allocations_strategy_id ON strategy_allocations (strategy_id);
CREATE UNIQUE INDEX uq_strategy_allocations_scope ON strategy_allocations (sub_account, symbol) NULLS NOT DISTINCT;
CREATE INDEX ix_strategy_config_versions_strategy_id ON strategy_config_versions (strategy_id);
CREATE INDEX ix_strategy_instances_desired_config ON strategy_instances (desired_config_version_id);
CREATE INDEX ix_strategy_instances_scope ON strategy_instances (sub_account, symbol);
CREATE INDEX ix_strategy_runtime_state_effective_config ON strategy_runtime_state (effective_config_version_id);
CREATE INDEX ix_strategy_state_events_desired_config_version_id ON strategy_state_events (desired_config_version_id);
CREATE INDEX ix_strategy_state_events_strategy_created ON strategy_state_events (strategy_id, created_at);
CREATE INDEX ix_strategy_state_events_strategy_id ON strategy_state_events (strategy_id);
CREATE INDEX ix_strategy_state_events_effective_config_version_id ON strategy_state_events (effective_config_version_id);
CREATE INDEX ix_system_state_last_reconciliation_id ON system_state (last_reconciliation_id);
ALTER TABLE action_budget_allocations ADD FOREIGN KEY(strategy_id) REFERENCES strategy_instances (strategy_id) ON DELETE RESTRICT;
ALTER TABLE action_budget_allocations ADD FOREIGN KEY(quota_owner_address) REFERENCES action_budget_scopes (quota_owner_address) ON DELETE RESTRICT;
ALTER TABLE action_budget_events ADD FOREIGN KEY(quota_owner_address) REFERENCES action_budget_scopes (quota_owner_address) ON DELETE RESTRICT;
ALTER TABLE action_budget_events ADD FOREIGN KEY(command_item_id) REFERENCES execution_command_items (id) ON DELETE RESTRICT;
ALTER TABLE action_budget_events ADD FOREIGN KEY(strategy_id) REFERENCES strategy_instances (strategy_id) ON DELETE RESTRICT;
ALTER TABLE execution_actions ADD FOREIGN KEY(command_item_id) REFERENCES execution_command_items (id) ON DELETE RESTRICT;
ALTER TABLE execution_command_items ADD FOREIGN KEY(command_id) REFERENCES execution_commands (command_id) ON DELETE RESTRICT;
ALTER TABLE execution_command_items ADD FOREIGN KEY(source_order_id) REFERENCES orders (order_id) ON DELETE RESTRICT;
ALTER TABLE execution_command_items ADD FOREIGN KEY(target_order_id) REFERENCES orders (order_id) ON DELETE RESTRICT;
ALTER TABLE execution_command_items ADD FOREIGN KEY(plan_item_id) REFERENCES quote_plan_items (id) ON DELETE RESTRICT;
ALTER TABLE execution_commands ADD FOREIGN KEY(order_id) REFERENCES orders (order_id) ON DELETE RESTRICT;
ALTER TABLE fills ADD FOREIGN KEY(order_id) REFERENCES orders (order_id) ON DELETE RESTRICT;
ALTER TABLE funding_arb_config_versions ADD FOREIGN KEY(config_version_id) REFERENCES strategy_config_versions (id) ON DELETE RESTRICT;
ALTER TABLE funding_arb_cycle_events ADD FOREIGN KEY(cycle_id) REFERENCES funding_arb_cycles (cycle_id) ON DELETE RESTRICT;
ALTER TABLE funding_arb_cycles ADD FOREIGN KEY(strategy_id) REFERENCES strategy_instances (strategy_id) ON DELETE RESTRICT;
ALTER TABLE funding_arb_cycles ADD CONSTRAINT fk_funding_arb_cycles_config FOREIGN KEY(config_version_id, strategy_id) REFERENCES strategy_config_versions (id, strategy_id) ON DELETE RESTRICT;
ALTER TABLE funding_payments ADD FOREIGN KEY(cycle_id) REFERENCES funding_arb_cycles (cycle_id) ON DELETE RESTRICT;
ALTER TABLE ledger_entries ADD FOREIGN KEY(fill_id) REFERENCES fills (fill_id) ON DELETE RESTRICT;
ALTER TABLE market_maker_config_versions ADD FOREIGN KEY(config_version_id) REFERENCES strategy_config_versions (id) ON DELETE RESTRICT;
ALTER TABLE market_making_sessions ADD CONSTRAINT fk_market_making_sessions_config FOREIGN KEY(config_version_id, strategy_id) REFERENCES strategy_config_versions (id, strategy_id) ON DELETE RESTRICT;
ALTER TABLE market_making_sessions ADD FOREIGN KEY(strategy_id) REFERENCES strategy_instances (strategy_id) ON DELETE RESTRICT;
ALTER TABLE order_events ADD FOREIGN KEY(order_id) REFERENCES orders (order_id) ON DELETE RESTRICT;
ALTER TABLE quote_plan_items ADD FOREIGN KEY(plan_id) REFERENCES quote_plans (plan_id) ON DELETE RESTRICT;
ALTER TABLE quote_plan_items ADD FOREIGN KEY(source_order_id) REFERENCES orders (order_id) ON DELETE RESTRICT;
ALTER TABLE quote_plan_items ADD FOREIGN KEY(target_order_id) REFERENCES orders (order_id) ON DELETE RESTRICT;
ALTER TABLE quote_plans ADD CONSTRAINT fk_quote_plans_config FOREIGN KEY(config_version_id, strategy_id) REFERENCES strategy_config_versions (id, strategy_id) ON DELETE RESTRICT;
ALTER TABLE quote_plans ADD FOREIGN KEY(strategy_id) REFERENCES strategy_instances (strategy_id) ON DELETE RESTRICT;
ALTER TABLE quote_plans ADD CONSTRAINT fk_quote_plans_session FOREIGN KEY(session_id, strategy_id) REFERENCES market_making_sessions (id, strategy_id) ON DELETE RESTRICT;
ALTER TABLE quote_slots ADD FOREIGN KEY(strategy_id) REFERENCES strategy_instances (strategy_id) ON DELETE RESTRICT;
ALTER TABLE quote_slots ADD CONSTRAINT fk_quote_slots_owner_plan FOREIGN KEY(owner_plan_id, strategy_id) REFERENCES quote_plans (plan_id, strategy_id) ON DELETE RESTRICT;
ALTER TABLE quote_slots ADD FOREIGN KEY(owner_order_id) REFERENCES orders (order_id) ON DELETE RESTRICT;
ALTER TABLE reconciliation_diffs ADD FOREIGN KEY(run_id) REFERENCES reconciliation_runs (run_id) ON DELETE CASCADE;
ALTER TABLE risk_events ADD FOREIGN KEY(order_id) REFERENCES orders (order_id) ON DELETE RESTRICT;
ALTER TABLE risk_reservations ADD CONSTRAINT fk_risk_reservations_command_item FOREIGN KEY(command_item_id, command_id) REFERENCES execution_command_items (id, command_id) ON DELETE RESTRICT;
ALTER TABLE risk_reservations ADD FOREIGN KEY(order_id) REFERENCES orders (order_id) ON DELETE RESTRICT;
ALTER TABLE strategy_allocations ADD FOREIGN KEY(strategy_id) REFERENCES strategy_instances (strategy_id) ON DELETE RESTRICT;
ALTER TABLE strategy_config_versions ADD FOREIGN KEY(strategy_id) REFERENCES strategy_instances (strategy_id) ON DELETE RESTRICT;
ALTER TABLE strategy_instances ADD CONSTRAINT fk_strategy_instances_desired_config FOREIGN KEY(desired_config_version_id, strategy_id) REFERENCES strategy_config_versions (id, strategy_id) ON DELETE RESTRICT;
ALTER TABLE strategy_runtime_state ADD FOREIGN KEY(strategy_id) REFERENCES strategy_instances (strategy_id) ON DELETE RESTRICT;
ALTER TABLE strategy_runtime_state ADD CONSTRAINT fk_strategy_runtime_state_effective_config FOREIGN KEY(effective_config_version_id, strategy_id) REFERENCES strategy_config_versions (id, strategy_id) ON DELETE RESTRICT;
ALTER TABLE strategy_state_events ADD FOREIGN KEY(desired_config_version_id) REFERENCES strategy_config_versions (id) ON DELETE RESTRICT;
ALTER TABLE strategy_state_events ADD FOREIGN KEY(strategy_id) REFERENCES strategy_instances (strategy_id) ON DELETE RESTRICT;
ALTER TABLE strategy_state_events ADD FOREIGN KEY(effective_config_version_id) REFERENCES strategy_config_versions (id) ON DELETE RESTRICT;
ALTER TABLE trend_follow_config_versions ADD FOREIGN KEY(config_version_id) REFERENCES strategy_config_versions (id) ON DELETE RESTRICT;
