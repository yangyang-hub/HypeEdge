.PHONY: lint typecheck test test-unit test-integration run clean \
	rust-lint rust-typecheck rust-test rust-parity rust-integration rust-run

# Lint with cargo fmt + clippy
lint:
	cargo fmt --all --check
	cargo clippy --workspace --all-targets -- -D warnings

# Auto-fix formatting
lint-fix:
	cargo fmt --all

# Type/compile check
typecheck:
	cargo check --workspace

# Run all tests
test:
	cargo test --workspace

# Run unit tests only
test-unit:
	cargo test --workspace --lib

# Run integration tests (requires network/services)
test-integration:
	cargo test --workspace --tests

# Run the application
run:
	cargo run -p hypeedge_app

# Emergency kill switch
kill-switch:
	@curl -s -X POST http://localhost:37001/api/kill-switch \
		-H "Content-Type: application/json" \
		-d '{"action":"trigger","reason":"manual_makefile_trigger"}' \
		|| echo "Error: Is the HypeEdge API server running on port 37001?"

# Clean build artifacts
clean:
	@cargo clean

# --- Rust target aliases (kept for compatibility) ---

rust-lint:
	cargo fmt --all --check
	cargo clippy --workspace --all-targets -- -D warnings

rust-typecheck:
	cargo check --workspace

rust-test:
	cargo test --workspace

rust-parity:
	cargo test -p hypeedge_domain --test decimal_corpus
	cargo test -p hypeedge_config --test config_parity

rust-integration:
	cargo test -p hypeedge_storage --test durable_store_integration
