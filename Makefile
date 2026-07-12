.PHONY: install sync lint typecheck test test-unit test-integration run clean

# Install dependencies
install:
	uv sync

# Update lockfile
sync:
	uv lock

# Lint with ruff
lint:
	uv run ruff check src/ tests/
	uv run ruff format --check src/ tests/

# Auto-fix lint issues
lint-fix:
	uv run ruff check --fix src/ tests/
	uv run ruff format src/ tests/

# Type check
typecheck:
	uv run mypy src/

# Run all tests
test:
	uv run pytest -v

# Run unit tests only
test-unit:
	uv run pytest tests/unit/ -v

# Run integration tests (requires network/services)
test-integration:
	uv run pytest tests/integration/ -v

# Run the application
run:
	uv run hypeedge

# Emergency kill switch
kill-switch:
	@curl -s -X POST http://localhost:37001/api/kill-switch \
		-H "Content-Type: application/json" \
		-d '{"action":"trigger","reason":"manual_makefile_trigger"}' \
		|| echo "Error: Is the HypeEdge API server running on port 37001?"

# Clean build artifacts
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .mypy_cache -exec rm -rf {} + 2>/dev/null || true
	rm -rf build/ dist/ *.egg-info/
