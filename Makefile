.PHONY: install ns node run test lint format typecheck check

install:
	uv sync

ns:
	uv run python -m Pyro5.nameserver --host localhost --port 9090

node:
	uv run python run_node.py $(NODE)

run:
	uv run main.py

test:
	uv run pytest

test-one:
	uv run pytest $(FILE)::$(TEST) -v

lint:
	uv run ruff check --fix

format:
	uv run ruff format

typecheck:
	uv run mypy .

check: format lint typecheck test
