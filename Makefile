# Makefile for sdm2influx

.PHONY: clean distclean check build install
.DEFAULT_GOAL := check

clean:
	rm -rf dist *.egg-info __pycache__ .mypy_cache .pytest_cache .ruff_cache

distclean: clean
	rm -rf .venv uv.lock

check:
	pre-commit run -a
	uv run pylint sdm2influx.py
	uv run mypy sdm2influx.py

build:
	uv build

install:
	uv sync --dev
