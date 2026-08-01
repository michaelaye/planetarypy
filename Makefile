.PHONY: help clean clean-build clean-pyc clean-test lint fix test test-fast \
        coverage docs docs-api docs-preview docs-clean dev-install install \
        dist check-dist release bump-patch bump-minor bump-major
.DEFAULT_GOAL := help

# Every target runs through the *active* interpreter rather than bare console
# scripts, so `make test` in one conda env can't silently pick up a `pytest` that
# belongs to another env earlier on PATH.
PY := python

define BROWSER_PYSCRIPT
import os, webbrowser, sys

from urllib.request import pathname2url

webbrowser.open("file://" + pathname2url(os.path.abspath(sys.argv[1])))
endef
export BROWSER_PYSCRIPT

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

BROWSER := $(PY) -c "$$BROWSER_PYSCRIPT"

help:
	@$(PY) -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test: ## remove test and coverage artifacts
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache

# Deliberately `ruff check` only, no `ruff format --check`: the tree predates
# ruff's formatter and 97 of 112 files would be rewritten by it. Adopting that is
# a decision to make on purpose, not a side effect of a lint target.
lint: ## check style with ruff
	$(PY) -m ruff check src/planetarypy tests

fix: ## apply ruff's safe autofixes
	$(PY) -m ruff check --fix src/planetarypy tests

test: ## run the test suite with coverage
	$(PY) -m pytest --cov

test-fast: ## run the test suite without coverage (noticeably quicker)
	$(PY) -m pytest --no-cov

coverage: ## write and open an HTML coverage report
	$(PY) -m pytest --cov --cov-report=html
	$(BROWSER) htmlcov/index.html

docs: docs-api ## render full documentation locally
	cd docs && quarto render
	$(BROWSER) docs/_build/index.html

docs-api: ## generate API reference (run before committing doc changes)
	cd docs && quartodoc build

docs-preview: ## preview docs with live reload
	cd docs && quarto preview

docs-clean: ## remove generated HTML (not reference/*.qmd)
	rm -rf docs/_build
	rm -rf docs/.quarto

dev-install: ## install runtime, dev and spice dependencies, then the package editable
	$(PY) install_dev_deps.py
	$(PY) -m pip install -e .

install: clean ## install the package into the active environment
	$(PY) -m pip install .

dist: clean ## build the sdist and wheel
	$(PY) -m build
	ls -l dist

check-dist: dist ## build, then validate the artifacts' PyPI metadata
	$(PY) -m twine check dist/*

release: check-dist ## build, validate and upload a release to PyPI
	$(PY) -m twine upload dist/*

# Version lives in pyproject.toml and src/planetarypy/__init__.py; the mapping is
# in [tool.bumpversion]. Each of these commits the change and tags it, so the
# usual release is: make bump-minor && git push --follow-tags && make release
bump-patch: ## bump the patch version, commit and tag
	$(PY) -m bumpversion bump patch

bump-minor: ## bump the minor version, commit and tag
	$(PY) -m bumpversion bump minor

bump-major: ## bump the major version, commit and tag
	$(PY) -m bumpversion bump major
