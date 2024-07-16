.DEFAULT_GOAL := print-docs
SHELL := /bin/bash

################################################################################
# RUN TESTS, LINTING & TYPING
################################################################################

.PHONY: test
test: ## Run tests
	@echo "Running tests"
		sh -c "poetry run pytest $(ARGS)"

.PHONY: lint
lint: ## Run linting checks
	@echo "Running linting checks..."
	@poetry run pre-commit run --all-files

.PHONY: typing
typing: ## Run typing checks
	@echo "Running typing checks..."
	@poetry run mypy ./

################################################################################
# UTILS
################################################################################

# See https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: print-docs
print-docs:  ## Pretty print this Makefile help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-35s\033[0m %s\n", $$1, $$2}'
