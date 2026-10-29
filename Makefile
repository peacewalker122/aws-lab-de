COMPOSE = docker compose -f infrastructure/compose.yaml
TF = terraform -chdir=infrastructure/terraform

.PHONY: doctor up down infra-init infra-plan infra-apply test test-integration athena clean

doctor:
	@command -v docker >/dev/null && docker compose version >/dev/null
	@command -v terraform >/dev/null && terraform version
	@command -v uv >/dev/null && uv --version
	@curl -fsS http://127.0.0.1:4566/_ministack/health
	@echo

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

infra-init:
	$(TF) init

infra-plan:
	$(TF) plan

infra-apply:
	$(TF) apply

test:
	uv run --locked pytest -m 'not integration'

test-integration:
	uv run --locked pytest -m integration

athena:
	uv run --locked python scripts/athena_probe.py

clean:
	@echo 'Destructive: deletes MiniStack volumes and all local emulator data. Terraform state is retained.'
	@printf 'Type DELETE to continue: '; read answer; [ "$$answer" = DELETE ]
	$(COMPOSE) down -v
