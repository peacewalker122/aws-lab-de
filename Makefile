COMPOSE = docker compose -f infrastructure/compose.yaml
TF = terraform -chdir=infrastructure/terraform

.PHONY: doctor up down infra-init infra-plan infra-apply migrate seed ingest test test-integration athena clean

doctor:
	@command -v docker >/dev/null && docker compose version >/dev/null
	@command -v terraform >/dev/null && terraform version
	@command -v uv >/dev/null && uv --version
	@set -a; . ./.env; set +a; curl -fsS "$${AWS_ENDPOINT_URL:-http://127.0.0.1:4566}/_ministack/ready" >/dev/null
	@$(COMPOSE) exec -T postgres pg_isready -U aster -d aster
	@echo 'MiniStack and PostgreSQL are ready'

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

migrate:
	@set -a; . ./.env; set +a; PYTHONPATH=src uv run --locked python -m aster migrate

seed:
	@set -a; . ./.env; set +a; PYTHONPATH=src uv run --locked python -m aster seed --size $${ASTER_SEED_SIZE:-small} --seed $${ASTER_SEED:-42}

ingest:
	@set -a; . ./.env; set +a; PYTHONPATH=src uv run --locked python -m aster ingest

test:
	uv run --locked pytest -m 'not integration'

test-integration:
	@set -a; . ./.env; set +a; uv run --locked pytest -m integration

athena:
	@set -a; . ./.env; set +a; uv run --locked python scripts/athena_probe.py

clean:
	@echo 'Destructive: deletes PostgreSQL and MiniStack volumes and all local data. Terraform state is retained.'
	@printf 'Type DELETE to continue: '; read answer; [ "$$answer" = DELETE ]
	$(COMPOSE) down -v
