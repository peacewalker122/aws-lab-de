COMPOSE = docker compose -f infrastructure/compose.yaml --env-file .env
TF = terraform -chdir=infrastructure/terraform
RUN_ID ?= $(shell date -u +%Y%m%dT%H%M%SZ)
RUN = --run-id $(RUN_ID)
UVENV = PYTHONPATH=src uv run --locked

.PHONY: doctor up down infra-init infra-plan infra-apply migrate seed ingest transform pipeline query validate test test-integration athena airflow-build airflow-up airflow-down airflow-run test-airflow monitoring-up monitoring-down monitoring-check test-observability test-failure lab-up clean

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
	@set -a; . ./.env; set +a; $(UVENV) python -m aster migrate

seed:
	@set -a; . ./.env; set +a; $(UVENV) python -m aster seed --size $${ASTER_SEED_SIZE:-small} --seed $${ASTER_SEED:-42}

ingest:
	@set -a; . ./.env; set +a; $(UVENV) python -m aster $(RUN) extract

transform:
	@set -a; . ./.env; set +a; $(UVENV) python -m aster $(RUN) transform-silver
	@set -a; . ./.env; set +a; $(UVENV) python -m aster $(RUN) validate-silver
	@set -a; . ./.env; set +a; $(UVENV) python -m aster $(RUN) build-gold
	@set -a; . ./.env; set +a; $(UVENV) python -m aster $(RUN) validate-gold

pipeline:
	@set -a; . ./.env; set +a; $(UVENV) python -m aster $(RUN) pipeline

validate:
	@set -a; . ./.env; set +a; $(UVENV) python -m aster $(RUN) validate-raw

query:
	@set -a; . ./.env; set +a; $(UVENV) python -m aster query --format table

test:
	uv run --locked pytest -m 'not integration'

test-integration:
	@set -a; . ./.env; set +a; uv run --locked pytest -m integration

athena:
	@set -a; . ./.env; set +a; uv run --locked python scripts/athena_probe.py

monitoring-up:
	$(COMPOSE) --profile observability up -d

monitoring-down:
	-$(COMPOSE) --profile observability stop loki tempo prometheus pushgateway grafana

monitoring-check:
	@set -e; for url in http://127.0.0.1:3100/ready http://127.0.0.1:3200/ready http://127.0.0.1:19090/-/ready http://127.0.0.1:9091/-/ready http://127.0.0.1:3000/api/health; do code=$$(curl -sS -m 5 -o /dev/null -w '%{http_code}' $$url); echo "$$url -> $$code"; [ "$$code" = 200 ]; done

test-observability:
	@set -a; . ./.env; set +a; uv run --locked pytest -m observability

test-failure:
	@set -a; . ./.env; set +a; uv run --locked pytest -m failure

lab-up: up monitoring-up airflow-up

airflow-build:
	docker build -f infrastructure/airflow/Dockerfile -t localhost/aster-airflow:3.1.0 .

airflow-up: airflow-build
	$(COMPOSE) --profile airflow run --rm airflow-init
	$(COMPOSE) --profile airflow up -d airflow-api-server airflow-scheduler airflow-dag-processor

airflow-down:
	-$(COMPOSE) --profile airflow stop airflow-api-server airflow-scheduler airflow-dag-processor

airflow-run:
	@$(COMPOSE) --profile airflow exec -T airflow-scheduler bash -c 'export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://aster:$$POSTGRES_PASSWORD@postgres:5432/airflow"; airflow dags test aster_pipeline $${AIRFLOW_LOGICAL_DATE:-2025-01-01}'

test-airflow:
	@set -a; . ./.env; set +a; uv run --locked pytest -m airflow

clean:
	@echo 'Destructive: deletes PostgreSQL and MiniStack volumes and all local data. Terraform state is retained.'
	@printf 'Type DELETE to continue: '; read answer; [ "$$answer" = DELETE ]
	$(COMPOSE) down -v
