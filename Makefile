.PHONY: setup infra-init infra-apply pipeline-run dbt-run dbt-test all

# Setup
setup:
	cp .env.example .env
	pip install uv
	uv venv .venv
	uv pip install -r requirements.txt

# Terraform
infra-init:
	cd terraform && terraform init

infra-apply:
	cd terraform && terraform apply

infra-destroy:
	cd terraform && terraform destroy

# Pipeline
bulk-load:
	python pipeline/bulk_load.py

pipeline-run:
	python pipeline/dag.py

# dbt
dbt-run:
	cd transforms && dbt run

dbt-test:
	cd transforms && dbt test

dbt-all: dbt-run dbt-test

# Full pipeline
all: infra-apply bulk-load dbt-all