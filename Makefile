.PHONY: lint
lint:
	docker compose run --rm --entrypoint=pylint test /src/transto

.PHONY: typecheck
typecheck:
	docker compose run --rm test --mypy /src/transto

.PHONY: dist
dist:
	pip install wheel build
	python -m build
