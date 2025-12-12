.PHONY: install test run docker

target: dependencies
	commands

## Dockerization and Deployment
compose-build:
	docker compose build

compose-up:
	docker compose up

compose-down:
	docker compose down --volumes --remove-orphans

compose-rebuild:
	docker compose up --build --force-recreate --remove-orphans


# Install dependencies in a virtual environment
install:
	python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt

# Build a Docker image
docker:
	docker build -t dc_project .

clean-cache:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +

clean:
	rm -r data

test:
	pytest -v



help:
	@echo "Makefile commands:"
	@echo "  compose-build   - Build all Docker Compose services"
	@echo "  compose-up      - Start Docker Compose services"
	@echo "  compose-down    - Stop and remove Docker Compose services, volumes, and orphans"
	@echo "  compose-rebuild - Rebuild and start Docker Compose services, force recreate, remove orphans"
	@echo "  install     - Set up a Python virtual environment and install dependencies from requirements.txt"
	@echo "  test        - Run all tests using pytest with verbose output"
	@echo "  docker      - Build a Docker image for the project"
	@echo "  clean-cache - Remove __pycache__ and pytest cache directories"
	@echo "  clean       - Remove generated data directories"
