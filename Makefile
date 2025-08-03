.PHONY: start stop build run clean restart
.DEFAULT_GOAL := help

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

start: ## Start all services in detached mode
	docker compose up -d

stop: ## Stop all services
	docker compose down

build: ## Build or rebuild services (no cache)
	docker compose build --no-cache

run: ## Run main.py inside python service
	docker compose exec python python app/main.py

clean: ## Stop and remove containers, networks, and volumes
	docker compose down -v --remove-orphans

restart: ## Restart all services
	docker compose down && docker compose up -d