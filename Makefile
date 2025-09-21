PROJECT?=tubewhale
COMPOSE?=docker-compose.yml

DC=docker compose -f $(COMPOSE)
BACKEND=backend

.PHONY: build up down logs shell migrate superuser seed collectstatic restart ps wipe

build:
	$(DC) build

up:
	$(DC) up -d postgres redis
	$(DC) up -d $(BACKEND) celery_worker celery_beat
	$(DC) ps

down:
	$(DC) down

logs:
	$(DC) logs -f $(BACKEND)

shell:
	$(DC) exec $(BACKEND) bash || $(DC) exec $(BACKEND) /bin/sh

migrate:
	$(DC) exec $(BACKEND) python manage.py migrate

superuser:
	$(DC) exec $(BACKEND) python manage.py createsuperuser

seed:
	$(DC) exec $(BACKEND) python manage.py seed_templates_and_prompts --apply

collectstatic:
	$(DC) exec $(BACKEND) python manage.py collectstatic --noinput

restart:
	$(DC) restart $(BACKEND)

ps:
	$(DC) ps

wipe: ## Dangerous: remove volumes
	$(DC) down -v