.PHONY: run
run:
	docker-compose up --build --force-recreate --detach

.PHONY: stop
stop:
	docker-compose stop

.PHONY: clean
clean:
	docker-compose down --volumes

.PHONY: shell
shell:
	docker-compose exec dbt bash

.PHONY: logs
logs:
	docker-compose logs --tail=500 -f

.PHONY: status
status:
	docker-compose ps

.PHONY: lint
lint:
	pre-commit run --all-files
