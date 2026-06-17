RIE = http://localhost:9000/2015-03-31/functions/function/invocations

.PHONY: start-local stop-local logs invoke-standard invoke-fifo invoke-invalid test test-integration

start-local:
	docker compose -f local/docker-compose.yml up -d --build

stop-local:
	docker compose -f local/docker-compose.yml down

logs:
	docker logs -f fastsqs-lambda

invoke-standard:
	curl -s "$(RIE)" -d @tests/events/sqs_standard_batch.json; echo

invoke-fifo:
	curl -s "$(RIE)" -d @tests/events/sqs_fifo_batch.json; echo

invoke-invalid:
	curl -s "$(RIE)" -d @tests/events/sqs_invalid_body.json; echo

test:
	pytest

test-integration:
	pytest --run-integration tests/integration
