JAVA8_HOME ?= $(shell /usr/libexec/java_home -v 1.8 2>/dev/null)

.PHONY: build data up load stop-load fraud analyze status logs down clean

build:              ## compile locally on JDK 8 (Scala 2.12 does not compile on JDK 21+)
	JAVA_HOME=$(JAVA8_HOME) mvn -q -B package -DskipTests

data:               ## generate the input CSVs FraudDetectionApp reads
	python3 scripts/generate_sample_data.py

up:                 ## Elasticsearch + Kibana + OTel Collector, then Kibana data views and dashboard
	docker compose up -d --build elasticsearch kibana otel-collector
	./scripts/setup-kibana.sh

load:               ## start the simulator: Spark pipelines back to back, ~15% skewed, ~15% failing
	docker compose --profile load up -d --build traffic

stop-load:
	docker compose --profile load stop traffic

fraud: data         ## one run of the original FraudDetectionApp over ./data/*.csv
	docker compose --profile fraud run --rm --build fraud-app

analyze:            ## read ./data/*.json and join logs to spans (no Elasticsearch needed)
	docker compose --profile analyze run --rm --build analyze

status:             ## document counts per data stream
	@curl -s 'http://localhost:9200/_cat/indices/traces-*,logs-*,metrics-*?v&h=index,docs.count,store.size&s=index'

logs:               ## follow the simulator
	docker compose --profile load logs -f traffic

down:               ## stop everything and delete Elasticsearch data
	docker compose --profile load --profile fraud --profile analyze down -v

clean: down
	rm -f data/spans.json data/logs.json data/metrics.json
