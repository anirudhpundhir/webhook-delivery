TEST_DB ?= jdbc:postgresql://localhost:5432/webhooks_test

.PHONY: build test bench run clean

build:          ## Build the executable jar (skips tests)
	mvn -B -DskipTests package

test:           ## Unit + integration tests against $(TEST_DB)
	WEBHOOK_TEST_DATABASE_URL="$(TEST_DB)" mvn -B test

bench:          ## End-to-end dispatch throughput (prints BENCH lines)
	WEBHOOK_TEST_DATABASE_URL="$(TEST_DB)" mvn -B test -Dtest=DispatchBenchmark -Dexcluded.groups=none | grep BENCH

run: build      ## Run against DATABASE_URL (default jdbc:postgresql://localhost:5432/webhooks)
	java -jar target/webhook-delivery-0.1.0.jar

clean:
	mvn -B clean
