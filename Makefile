run:
	@go run main/nats/main.go embedded run --manifest example.manifest.yaml

metric:
	@go run main/metric/main.go embedded run