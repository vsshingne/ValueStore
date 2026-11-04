.PHONY: test race build integration demo
test:
	go test ./...
race:
	go test -race ./...
build:
	go build ./cmd/...
integration:
	go test -tags=integration ./tests/integration -v -count=1
demo:
	python scripts/demo.py
