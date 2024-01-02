db:
	go run examples/db/db.go

batch:
	go run examples/batch/batch.go

build:
	go build -v

clean:
	rm -rf tests/nimbusdb* benchmark/nimbusdb*

test:
	go test ./tests -v
.PHONY: all test

testrace:
	go test ./tests -v --race

bench:
	cd benchmark && go test -bench=. -benchmem
