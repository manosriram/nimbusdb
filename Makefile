db:
	go run examples/db/db.go

batch:
	go run examples/batch/batch.go

build:
	go build -v

clean:
	rm -rf ./nimbusdb_temp* benchmark/nimbusdb_temp*
	rm -rf ~/nimbusdb/test_data
	rm -rf ./dd

test:
	go test -v -failfast
.PHONY: all test

test-race:
	go test -v -failfast --race

bench:
	cd benchmark && go test -bench=. -benchmem
