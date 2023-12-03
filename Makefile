db:
	go run examples/db.go

build:
	go build -v

clean:
	rm -rf tests/nimbusdb* benchmark/nimbusdb*

test:
	go test ./tests -v
.PHONY: all test

bench:
	cd benchmark && go test -bench=. -benchmem
