db:
	go run examples/db.go

clean:
	rm -rf /Users/manosriram/nimbusdb

test_clean:
	rm -rf /Users/manosriram/nimbusdb

test:
	make test_clean && go test ./tests -v

bench:
	make test_clean && cd benchmark && go test -bench=. -benchmem
