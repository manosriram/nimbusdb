db:
	go run examples/db.go

clean:
	rm -rf /Users/manosriram/nimbusdb/test/

test_clean:
	rm -rf /Users/manosriram/nimbusdb/test_data/

test:
	go test ./tests -v
.PHONY: all test

bench:
	make test_clean && cd benchmark && go test -bench=. -benchmem
