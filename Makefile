db:
	go run examples/db.go

clean:
	rm -f data/*

test_clean:
	rm -f test_data/* concurrent_test_data/*

test:
	make test_clean && go test ./tests -v

bench:
	make test_clean && cd benchmark && go test -bench=. -benchmem
