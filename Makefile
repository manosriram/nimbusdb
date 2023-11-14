db:
	go run examples/db.go

clean:
	rm -f data/*

test:
	go test ./tests -v
