## nimbusdb
> WARNING: nimbusdb is in early stages of development; do not use this in production.

Persistent key-value store based on Bitcask paper.

nimbusdb is a fast, lightweight, and scalable key-value store based on Bitcask.

nimbusdb maintains an active datafile to which data is written. When it crosses a threshold, the datafile is made inactive, and a new datafile is created.
As time passes, expired or deleted keys take up space that is not useful. Hence, a process called `merge` is done to remove all expired or deleted keys and free up space.

## Features
<details>
  <summary>
  Thread-Safe
  </summary>
  All operations are thread-safe. Read and Write operations can handle multiple operations from multiple goroutines at the same time with consistency.
</details>

<details>
  <summary>
  Portable
  </summary>
  Data is extremely portable since it is only a bunch of files. All you have to do is move the folder and open an DB connection at that path.
</details>

<details>
  <summary>
  Custom Expiry
  </summary>
  Supports custom expiry for keys. Default expiry is 1 week.
</details>

<details>
  <summary>
  Supports Merge
  </summary>
  Supports `Sync` which can be called periodically to remove expired/deleted keys from disk and free-up more space.
</details>

<details>
  <summary>
  Supports Batch operations
  </summary>
  Batch operations can be performed and committed to save to disk or rollbacked to discard the batch. Operations
  cannot be performed once the batch is closed.
</details>

<details>
  <summary>
  Single disk-seek write
  </summary>
  Writes are just one disk seek since we're appending to the file.
</details>

<details>
  <summary>
  Block cache for faster reads.
  </summary>
  Blocks are cached for faster reads. Default size of an Block is 32KB.
</details>

## Usage
#### Initialize db connection
```go
d, err := nimbusdb.Open(&nimbusdb.Options{Path: "/path/to/data/directory"})
if err != nil {
  // handle error
}
defer d.Close()
```

#### Set
```go
kvPair := &nimbusdb.KeyValuePair{
  Key:   []byte("key"),
  Value: []byte("value"),
  Ttl: 5 * time.Minute, // Optional, default is 1 week
}
setValue, err := d.Set(kvPair)
if err != nil {
  // handle error
}
```

#### Get

```go
value, err := d.Get([]byte("key"))
if err != nil {
  // handle error
}
```

#### Delete

```go
value, err := d.Delete([]byte("key"))
if err != nil {
  // handle error
}
```

#### Sync
This does the merge process. This can be an expensive operation, hence it is better to run this periodically and whenever the traffic is low.

```go
err := d.Sync()
if err != nil {
  // handle error
}
```

#### Batch Operations
```go
d, err := nimbusdb.Open(&nimbusdb.Options{Path: "/path/to/data/directory"})
if err != nil {
  // handle error
}
defer d.Close()
b, err := d.NewBatch()
if err != nil {
  // handle error
}
defer b.Close()

_, err = b.Set([]byte("key"), []byte("value")) // not written to disk yet.
if err != nil {
  // handle error
}

key, err := b.Get([]byte("key"))
if err != nil {
  // handle error
}

err = b.Delete([]byte("key"))
if err != nil {
  // handle error
}

exists, err := b.Exists([]byte("key"))
if err != nil {
  // handle error
}

b.Commit() // write all pending writes to disk
b.Rollback() // discard all pending writes
```

#### Watch keys
```go
func watchKeyChange(ch chan nimbusdb.WatcherEvent) {
  for event := range ch {
    switch event.EventType {
      case "CREATE":
        // Handle create key event
        break

      case "UPDATE":
        // Handle update key event
        break

      case "DELETE":
        // Handle delete key event
        break
    }
  }
}

func main() {
  d, err := nimbusdb.Open(&nimbusdb.Options{Path: "/path/to/data/directory", ShouldWatch: true})
  if err != nil {
    // handle error
  }
  defer d.Close()
  defer d.CloseWatch() // optional
  
  watchChannel, err := d.Watch()
  if err != nil {
    // handle error
  }
  
  go watchEvents(watchChannel)

  kvPair := &nimbusdb.KeyValuePair{
    Key:   []byte("key"),
    Value: []byte("value"),
  }
  setValue, err := d.Set(kvPair) // will trigger an CREATE event
  if err != nil {
    // handle error
  }

  setValue, err := d.Set(kvPair) // will trigger an UPDATE event
  if err != nil {
    // handle error
  }

  err = d.Delete(kvPair.Key) // will trigger an DELETE event
  if err != nil {
    // handle error
  }
}
```

[Progress Board](https://trello.com/b/2eDSLLb3/nimbusdb) | [Streams](https://youtube.com/playlist?list=PLJALjJgNSDVo5veOf2apgMIE1QgN7IEfk) | [godoc](https://pkg.go.dev/github.com/manosriram/nimbusdb)

[![Go](https://github.com/manosriram/nimbusdb/actions/workflows/go.yml/badge.svg?branch=main)](https://github.com/manosriram/nimbusdb/actions/workflows/go.yml)
[![CodeFactor](https://www.codefactor.io/repository/github/manosriram/nimbusdb/badge)](https://www.codefactor.io/repository/github/manosriram/nimbusdb)
