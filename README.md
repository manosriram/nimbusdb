## nimbusdb
Persistent Key-Value store based on Bitcask paper.

nimbusdb is a fast, lightweight, and scalable key-value store written in golang, based on bitcask.

nimbusdb maintains an active datafile to which data is written. When it crosses a threshold, the datafile is made inactive and new datafile is created.
As time passes, expired/deleted keys take up space which is not useful; Hence, a process called `merge` is done which removes all expired/deleted keys and frees up space.

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

## Documentation
#### Open DB connection
```go
d, err := nimbusdb.Open(&nimbusdb.Options{Path: "/path/to/data/directory"})
if err != nil {
  // handle error
}
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


[Progress Board](https://trello.com/b/2eDSLLb3/nimbusdb) | [Streams](https://youtube.com/playlist?list=PLJALjJgNSDVo5veOf2apgMIE1QgN7IEfk) | [godoc](https://pkg.go.dev/github.com/manosriram/nimbusdb)

[![Go](https://github.com/manosriram/nimbusdb/actions/workflows/go.yml/badge.svg?branch=main)](https://github.com/manosriram/nimbusdb/actions/workflows/go.yml)
