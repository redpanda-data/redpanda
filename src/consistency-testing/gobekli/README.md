# Linearalizability testing with Gobekli

Gobekli is a tool for testing linearalizability of systems with key-value http
interface. It generates workload and checks that observed behaviour (read and 
writes) doesn't violate consistency.

## How to test

Let the Gobekli proxy is listening on `host1:11206`, `host2:11206` and 
`host3:11206` then one may use the following command to start testing:

> gobekli mrsw --writers 2 --readers 3 \
>         --kv host1:11206 --kv host2:11206 --kv host2:11206 --duration 120

`mrsw` stands for multi reader single writer and describe the following
workload.

For each node (see kv params) it creates 2 writer and each of them is 
continiously write with cas to its key, so first writer writes to `key0` and 
second to `key1`. In total there are 3 writers who write to `key0` (using 
`host1`, `host2` and `host3`) and 3 who write to `key1`.

Also for each key and for each node there are 3 readers who continiously read 
the key. So in total there are 18 (2 keys x 3 nodes x 3 readers) readers.

The test lasts 120 secords or until first consistency violation whatever happens
first.

## HTTP storage interface

Gobekli is storage agnostic so to test a storage one should implement a Gobekli
compatible proxy and run it along the storage nodes.

### Reading a key

To read a key `key1` Gobekli issues a get request `/read?key=key1` to any node.

#### Responce

If a key exists then a client expects to receive

```json
{
    "status": "ok",
    "hasData": true,
    "writeID": "j3qq4",
    "value": "42"
}
```

`writeID` is something like a version and uniquely identifies a write that wrote
that value.

In case the key is mising the output should be

```json
{
    "status": "ok",
    "hasData": false
}
```

When a underlying storage timeouts a client gets

```json
{
    "status": "unknown"
}
```

If a storage reject a command without changing the storage's state the output
should be

```json
{
    "status": "fail"
}
```

A storage may reject a read for example when it has a notion of a leader and a
request is processed by a follower.

### Writing a key

To put or blindly overwrite a key `key1` with value `42` Gobekli issues a post
request to `/write` with the following body

```json
{
    "key": "key1",
    "writeID": "j3qq4",
    "value": "42"
}
```

`writeID` is used for consistency checking and to perform compare and set. The
storage is expected to treat it as a part of a value. For example an SQL
database may store it in an additional column, a document database as a field
and a key/value store may concat it with the value. Gobekli uses `uuid()` to
generate writeIDs.

#### Responce

When the write succeeds it returns the written value

```json
{
    "status": "ok",
    "hasData": true,
    "writeID": "j3qq4",
    "value": "42"
}
```

Just like with read requests the storage may timeout or reject a write. In those
cases the status is either `unknown` or `fail`.

### Conditional write

Gobekli depends on conditional write to do efficient consistency check. To
replace a key with a conditional on the current value's writeID it issues a post
request to `/cas` with the following body

```json
{
    "key": "key1",
    "prevWriteID": "j3qq4",
    "writeID": "h7h2v",
    "value": "43"
}
```

#### Responce

The responce is the same as with `/write` request. When the condition holds and
the write succeeds it returns the written value. If the condition doesn't hold
the proxy should return the current stored value