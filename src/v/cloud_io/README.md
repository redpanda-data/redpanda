# Cloud IO

Encapsulates IO related to remote storage. Callers should interact with this
code via generic interfaces, like streams, files, and buffers, building logical
abstractions on top (e.g. partition manifests and friends in `/cloud_storage`)
and hiding the details of per-backend client connections (e.g. in
`/cloud_storage_clients`).

### Why does this exist?

As new cloud storage applications are developed, lumping everything into
`/cloud_storage` won't be sustainable, as this would encourage monolithic
classes that are more easily maintained split up.

### What code should go here?

A high level goal is to move re-usable code into `/cloud_io` and leave
abstractions specific to tiered storage in `/cloud_storage`. Caches of bytes or
cached files belong here; caches for segments or batches do not.
