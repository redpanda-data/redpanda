The `io` module implements a page-based write-back cache.

### `cache`

S3-FIFO cache eviction algorithm.

### `io_queue`

Per-file I/O request submission queue and scheduler.

### `scheduler`

High-level scheduling across I/O queues.

### `persistence`

Abstract storage interface with disk and memory backends.

### `pager`

Abstraction of an append-only file.
