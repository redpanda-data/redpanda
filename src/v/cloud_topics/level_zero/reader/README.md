### L0 read path

The read request is first added to the `read_pipeline` component. The `resolver` component fetches
read requests and resolves them by creating the reader that returns materialized record batches.

The record batches are either fetched from the cloud storage or cache (either in-memory or disk).
Current implementation is not doing any caching or multi-step processing. Instead, it just creates a
reader object and resolves the read request using this reader.

The resolver is gated by the requested offset range. If the offset range is above the last reconciled
offset or it contains last reconciled offset the resolver is invoked. Otherwise L1 resolver should be
invoked.