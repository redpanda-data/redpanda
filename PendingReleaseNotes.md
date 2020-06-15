* Raft voted-for persistent metadata is now stored in a per-core key-value store
  which avoids a I/O flush storm when many raft groups vote at the same time.
  This is important for deployments where the underlying I/O system may force
  limitations on iops.
