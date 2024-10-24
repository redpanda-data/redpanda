# Golang Verification Programs

Sometimes it's useful for tests to verify behavior against another library implementation (kafka protocol, serialization format, etc).
These golang programs are used in verification of Redpanda, and all share a single `go.mod` to limit the sprawl of `go.mod` files in
Redpanda.
