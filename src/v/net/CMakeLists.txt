v_cc_library(
  NAME net
  SRCS
    batched_output_stream.cc
    dns.cc
    transport.cc
    connection.cc
    conn_quota.cc
    server.cc
    probes.cc
    tls.cc
  DEPS
    Seastar::seastar
    absl::node_hash_map
    v::config
    v::metrics
    v::hashing
    v::compression
  )

add_subdirectory(tests)
