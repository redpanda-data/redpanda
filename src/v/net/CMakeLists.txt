v_cc_library(
  NAME net
  SRCS
    batched_output_stream.cc
    connection_rate.cc
    dns.cc
    transport.cc
    connection.cc
    server.cc
    probes.cc
    tls.cc
  DEPS
    Seastar::seastar
    absl::node_hash_map
    v::config
  )

add_subdirectory(tests)
