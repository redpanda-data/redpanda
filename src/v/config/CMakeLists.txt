v_cc_library(
  NAME config
  SRCS
    broker_authn_endpoint.cc
    configuration.cc
    node_config.cc
    base_property.cc
    rjson_serialization.cc
    validators.cc
  DEPS
    v::json
    v::model
    v::security
   boost_filesystem
   absl::node_hash_set
)
add_subdirectory(tests)
