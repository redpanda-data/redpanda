
v_cc_library(
  NAME cloud_storage
  SRCS
    cache_service.cc
    access_time_tracker.cc
    cache_probe.cc
    topic_manifest.cc
    partition_manifest.cc
    recursive_directory_walker.cc
    remote.cc
    offset_translation_layer.cc
    probe.cc
    partition_probe.cc
    types.cc
    remote_segment.cc
    remote_partition.cc
    remote_segment_index.cc
    tx_range_manifest.cc
    materialized_segments.cc
    segment_state.cc
    recovery_errors.cc
    recovery_request.cc
    recovery_utils.cc
    segment_meta_cstore.cc
  DEPS
    Seastar::seastar
    v::bytes
    v::http
    v::cloud_storage_clients
    v::json
    v::model
    v::rphashing
    v::cloud_roles
    # NOTE: do not add v::cloud as a dependency
)
add_subdirectory(tests)
