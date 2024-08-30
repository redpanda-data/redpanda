find_package(Roaring REQUIRED)
v_cc_library(
  NAME storage
  SRCS
    segment_reader.cc
    segment_deduplication_utils.cc
    log_manager.cc
    disk_log_impl.cc
    disk_log_appender.cc
    parser.cc
    log_reader.cc
    log_replayer.cc
    offset_translator_state.cc
    probe.cc
    record_batch_builder.cc
    logger.cc
    segment_appender.cc
    segment_set.cc
    segment.cc
    segment_index.cc
    record_batch_utils.cc
    storage_resources.cc
    batch_cache.cc
    index_state.cc
    lock_manager.cc
    types.cc
    spill_key_index.cc
    compacted_index_chunk_reader.cc
    snapshot.cc
    kvstore.cc
    segment_utils.cc
    compaction_reducers.cc
    parser_utils.cc
    readers_cache.cc
    backlog_controller.cc
    compaction_controller.cc
    offset_to_filepos.cc
    offset_translator.cc
    fs_utils.cc
    file_sanitizer_types.cc
    api.cc
    node.cc
    key_offset_map.cc
    chunk_cache.cc
  DEPS
    Seastar::seastar
    v::bytes
    v::config
    v::metrics
    v::model
    v::hashing
    v::finjector
    v::syschecks
    v::compression
    v::random
    v::resource_mgmt
    absl::flat_hash_map
    absl::btree
    Roaring::roaring
  PUBLIC
)

add_subdirectory(mvlog)
add_subdirectory(tests)
add_subdirectory(opfuzz)

set_source_files_properties(
    disk_log_impl.cc # too big => negative impact on minimal rebuilds for unity
    segment.cc # too big => negative impact on minimal rebuilds for unity
  PROPERTIES SKIP_UNITY_BUILD_INCLUSION 1)
