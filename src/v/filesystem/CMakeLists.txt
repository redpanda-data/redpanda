include(smfc_generator)
smfc_gen(
    CPP
    TARGET_NAME wal_gened
    OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    SOURCES wal.fbs
    )
set(rpfs_sources
  ${wal_gened}
  wal_writer_node.cc
  write_ahead_log.cc
  wal_requests.cc
  wal_reader_node.cc
  wal_name_extractor_utils.cc
  wal_writer_utils.cc
  wal_pretty_print_utils.cc
  wal_opts.cc
  wal_segment.cc
  wal_nstpidx_manager.cc
  wal_core_mapping.cc
  wal_topics_manager.cc
  wal_segment_record.cc
  wal_segment_indexer.cc
  wal_nstpidx.cc
  wal_cold_boot.cc
  page_cache.cc
  wal_disk_pager.cc
  page_cache_file_idx.cc
  wal_nstpidx_repair.cc
  )
rp_hbadger_lib(
  STATIC
  NAME rpfs
  SOURCES ${rpfs_sources}
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
  INCLUDES ${PROJECT_SOURCE_DIR}/src/v
  COMPILE_OPTIONS 
    -Wall 
    -Werror 
    $<$<CXX_COMPILER_ID:Gnu>:-fconcepts> 
    -Wno-unused-variable
    -Wno-unused-function
    -Wno-unused-lambda-capture
  LIBRARIES ioutil rprandom config
  )



# Testing utils for filesystem
add_subdirectory(tests)
