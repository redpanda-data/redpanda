find_program(KAFKA_CODEGEN_VENV "kafka-codegen-venv")

set(schemata
  api_versions_request.json
  api_versions_response.json
  offset_fetch_request.json
  offset_fetch_response.json
  offset_commit_request.json
  offset_commit_response.json
  join_group_request.json
  join_group_response.json
  sync_group_request.json
  sync_group_response.json
  heartbeat_request.json
  heartbeat_response.json
  leave_group_request.json
  leave_group_response.json
  delete_topics_request.json
  delete_topics_response.json
  describe_configs_request.json
  describe_configs_response.json
  find_coordinator_request.json
  find_coordinator_response.json
  list_offset_request.json
  list_offset_response.json
  alter_configs_request.json
  alter_configs_response.json
  list_groups_request.json
  list_groups_response.json
  describe_groups_request.json
  describe_groups_response.json
  create_topics_request.json
  create_topics_response.json
  sasl_handshake_request.json
  sasl_handshake_response.json
  sasl_authenticate_request.json
  sasl_authenticate_response.json
  init_producer_id_request.json
  init_producer_id_response.json
  incremental_alter_configs_request.json
  incremental_alter_configs_response.json
  delete_groups_request.json
  delete_groups_response.json
  describe_acls_request.json
  describe_acls_response.json
  describe_log_dirs_request.json
  describe_log_dirs_response.json
  create_acls_request.json
  create_acls_response.json
  delete_acls_request.json
  delete_acls_response.json
  produce_request.json
  produce_response.json
  metadata_request.json
  metadata_response.json
  add_partitions_to_txn_request.json
  add_partitions_to_txn_response.json
  txn_offset_commit_request.json
  txn_offset_commit_response.json
  fetch_request.json
  fetch_response.json
  end_txn_request.json
  end_txn_response.json  
  create_partitions_request.json
  create_partitions_response.json
  add_offsets_to_txn_request.json
  add_offsets_to_txn_response.json
  offset_for_leader_epoch_request.json
  offset_for_leader_epoch_response.json)

set(srcs)
foreach(schema ${schemata})
  get_filename_component(msg_name ${schema} NAME_WE)
  set(hdr "${CMAKE_CURRENT_BINARY_DIR}/${msg_name}.h")
  set(src "${CMAKE_CURRENT_BINARY_DIR}/${msg_name}.cc")
  list(APPEND srcs ${hdr})
  list(APPEND srcs ${src})
  add_custom_command(
    OUTPUT ${hdr} ${src}
    COMMAND ${KAFKA_CODEGEN_VENV} ${CMAKE_CURRENT_SOURCE_DIR}/generator.py
    ARGS ${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/${schema}
    DEPENDS ${schema} ${CMAKE_CURRENT_SOURCE_DIR}/generator.py ${KAFKA_CODEGEN_VENV}
    COMMENT "Running kafka request codegen on ${schema}"
    VERBATIM)
endforeach()

v_cc_library(
  NAME kafka_request_schemata
  SRCS
    ${srcs}
  COPTS
    "-Wno-unused-lambda-capture"
  DEPS
    Seastar::seastar
    v::bytes
    v::rpc
    v::kafka
    absl::flat_hash_map
    absl::flat_hash_set
)
