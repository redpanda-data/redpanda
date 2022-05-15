raft_service_h = custom_target(
  output: 'raftgen_service.h',
  input: 'raftgen.json',
  command: [rpc_compiler, '--service_file', '@INPUT@',
    '--output_file', '@OUTPUT@'])

libraft = library('raft',
  'consensus.cc',
  'consensus_utils.cc',
  'heartbeat_manager.cc',
  'configuration_bootstrap_state.cc',
  'logger.cc',
  'types.cc',
  'replicate_entries_stm.cc',
  'vote_stm.cc',
  'prevote_stm.cc',
  'recovery_stm.cc',
  'follower_stats.cc',
  'replicate_batcher.cc',
  'rpc_client_protocol.cc',
  'group_manager.cc',
  'probe.cc',
  'offset_monitor.cc',
  'event_manager.cc',
  'state_machine.cc',
  'log_eviction_stm.cc',
  'configuration_manager.cc',
  'group_configuration.cc',
  'append_entries_buffer.cc',
  'follower_queue.cc',
  'offset_translator.cc',
  'recovery_memory_quota.cc',
  raft_service_h,
  implicit_include_directories: false,
  dependencies: [model, utils, storage, rpc, finjector])

raft = declare_dependency(
  sources: raft_service_h,
  dependencies: [model, utils, storage, rpc, finjector],
  link_with: libraft)

t = executable('test_consensus_utils',
  'tests/consensus_utils_test.cc',
  dependencies: [boost, raft, model_test_utils])
test('consensus utils', t)

t = executable('test_group_configuration',
  'tests/group_configuration_tests.cc',
  dependencies: [boost, raft])
test('group configuration', t)

t = executable('test_raft',
  'tests/jitter_tests.cc',
  'tests/bootstrap_configuration_test.cc',
  'tests/foreign_entry_test.cc',
  'tests/configuration_serialization_test.cc',
  'tests/type_serialization_tests.cc',
  'tests/term_assigning_reader_test.cc',
  'tests/membership_test.cc',
  'tests/leadership_test.cc',
  'tests/append_entries_test.cc',
  'tests/offset_monitor_test.cc',
  'tests/mux_state_machine_test.cc',
  'tests/mutex_buffer_test.cc',
  'tests/manual_log_deletion_test.cc',
  'tests/state_removal_test.cc',
  'tests/configuration_manager_test.cc',
  dependencies: [raft, seastar_testing_main, model_test_utils,
    storage_test_utils, features])
test('raft', t)

t = executable('test_offset_translator',
  'tests/offset_translator_tests.cc',
  dependencies: [raft, seastar_testing_main])
test('offset translator', t)

executable('demo_kvelldb',
  'kvelldb/kvserver.cc',
  'kvelldb/logger.cc',
  'kvelldb/kvrsm.cc',
  'kvelldb/httpkvrsm.cc',
  dependencies: [raft, features])
