coproc_supervisor_service_h = custom_target(
  output: 'supervisor.h',
  input: 'gen.json',
  command: [rpc_compiler, '--service_file', '@INPUT@',
    '--output_file', '@OUTPUT@'])

libcoproc = library('coproc',
  'api.cc',
  'types.cc',
  'logger.cc',
  'script_context.cc',
  'script_context_frontend.cc',
  'script_context_backend.cc',
  'script_context_router.cc',
  'pacemaker.cc',
  'offset_storage_utils.cc',
  'wasm_event.cc',
  'event_listener.cc',
  'script_dispatcher.cc',
  'event_handler.cc',
  'partition.cc',
  'partition_manager.cc',
  'reconciliation_backend.cc',
  coproc_supervisor_service_h,
  implicit_include_directories: false,
  dependencies: [cluster, kafka_client])

coproc = declare_dependency(
  sources: coproc_supervisor_service_h,
  dependencies: [cluster, kafka_client],
  link_with: libcoproc)
