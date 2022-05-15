libarchival = library('archival',
  'archival_policy.cc',
  'ntp_archiver_service.cc',
  'probe.cc',
  'types.cc',
  'upload_controller.cc',
  'segment_reupload.cc',
  'retention_calculator.cc',
  'upload_housekeeping_service.cc',
  'adjacent_segment_merger.cc',
  implicit_include_directories: false,
  dependencies: [cloud_storage_clients, storage])

archival = declare_dependency(
  dependencies: [cloud_storage_clients, storage],
  link_with: libarchival)
