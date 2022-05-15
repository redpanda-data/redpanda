libcloud_storage = library('cloud_storage',
  'cache_service.cc',
  'access_time_tracker.cc',
  'cache_probe.cc',
  'topic_manifest.cc',
  'partition_manifest.cc',
  'recursive_directory_walker.cc',
  'remote.cc',
  'offset_translation_layer.cc',
  'probe.cc',
  'partition_probe.cc',
  'partition_recovery_manager.cc',
  'types.cc',
  'remote_segment.cc',
  'remote_partition.cc',
  'remote_segment_index.cc',
  'tx_range_manifest.cc',
  implicit_include_directories: false,
  dependencies: [seastar, config, model, s3, storage])

cloud_storage = declare_dependency(
  dependencies: [seastar, config, model, s3, storage],
  link_with: libcloud_storage)
