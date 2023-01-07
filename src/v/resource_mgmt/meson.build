libresource_mgmt = library('resource_mgmt',
  'available_memory.cc',
  'storage.cc',
  implicit_include_directories: false,
  dependencies: [seastar, ssx, config])

resource_mgmt = declare_dependency(
  dependencies: [seastar, ssx, config],
  link_with: libresource_mgmt)
