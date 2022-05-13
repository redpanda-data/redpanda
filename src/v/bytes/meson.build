libbytes = library('bytes',
  'bytes.cc',
  'iobuf.cc',
  'oncore.cc',
  dependencies: [seastar],
  implicit_include_directories: false)

bytes = declare_dependency(
  link_with: libbytes,
  dependencies: [seastar])
