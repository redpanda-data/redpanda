libfinjector = library('finjector',
  'hbadger.cc',
  dependencies: [seastar, absl],
  implicit_include_directories: false)

finjector = declare_dependency(
  dependencies: [seastar, absl],
  link_with: libfinjector)
