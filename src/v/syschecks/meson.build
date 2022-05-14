libsyschecks = library('syschecks',
  'syschecks.cc',
  'pidfile.cc',
  version_h,
  implicit_include_directories: false,
  dependencies: [seastar, common])

syschecks = declare_dependency(
  dependencies: [seastar, common],
  link_with: libsyschecks)
