libpandaproxy_parsing = library('pandaproxy_parsing',
  'error.cc',
  implicit_include_directories: false,
  dependencies: [utils])

pandaproxy_parsing = declare_dependency(
  dependencies: [utils],
  link_with: libpandaproxy_parsing)

t = executable('test_pp_parsing',
  'test/from_chars.cc',
  'test/httpd.cc',
  dependencies: [boost, pandaproxy_parsing])
test('panda proxy parsing', t)
