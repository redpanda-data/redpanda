libv8_engine = library('v8_engine',
  'data_policy.cc',
  'data_policy_table.cc',
  implicit_include_directories: false,
  dependencies: [model])

v8_engine = declare_dependency(
  dependencies: [model],
  link_with: libv8_engine)
