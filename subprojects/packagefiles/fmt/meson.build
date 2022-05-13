project('fmt', 'cpp')

fmt_private_cpp_args = []
fmt_interface_cpp_args = []
if get_option('default_library') == 'shared'
  fmt_private_cpp_args += ['-DFMT_EXPORT']
  fmt_interface_cpp_args += ['-DFMT_SHARED']
endif

fmt_inc = include_directories('include')
fmt_lib = library('fmt',
  'src/format.cc',
  'src/os.cc',
  cpp_args: fmt_private_cpp_args,
  include_directories: fmt_inc
)

fmt_dep = declare_dependency(
  include_directories: fmt_inc,
  compile_args: fmt_interface_cpp_args,
  link_with: fmt_lib
)
