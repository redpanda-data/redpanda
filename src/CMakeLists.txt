find_package(Seastar REQUIRED)

# normalize the name of the gnutls dependency. when seastar is imported via a
# normal installation arrange for the library name given by seastar's find
# module to match the name from the system cmake find module for gnutls.
if (TARGET GnuTLS::gnutls)
  add_library(GnuTLS::GnuTLS ALIAS GnuTLS::gnutls)
else()
  find_package(GnuTLS)
endif()

find_package(Boost REQUIRED
  COMPONENTS
    iostreams
    unit_test_framework)
find_package(absl REQUIRED)
add_subdirectory(v)
add_subdirectory(go/kreq-gen)
add_subdirectory(go/transform-sdk/internal/testdata)
