find_package(Seastar REQUIRED)

find_package(Boost REQUIRED
  COMPONENTS
    iostreams
    unit_test_framework)
find_package(absl REQUIRED)
find_program(GO_PROGRAM go REQUIRED)
add_subdirectory(v)
add_subdirectory(transform-sdk/go/transform/internal/testdata)
