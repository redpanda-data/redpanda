find_package(Seastar REQUIRED)
find_package(Boost REQUIRED
  COMPONENTS
    unit_test_framework)
find_package(absl REQUIRED)
add_subdirectory(v)
