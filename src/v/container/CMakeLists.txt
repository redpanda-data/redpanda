find_package(Roaring REQUIRED)
find_package(unordered_dense REQUIRED)

v_cc_library(
  NAME container
  DEPS
    Roaring::roaring
    unordered_dense::unordered_dense
)

add_subdirectory(tests)
