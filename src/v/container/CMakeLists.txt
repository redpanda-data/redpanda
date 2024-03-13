find_package(Roaring REQUIRED)
v_cc_library(
  NAME container
  DEPS Roaring::roaring
)

add_subdirectory(tests)
