add_executable(ai_demo demo.cc)
target_link_libraries(ai_demo PUBLIC v::ai)
set_property(TARGET ai_demo PROPERTY POSITION_INDEPENDENT_CODE ON)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME ai_smoke_test
  SOURCES
    smoke_test.cc
  LIBRARIES 
    v::gtest_main
    v::ai
  ARGS "-- -c 1"
  LABELS wasm
)
