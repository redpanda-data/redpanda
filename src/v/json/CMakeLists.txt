v_cc_library(
  NAME json
  SRCS
    json.cc
  DEPS
    Seastar::seastar
    RapidJSON::rapidjson
    v::rphashing
)

add_subdirectory(tests)
