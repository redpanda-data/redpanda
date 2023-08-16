v_cc_library(
  NAME json
  SRCS
    json.cc
  DEPS
    Seastar::seastar
    RapidJSON::rapidjson
    v::model
)

add_subdirectory(tests)