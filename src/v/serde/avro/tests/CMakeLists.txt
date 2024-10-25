find_package(Avro)

set(testdata_dir "${CMAKE_CURRENT_SOURCE_DIR}/testdata")

v_cc_library(
  NAME avro_test_utils
  HDRS
    data_generator.h
  SRCS
    data_generator.cc
  DEPS
    Avro::avro
    v::random
)

rp_test(
  UNIT_TEST
  GTEST
  USE_CWD
  BINARY_NAME serde_avro
  SOURCES
    parser_test.cc
  LIBRARIES
    Avro::avro
    Boost::iostreams
    v::avro_test_utils
    v::bytes
    v::gtest_main
    v::serde_avro
    v::utils
  INPUT_FILES
        ${testdata_dir}/array
        ${testdata_dir}/big_union
        ${testdata_dir}/bigrecord
        ${testdata_dir}/bigrecord2
        ${testdata_dir}/bigrecord_r
        ${testdata_dir}/circulardep
        ${testdata_dir}/cpp_reserved_words
        ${testdata_dir}/cpp_reserved_words_union_typedef
        ${testdata_dir}/crossref
        ${testdata_dir}/empty_record
        ${testdata_dir}/enum
        ${testdata_dir}/fixed
        ${testdata_dir}/int
        ${testdata_dir}/large_schema.avsc
        ${testdata_dir}/map
        ${testdata_dir}/padded_record
        ${testdata_dir}/primitivetypes
        ${testdata_dir}/recinrec
        ${testdata_dir}/record
        ${testdata_dir}/record2
        ${testdata_dir}/reuse
        ${testdata_dir}/tree1
        ${testdata_dir}/tree2
        ${testdata_dir}/tweet
        ${testdata_dir}/union
        ${testdata_dir}/union_array_union
        ${testdata_dir}/union_conflict
        ${testdata_dir}/union_empty_record
        ${testdata_dir}/union_map_union
        ${testdata_dir}/union_redundant_types
        ${testdata_dir}/unionwithmap
  ARGS "-- -c1"
)
