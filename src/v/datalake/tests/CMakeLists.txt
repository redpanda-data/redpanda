find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
rp_test(
  FIXTURE_TEST
 BINARY_NAME arrow_writer_test
 SOURCES arrow_writer_test.cc
 LIBRARIES
   v::seastar_testing_main
   v::storage_test_utils
   v::model_test_utils
   v::datalake
   v::cluster
   Arrow::arrow_shared
   Parquet::parquet_shared
 ARGS "-- -c 1"
 LABELS datalake
)


rp_test(
    UNIT_TEST
    GTEST
    BINARY_NAME gtest_parquet_writer
    SOURCES
      arrow_writer_gtest.cc
    LIBRARIES
      Arrow::arrow_shared
      Parquet::parquet_shared
      # v::cluster
      v::datalake
      v::gtest_main
    LABELS datalake
    ARGS "-- -c 1"
  )

rp_test(
  BENCHMARK_TEST
  BINARY_NAME arrow_writer
  SOURCES arrow_writer_bench.cc
  LIBRARIES
  Arrow::arrow_shared
    Parquet::parquet_shared
    # v::cluster
    v::datalake
    Seastar::seastar_perf_testing
LABELS datalake
)