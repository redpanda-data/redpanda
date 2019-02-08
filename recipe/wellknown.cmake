# Useful definitions for `cmake -E env`.
set (amended_PATH PATH=${Cooking_INGREDIENTS_DIR}/bin:$ENV{PATH})
set (PKG_CONFIG_PATH PKG_CONFIG_PATH=${Cooking_INGREDIENTS_DIR}/lib/pkgconfig)
set (autotools_ingredients_flags
  CFLAGS=-I${Cooking_INGREDIENTS_DIR}/include
  CXXFLAGS=-I${Cooking_INGREDIENTS_DIR}/include
  LDFLAGS=-L${Cooking_INGREDIENTS_DIR}/lib)
set (info_dir --infodir=<INSTALL_DIR>/share/info)
cmake_host_system_information (
  RESULT build_concurrency_factor
  QUERY NUMBER_OF_LOGICAL_CORES)
set (make_command make -j ${build_concurrency_factor})


# public api
cooking_ingredient(re2
  CMAKE_ARGS
    -DRE2_BUILD_TESTING=OF
  EXTERNAL_PROJECT_ARGS
    URL https://github.com/google/re2/archive/30cad26.zip
    URL_MD5 26742d17080a64276974e35634729e22)

cooking_ingredient(GSL
  CMAKE_ARGS
    -DGSL_TEST=OFF
  EXTERNAL_PROJECT_ARGS
    URL https://github.com/Microsoft/GSL/archive/0f68d13.tar.gz
    URL_MD5 d5cc382eadc5f757416d44d91d0222cf)

cooking_ingredient(luajit
  EXTERNAL_PROJECT_ARGS
    URL https://github.com/LuaJIT/LuaJIT/archive/v2.1.0-beta3.tar.gz
    URL_MD5 9277838df4f8d50f2aa439fdcc7eae7b
    CONFIGURE_COMMAND <DISABLE>
    BUILD_IN_SOURCE ON
    BUILD_COMMAND <DISABLE>
    INSTALL_COMMAND ${make_command} PREFIX=<INSTALL_DIR> install)

cooking_ingredient(sol2
  REQUIRES luajit
  CMAKE_ARGS
    -DSINGLE=ON
    -DLUA_VERSION="luajit-2.1"
  EXTERNAL_PROJECT_ARGS
    URL https://github.com/ThePhD/sol2/archive/248fd71.tar.gz
    URL_MD5 920d266cd552d97bc588fca91aabeef5)

cooking_ingredient(croaring
  CMAKE_ARGS
    -DENABLE_ROARING_TESTS=OFF
    -DROARING_BUILD_STATIC=ON
    -DROARING_DISABLE_NATIVE=ON
  EXTERNAL_PROJECT_ARGS
    URL https://github.com/RoaringBitmap/CRoaring/archive/6154187.tar.gz
    URL_MD5 e64f4712d4b1235bd009279f4485b044)


cooking_ingredient(pcg
  EXTERNAL_PROJECT_ARGS
    URL https://github.com/imneme/pcg-cpp/archive/b656278.tar.gz
    URL_MD5 576a4427f2c63ee15e36b5c88585569c
    PATCH_COMMAND <DISABLE>
    CONFIGURE_COMMAND <DISABLE>
    BUILD_COMMAND <DISABLE>
    INSTALL_COMMAND
     COMMAND ${CMAKE_COMMAND} -E copy <SOURCE_DIR>/include/pcg_extras.hpp <INSTALL_DIR>/include/pcg_extras.hpp
     COMMAND ${CMAKE_COMMAND} -E copy <SOURCE_DIR>/include/pcg_random.hpp <INSTALL_DIR>/include/pcg_random.hpp
     COMMAND ${CMAKE_COMMAND} -E copy <SOURCE_DIR>/include/pcg_uint128.hpp <INSTALL_DIR>/include/pcg_uint128.hpp
    )
cooking_ingredient(bytellhashmap
  EXTERNAL_PROJECT_ARGS
    URL https://github.com/skarupke/flat_hash_map/archive/2c46874.tar.gz
    URL_MD5 0ed67930d4b3795d3fb2ab6ac721b66d
    PATCH_COMMAND <DISABLE>
    CONFIGURE_COMMAND <DISABLE>
    BUILD_COMMAND <DISABLE>
    INSTALL_COMMAND
     COMMAND ${CMAKE_COMMAND} -E copy <SOURCE_DIR>/bytell_hash_map.hpp <INSTALL_DIR>/include/bytell_hash_map.hpp
     COMMAND ${CMAKE_COMMAND} -E copy <SOURCE_DIR>/flat_hash_map.hpp <INSTALL_DIR>/include/flat_hash_map.hpp
     #COMMAND ${CMAKE_COMMAND} -E copy <SOURCE_DIR>/unordered_map.hpp <INSTALL_DIR>/include/unordered_map.hpp
    )


# -- below are seastar & smf deps
cooking_ingredient(smf
  COOKING_RECIPE wellknown
  COOKING_CMAKE_ARGS
    -DSMF_ENABLE_TESTS=OFF
    -DSMF_BUILD_PROGRAMS=OFF
  EXTERNAL_PROJECT_ARGS
    GIT_REPOSITORY https://github.com/smfrpc/smf.git
    GIT_TAG ae7acfa
    )
