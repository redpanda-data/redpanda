find_package(sol2 REQUIRED)
find_package(Luajit REQUIRED)
add_library(rphbadger STATIC
  hbadger.cc
  )
target_link_libraries(rphbadger
  PUBLIC
  ${CMAKE_DL_LIBS}
  Luajit::luajit
  sol2::sol2)
install(TARGETS rphbadger DESTINATION lib)

target_compile_definitions(rphbadger PUBLIC -DHONEY_BADGER)

rp_test(
  HBADGER_TEST
  BINARY_NAME simple_hbadger_test
  SOURCES hbadger_test.cc
  INPUT_FILES ${CMAKE_CURRENT_SOURCE_DIR}/honey_badger.lua
  LIBRARIES rphbadger
)

function (rp_hbadger_lib)
  set(options STATIC SHARED)
  set(oneValueArgs NAME)
  set(multiValueArgs
    INCLUDES
    SOURCES
    LIBRARIES
    HBADGER_LIBRARIES
    DEFINITIONS
    COMPILE_OPTIONS)
  cmake_parse_arguments(BADGER_LIB "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

  set(lib_targets "${BADGER_LIB_NAME}" "${BADGER_LIB_NAME}_hbadger")
  foreach(i ${lib_targets})
    add_library(${i} STATIC
      "${BADGER_LIB_SOURCES}")
    target_include_directories(${i}
      PUBLIC "${BADGER_LIB_INCLUDES}")
    target_compile_options(${i}
      PUBLIC "${BADGER_LIB_COMPILE_OPTIONS}")
    target_link_libraries(${i}
      PUBLIC "${BADGER_LIB_LIBRARIES}")
    install(TARGETS ${i} DESTINATION lib)
  endforeach()
  # actually create the badger lib!
  target_link_libraries("${BADGER_LIB_NAME}_hbadger" PUBLIC rphbadger)
  if(BADGER_LIB_HBADGER_LIBRARIES)
    target_link_libraries("${BADGER_LIB_NAME}_hbadger"
      PUBLIC "${BADGER_LIB_HBADGER_LIBRARIES}")
  endif()
endfunction()
