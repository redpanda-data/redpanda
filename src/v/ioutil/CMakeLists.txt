find_package(smf)
set(ioutil_sources
  dir_utils.cc
  priority_manager.cc)
rp_hbadger_lib(
  STATIC
  NAME ioutil
  SOURCES ${ioutil_sources}
  INCLUDES ${PROJECT_SOURCE_DIR}/src/v
  COMPILE_OPTIONS -Wall -Werror -fconcepts -Wno-unused-variable
  LIBRARIES smf::smf
  )
