include(set_option)
find_program(GIT git)
if(NOT DEFINED GIT_VERSION AND GIT)
  execute_process(
    COMMAND ${GIT} log -1 --pretty=format:%h
    WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
    OUTPUT_VARIABLE GIT_VERSION
    OUTPUT_STRIP_TRAILING_WHITESPACE
    )
endif()
if(NOT DEFINED GIT_VERSION)
  # We don't have git
  string(TIMESTAMP GIT_VERSION "%Y-%m-%d %H:%M")
endif()

set(CPACK_SOURCE_IGNORE_FILES "${CPACK_SOURCE_IGNORE_FILES}")
set(CPACK_PACKAGE_FILE_NAME "rp-${GIT_VERSION}")
set(CPACK_SOURCE_PACKAGE_FILE_NAME ${CPACK_PACKAGE_FILE_NAME})

RP_SET_OPTION(CPACK_SOURCE_TGZ ON)
# By default, all forms of binary packages are disabled.
RP_SET_OPTION(CPACK_BINARY_BUNDLE OFF)
RP_SET_OPTION(CPACK_BINARY_DEB OFF)
RP_SET_OPTION(CPACK_BINARY_DRAGNDROP OFF)
RP_SET_OPTION(CPACK_BINARY_IFW OFF)
RP_SET_OPTION(CPACK_BINARY_NSIS OFF)
RP_SET_OPTION(CPACK_BINARY_OSXX11 OFF)
RP_SET_OPTION(CPACK_BINARY_PACKAGEMAKER OFF)
RP_SET_OPTION(CPACK_BINARY_RPM OFF)
RP_SET_OPTION(CPACK_BINARY_STGZ OFF)
RP_SET_OPTION(CPACK_BINARY_TBZ2 OFF)
RP_SET_OPTION(CPACK_BINARY_TGZ OFF)
RP_SET_OPTION(CPACK_BINARY_TXZ OFF)
# By default, other forms of source packages are disabled.
RP_SET_OPTION(CPACK_SOURCE_TBZ2 OFF)
RP_SET_OPTION(CPACK_SOURCE_TXZ OFF)
RP_SET_OPTION(CPACK_SOURCE_TZ OFF)
RP_SET_OPTION(CPACK_SOURCE_ZIP OFF)

include(CPack)
