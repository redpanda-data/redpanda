find_package (PkgConfig REQUIRED)
pkg_search_module (RE2_PC
  QUIET
  re2)
find_library(RE2_LIBRARY
  NAMES re2
  HINTS
    ${RE2_PC_LIBDIR}
    ${RE2_PC_LIBRARY_DIRS})
find_path(RE2_INCLUDE_DIR
  NAMES "re2/re2.h"
  HINTS
    ${RE2_PC_INCLUDEDIR}
    ${RE2_PC_INCLUDEDIRS})
mark_as_advanced(
  RE2_LIBRARY
  RE2_INCLUDE_DIR)
include (FindPackageHandleStandardArgs)
find_package_handle_standard_args(RE2
  REQUIRED_VARS
    RE2_LIBRARY
    RE2_INCLUDE_DIR
  VERSION_VAR RE2_PC_VERSION)
set (RE2_LIBRARIES ${RE2_LIBRARY})
list(APPEND RE2_INCLUDE_DIRS
  ${RE2_INCLUDE_DIR}
  "${RE2_INCLUDE_DIR}/re2"
  )
if (RE2_FOUND AND NOT (TARGET RE2::re2))
  add_library (RE2::re2 UNKNOWN IMPORTED)
  set_target_properties (RE2::re2
    PROPERTIES
      IMPORTED_LOCATION ${RE2_LIBRARY}
      INTERFACE_INCLUDE_DIRECTORIES "${RE2_INCLUDE_DIRS}")
endif ()
