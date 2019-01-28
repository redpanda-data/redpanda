find_package (PkgConfig REQUIRED)

pkg_search_module (Roaring_PC
  QUIET
  roaring)

find_library(Roaring_LIBRARY
  NAMES roaring
  HINTS
    ${Roaring_PC_LIBDIR}
    ${Roaring_PC_LIBRARY_DIRS})

find_path(Roaring_INCLUDE_DIR
  NAMES "roaring/roaring.h"
  HINTS
    ${Roaring_PC_INCLUDEDIR}/include
    ${Roaring_PC_INCLUDEDIRS})

mark_as_advanced(
  Roaring_LIBRARY
  Roaring_INCLUDE_DIR)

include (FindPackageHandleStandardArgs)

find_package_handle_standard_args(Roaring
  REQUIRED_VARS
    Roaring_LIBRARY
    Roaring_INCLUDE_DIR
  VERSION_VAR Roaring_PC_VERSION)

set (Roaring_LIBRARIES ${Roaring_LIBRARY})
set (Roaring_INCLUDE_DIRS ${Roaring_INCLUDE_DIR})

if (Roaring_FOUND AND NOT (TARGET Roaring::roaring))
  add_library (Roaring::roaring UNKNOWN IMPORTED)

  set_target_properties (Roaring::roaring
    PROPERTIES
      IMPORTED_LOCATION ${Roaring_LIBRARY}
      INTERFACE_INCLUDE_DIRECTORIES ${Roaring_INCLUDE_DIRS})
endif ()
