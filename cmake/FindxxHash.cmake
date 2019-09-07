find_package (PkgConfig REQUIRED)
pkg_search_module(xxHash_PC
  QUIET
  xxhash)
find_path(xxHash_INCLUDE_DIR
  NAMES xxhash.h
  HINTS
    ${xxHash_PC_INCLUDEDIR}
    ${xxHash_PC_INCLUDEDIRS})
find_library (xxHash_LIBRARY
  NAMES xxhash
  HINTS
    ${xxHash_PC_LIBDIR}
    ${xxHash_PC_LIBRARY_DIRS})
mark_as_advanced(
  xxHash_INCLUDE_DIR
  xxHash_LIBRARY
  )


include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(xxHash
  REQUIRED_VARS
    xxHash_INCLUDE_DIR
    xxHash_LIBRARY
  VERSION_VAR xxHash_PC_VERSION)

set(xxHash_LIBRARIES ${xxHash_LIBRARY})
set(xxHash_INCLUDE_DIRS ${xxHash_INCLUDE_DIR})

if (xxHash_FOUND AND NOT (TARGET xxHash::xxhash))
  add_library(xxHash::xxhash UNKNOWN IMPORTED)
  set_target_properties(xxHash::xxhash
    PROPERTIES
      IMPORTED_LOCATION ${xxHash_LIBRARY}
      INTERFACE_INCLUDE_DIRECTORIES ${xxHash_INCLUDE_DIRS})
endif ()

# always allow for inline
# To debug that the installation worked, uncomment the following lines
# it verifies that there exist an override to the ExternalProjectAdd
# INSTALL_COMMAND with a copy of the xxhash.c into the installation directory
# set_target_properties(
#   xxHash::xxhash
#   PROPERTIES
#     COMPILE_DEFINITIONS -DXXH_PRIVATE_API)
