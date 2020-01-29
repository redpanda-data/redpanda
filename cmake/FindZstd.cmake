find_package (PkgConfig REQUIRED)

pkg_search_module (Zstd_PC
  QUIET
  libzstd)

find_library (Zstd_LIBRARY
  NAMES zstd
  HINTS
    ${Zstd_PC_LIBRARY_DIRS})

find_path (Zstd_INCLUDE_DIR
  NAMES zstd.h
  HINTS
    ${Zstd_PC_INCLUDE_DIRS})

mark_as_advanced (
  Zstd_LIBRARY
  Zstd_INCLUDE_DIR)

include (FindPackageHandleStandardArgs)

find_package_handle_standard_args (Zstd
  REQUIRED_VARS
    Zstd_LIBRARY
    Zstd_INCLUDE_DIR
  VERSION_VAR Zstd_PC_VERSION)

set (Zstd_LIBRARIES ${Zstd_LIBRARY})
set (Zstd_INCLUDE_DIRS ${Zstd_INCLUDE_DIR})

if (Zstd_FOUND AND NOT (TARGET Zstd::zstd))
  add_library (Zstd::zstd UNKNOWN IMPORTED)

  set_target_properties (Zstd::zstd
    PROPERTIES
      IMPORTED_LOCATION ${Zstd_LIBRARY}
      INTERFACE_INCLUDE_DIRECTORIES ${Zstd_INCLUDE_DIRS})
endif ()
