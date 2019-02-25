# adapted form the smfrpc/smf repo
# I think this is useless. We should just replace it.

file(READ ${Seastar_DPDK_CONFIG_FILE_IN} dpdk_config)
file(STRINGS ${Seastar_DPDK_CONFIG_FILE_CHANGES} dpdk_config_changes)
set(word_pattern "[^\n\r \t]+")
foreach(var ${dpdk_config_changes})
  if(var MATCHES "(${word_pattern})=(${word_pattern})")
    set(key ${CMAKE_MATCH_1})
    set(value ${CMAKE_MATCH_2})
    string (REGEX REPLACE
      "${key}=${word_pattern}"
      "${key}=${value}"
      dpdk_config
      ${dpdk_config})
  endif()
endforeach()
file(WRITE ${Seastar_DPDK_CONFIG_FILE_OUT} ${dpdk_config})
