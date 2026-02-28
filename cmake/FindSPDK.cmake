# FindSPDK.cmake
# Locates SPDK libraries via pkg-config from the in-tree build.
#
# Sets:
#   SPDK_FOUND
#   SPDK_INCLUDE_DIRS
#   SPDK_LINK_LIBRARIES   (full link flags including --whole-archive wrapping)
#   SPDK_LIBRARY_DIRS

find_package(PkgConfig REQUIRED)

# Allow override via cmake -DSPDK_PKG_CONFIG_PATH=...
if(NOT SPDK_PKG_CONFIG_PATH)
    set(SPDK_PKG_CONFIG_PATH "${CMAKE_SOURCE_DIR}/third_party/spdk/build/lib/pkgconfig")
endif()

set(ENV{PKG_CONFIG_PATH} "${SPDK_PKG_CONFIG_PATH}:$ENV{PKG_CONFIG_PATH}")

set(_spdk_modules
    spdk_iscsi
    spdk_scsi
    spdk_bdev
    spdk_thread
    spdk_app_rpc
    spdk_env_dpdk
    spdk_log
    spdk_util
    spdk_json
    spdk_jsonrpc
    spdk_rpc
    spdk_trace
    spdk_sock
    spdk_notify
    spdk_dma
)

# Collect include dirs and link flags from each module
set(SPDK_INCLUDE_DIRS "")
set(SPDK_LINK_LIBRARIES "")
set(SPDK_LIBRARY_DIRS "")
set(SPDK_FOUND TRUE)

foreach(_mod ${_spdk_modules})
    pkg_check_modules(_SPDK_MOD ${_mod})
    if(_SPDK_MOD_FOUND)
        list(APPEND SPDK_INCLUDE_DIRS ${_SPDK_MOD_INCLUDE_DIRS})
        list(APPEND SPDK_LIBRARY_DIRS ${_SPDK_MOD_LIBRARY_DIRS})
        list(APPEND SPDK_LINK_LIBRARIES ${_SPDK_MOD_LINK_LIBRARIES})
    else()
        message(WARNING "SPDK module ${_mod} not found via pkg-config")
    endif()
endforeach()

list(REMOVE_DUPLICATES SPDK_INCLUDE_DIRS)
list(REMOVE_DUPLICATES SPDK_LIBRARY_DIRS)

if(NOT SPDK_LINK_LIBRARIES)
    set(SPDK_FOUND FALSE)
    if(FindSPDK_FIND_REQUIRED)
        message(FATAL_ERROR "SPDK not found. Build SPDK first: cd third_party/spdk && ./configure && make")
    endif()
endif()

mark_as_advanced(SPDK_INCLUDE_DIRS SPDK_LINK_LIBRARIES SPDK_LIBRARY_DIRS)
