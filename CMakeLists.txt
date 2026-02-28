cmake_minimum_required(VERSION 3.20)
project(fy-disks LANGUAGES C CXX)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")

# ── Dependencies ────────────────────────────────────────────────────────────

find_package(CLI11 REQUIRED)

# ChDB shared library
find_library(CHDB_LIB chdb
    HINTS /usr/local/lib /usr/lib
    REQUIRED
)

# ChDB headers (chdb.h)
find_path(CHDB_INCLUDE_DIR chdb.h
    HINTS /usr/local/include /usr/include
    REQUIRED
)

# liburing
find_library(URING_LIB uring REQUIRED)
find_path(URING_INCLUDE_DIR liburing.h
    HINTS /usr/local/include /usr/include
    REQUIRED
)

# SPDK (built from third_party/spdk)
find_package(SPDK REQUIRED)

# ── Main executable ──────────────────────────────────────────────────────────

add_executable(fy-iscsi
    src/main.cpp
    src/meta.cpp
    src/vdisk_bdev.c
    src/startup.c
)

target_include_directories(fy-iscsi PRIVATE
    src/
    lib/
    ${CHDB_INCLUDE_DIR}
    ${URING_INCLUDE_DIR}
    ${SPDK_INCLUDE_DIRS}
    third_party/spdk/lib/iscsi
    third_party/spdk/lib/bdev
)

target_link_directories(fy-iscsi PRIVATE ${SPDK_LIBRARY_DIRS})

target_link_libraries(fy-iscsi PRIVATE
    CLI11::CLI11
    ${SPDK_LINK_LIBRARIES}
    ${CHDB_LIB}
    ${URING_LIB}
    pthread
    dl
    rt
)

# SPDK requires these compiler options
target_compile_options(fy-iscsi PRIVATE
    -march=native
    -D_GNU_SOURCE
)

# ── Tests ─────────────────────────────────────────────────────────────────

enable_testing()

find_library(ISCSI_LIB iscsi
    HINTS /usr/local/lib /usr/lib
          "${CMAKE_SOURCE_DIR}/third_party/libiscsi/lib/.libs"
)
find_path(ISCSI_INCLUDE_DIR iscsi/iscsi.h
    HINTS /usr/local/include /usr/include
          "${CMAKE_SOURCE_DIR}/third_party/libiscsi"
)

if(ISCSI_LIB AND ISCSI_INCLUDE_DIR)
    add_executable(iscsi_test test/iscsi_test.cpp)

    target_include_directories(iscsi_test PRIVATE
        ${ISCSI_INCLUDE_DIR}
    )

    target_link_libraries(iscsi_test PRIVATE
        ${ISCSI_LIB}
        pthread
    )

    target_compile_options(iscsi_test PRIVATE -D_GNU_SOURCE)

    add_test(NAME BasicIO
        COMMAND iscsi_test --addr 127.0.0.1 --port 3260 --case basic
    )
    add_test(NAME CrossSeg
        COMMAND iscsi_test --addr 127.0.0.1 --port 3260 --case cross_segment
    )
else()
    message(STATUS "libiscsi not found — skipping iscsi_test (dnf install libiscsi-devel)")
endif()
