cmake_minimum_required(VERSION 3.8)

project(ossfs2 VERSION 2.0.6)

list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
include(FindPackageHandleStandardArgs)
include(ExternalProject)
include(cmake/build-from-src.cmake)

if (NOT CMAKE_SYSTEM_NAME STREQUAL Linux)
  message(FATAL_ERROR "Only Linux is supported")
endif ()

# Get CPU arch
execute_process(COMMAND uname -m OUTPUT_VARIABLE ARCH OUTPUT_STRIP_TRAILING_WHITESPACE)
if (NOT (${ARCH} STREQUAL x86_64) AND NOT (${ARCH} STREQUAL aarch64))
  message(FATAL_ERROR "Unknown CPU architecture ${ARCH}")
endif ()

option(ENABLE_TESTING "Build testing" OFF)
option(ENABLE_COVERAGE "Build with coverage" OFF)
option(ENABLE_ASAN "Build with address sanitizer" OFF)
option(SEPARATE_DEBUG_SYMBOLS "Separate debug symbols" OFF)

if (NOT CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL 9)
  message(FATAL_ERROR "need gcc version >= 9.0" )
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g")
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -DNDEBUG -g")
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_BUILD_RPATH_USE_ORIGIN ON)

# Get OSSFS2 version, commit ID and build time
execute_process(
  COMMAND git log --format=%H -n1
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  OUTPUT_VARIABLE OSSFS_COMMIT_ID
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process(
  COMMAND date -u "+%Y-%m-%d.%H:%M:%S.%Z"
  OUTPUT_VARIABLE OSSFS_BUILD_TIME
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

set(OSSFS_VERSION_ID ${PROJECT_VERSION})

add_definitions("-D_FILE_OFFSET_BITS=64")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror -Wno-error=unused-result")
if (${ARCH} STREQUAL x86_64)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2 -mpclmul")
endif ()

if (ENABLE_TESTING AND ENABLE_COVERAGE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage -g")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fprofile-arcs -ftest-coverage -g")
  set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fprofile-arcs -ftest-coverage -g")
endif()

if (ENABLE_ASAN)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lasan")
  set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -lasan")
endif ()

# Set default build type to Release
if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif ()

add_subdirectory(dependencies)

set(DEPS_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/dependencies/src")
set(GFLAGS_SOURCE "file://${DEPS_SOURCE_DIR}/gflags-2.2.2.tar.gz" CACHE STRING "")
set(GOOGLETEST_SOURCE "file://${DEPS_SOURCE_DIR}/googletest-1.12.1.tar.gz" CACHE STRING "")
set(OPENSSL_SOURCE "file://${DEPS_SOURCE_DIR}/openssl-1.0.2-stable.tar.gz" CACHE STRING "")
set(AIO_SOURCE "file://${DEPS_SOURCE_DIR}/libaio-0.3.113.tar.gz" CACHE STRING "")

message("patching PhotonLibOS with fault injections")
find_program(TAR_EXECUTABLE tar REQUIRED)
set(PHOTON_TARBALL "${CMAKE_SOURCE_DIR}/dependencies/src/PhotonLibOS-feature-oss.tar.gz")
set(PATCH_FILE     "${CMAKE_SOURCE_DIR}/dependencies/patches/photon_injections.patch")
set(PHOTON_PATCHED_SRC_DIR "${CMAKE_BINARY_DIR}/PhotonLibOS-patched")

add_custom_command(
  OUTPUT ${PHOTON_PATCHED_SRC_DIR}/.unpacked
  COMMAND ${CMAKE_COMMAND} -E make_directory ${PHOTON_PATCHED_SRC_DIR}
  COMMAND tar xvf ${PHOTON_TARBALL} --strip-components=1 -C ${PHOTON_PATCHED_SRC_DIR}
  COMMAND ${CMAKE_COMMAND} -E touch ${PHOTON_PATCHED_SRC_DIR}/.unpacked
  DEPENDS ${PHOTON_TARBALL}
  COMMENT "Unpacking PhotonLibOS source..."
)

add_custom_command(
  OUTPUT ${PHOTON_PATCHED_SRC_DIR}/ecosystem/oss_patched.cpp ${PHOTON_PATCHED_SRC_DIR}/.patched
  COMMAND patch -p1 -d ${PHOTON_PATCHED_SRC_DIR} < ${PATCH_FILE}
  COMMAND ${CMAKE_COMMAND} -E touch ${PHOTON_PATCHED_SRC_DIR}/.patched
  DEPENDS ${PHOTON_PATCHED_SRC_DIR}/.unpacked ${PATCH_FILE}
  COMMENT "Applying patch.diff to PhotonLibOS..."
)

add_custom_target(photon_patched 
  DEPENDS ${PHOTON_PATCHED_SRC_DIR}/.patched
)

set(deps openssl gflags aio)
if (ENABLE_TESTING)
  list(APPEND deps googletest)
endif ()

if (${ARCH} STREQUAL aarch64)
  # We only support aarch64 in alibaba linux cloud 3, use shared openssl
  set(OPENSSL_SOURCE "")
endif ()

FOREACH (dep ${deps})
  string(TOUPPER ${dep} DEP)
  set(source_url "${${DEP}_SOURCE}")
  if (NOT source_url STREQUAL "")
    message(STATUS "Will build ${dep} from source")
    message(STATUS "    URL: ${source_url}")
    build_from_src(dep)
  else ()
    message(STATUS "Will find ${dep}")
    find_package(${dep} REQUIRED)
  endif ()
endforeach ()

include_directories(
  src/
  ${LIBFUSE_INCLUDE_DIRS}
  ${PHOTONLIBOS_INCLUDE_DIRS}
  ${OPENSSL_INCLUDE_DIRS}
  ${GFLAGS_INCLUDE_DIRS}
  ${CLI11_INCLUDE_DIRS}
  ${AIO_INCLUDE_DIRS})

file(GLOB_RECURSE ossfs2_common_srcs RELATIVE "${PROJECT_SOURCE_DIR}"
  src/common/*.cpp
  src/fs/*.cpp
  src/oss/*.cpp
  src/metric/*.cpp
  src/credentials/*.cpp
  src/admin/*.cpp)

file(GLOB_RECURSE ossfs2_test_srcs RELATIVE "${PROJECT_SOURCE_DIR}"
  src/common/test/*.cpp
  src/fs/test/*.cpp)

list(REMOVE_ITEM ossfs2_common_srcs ${ossfs2_test_srcs})

set(static_deps
  ${PHOTONLIBOS_LIBRARIES}
  ${OPENSSL_LIBRARIES}
  ${GFLAGS_LIBRARIES}
  ${AIO_LIBRARIES})

add_library(ossfs2_common STATIC ${ossfs2_common_srcs})

target_sources(ossfs2_common PRIVATE
  ${PHOTON_PATCHED_SRC_DIR}/ecosystem/oss_patched.cpp
)
include_directories(${PHOTON_PATCHED_SRC_DIR}/ecosystem)

add_dependencies(ossfs2_common libfuse PhotonLibOS photon_patched)
if (actually_built)
  add_dependencies(ossfs2_common ${actually_built})
endif ()

file(GLOB ossfs2_srcs RELATIVE "${PROJECT_SOURCE_DIR}" 
  src/*.cpp)

add_executable(ossfs2 ${ossfs2_srcs})

# we will install libfuse to /usr/local/lib64/ossfs2, make sure ossfs2 
# can find it instead of using the system one
set(OSSFS2_RPATH "/usr/local/lib64/ossfs2")
set(COMMON_RPATH "/usr/lib:/usr/lib64:/usr/local/lib:/usr/local/lib64")
get_filename_component(LIBFUSE_LIB_DIR ${LIBFUSE_LIBRARIES} DIRECTORY)
set_target_properties(ossfs2 PROPERTIES
  BUILD_RPATH "$ORIGIN:${LIBFUSE_LIB_DIR}:${OSSFS2_RPATH}:${COMMON_RPATH}"
  INSTALL_RPATH "$ORIGIN:${OSSFS2_RPATH}:${COMMON_RPATH}")

target_compile_definitions(ossfs2 PRIVATE
  OSSFS_COMMIT_ID=${OSSFS_COMMIT_ID}
  OSSFS_BUILD_TIME=${OSSFS_BUILD_TIME}
  OSSFS_VERSION_ID=${OSSFS_VERSION_ID})

target_link_libraries(ossfs2 ossfs2_common ${static_deps} ${LIBFUSE_LIBRARIES} 
  rt dl pthread -static-libgcc -static-libstdc++)

if (SEPARATE_DEBUG_SYMBOLS OR CPACK_GENERATOR)
  add_custom_command(TARGET ossfs2 POST_BUILD
    COMMAND ${CMAKE_OBJCOPY} --only-keep-debug $<TARGET_FILE:ossfs2> $<TARGET_FILE:ossfs2>.debug
    COMMAND ${CMAKE_STRIP} --strip-debug $<TARGET_FILE:ossfs2>
    COMMAND ${CMAKE_OBJCOPY} --add-gnu-debuglink=$<TARGET_FILE:ossfs2>.debug $<TARGET_FILE:ossfs2>
    COMMENT "Stripping debug symbols from ossfs2 and creating separate debug file"
    VERBATIM)
endif()

install(TARGETS ossfs2 RUNTIME DESTINATION /usr/local/bin COMPONENT main)
install(FILES ${LIBFUSE_LIBRARIES} DESTINATION /usr/local/lib64/ossfs2/
        RENAME libfuse3.so.3 COMPONENT main)

add_custom_command(
  OUTPUT ${CMAKE_BINARY_DIR}/mount.ossfs2
  COMMAND ${CMAKE_COMMAND} -E create_symlink /usr/local/bin/ossfs2 ${CMAKE_BINARY_DIR}/mount.ossfs2
  DEPENDS ossfs2
  COMMENT "Creating symlink: mount.ossfs2 -> ossfs2"
)
add_custom_target(generate_ossfs2_symlink ALL DEPENDS ${CMAKE_BINARY_DIR}/mount.ossfs2)
install(FILES ${CMAKE_BINARY_DIR}/mount.ossfs2 DESTINATION /usr/sbin
        COMPONENT main)

if (SEPARATE_DEBUG_SYMBOLS OR CPACK_GENERATOR)
  install(FILES ${CMAKE_BINARY_DIR}/ossfs2.debug DESTINATION 
    /usr/lib/debug/usr/local/bin COMPONENT debuginfo)
endif()

if (ENABLE_TESTING)
  set(test_static_deps ${static_deps})
  list(APPEND test_static_deps ${GOOGLETEST_LIBRARIES})

  include_directories(${GOOGLETEST_INCLUDE_DIRS})

  file(GLOB ossfs2_test_srcs RELATIVE "${PROJECT_SOURCE_DIR}" 
    src/fs/test/*.cpp
    src/common/test/*.cpp)

  add_executable(ossfs2-test ${ossfs2_test_srcs})

  set_target_properties(ossfs2-test PROPERTIES
    BUILD_RPATH "$ORIGIN:${LIBFUSE_LIB_DIR}")
  target_link_libraries(ossfs2-test ossfs2_common ${test_static_deps} 
    ${LIBFUSE_LIBRARIES} rt dl pthread -static-libgcc -static-libstdc++)
endif()

set(CPACK_COMPONENTS_ALL main debuginfo)
set(CPACK_PACKAGE_NAME "ossfs2")
set(CPACK_DEBUGINFO_PACKAGE_NAME "${CPACK_PACKAGE_NAME}-debuginfo")
set(CPACK_PACKAGE_VERSION "${OSSFS_VERSION_ID}")

set(CPACK_PACKAGE_DESCRIPTION "\
 This package includes OSSFS2: A High Performance OSS Posix Client.
 Commit ID: ${OSSFS_COMMIT_ID}")
set(CPACK_PACKAGE_DEBUGINFO_DESCRIPTION "\
 This package includes debuginfo of OSSFS2.
 Commit ID: ${OSSFS_COMMIT_ID}")

set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "OSSFS2: A High Performance OSS Posix Client") 

set(CPACK_PACKAGE_GROUP "Alibaba Cloud")
set(CPACK_PACKAGE_RELOCATABLE OFF)

set(CPACK_FILE_NAME "${CPACK_PACKAGE_NAME}_${CPACK_PACKAGE_VERSION}_linux_${ARCH}")
set(CPACK_DEBUGINFO_FILE_NAME 
  "${CPACK_PACKAGE_NAME}_debuginfo_${CPACK_PACKAGE_VERSION}_linux_${ARCH}")

if(CPACK_GENERATOR MATCHES "RPM")
  set(CPACK_RPM_COMPONENT_INSTALL ON)

  set(CPACK_RPM_PACKAGE_AUTOREQ OFF)
  set(CPACK_RPM_PACKAGE_RELEASE "1")
  set(CPACK_RPM_PACKAGE_REQUIRES "glibc >= 2.17")
  set(CPACK_RPM_PACKAGE_SUMMARY "${CPACK_PACKAGE_DESCRIPTION_SUMMARY}")
  set(CPACK_RPM_PACKAGE_DESCRIPTION "${CPACK_PACKAGE_DESCRIPTION}")
  set(CPACK_RPM_PACKAGE_LICENSE "Commercial")
  set(CPACK_RPM_PACKAGE_GROUP "${CPACK_PACKAGE_GROUP}")

  set(CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST 
      "/usr"
      "/usr/local"
      "/usr/local/bin"
      "/usr/local/lib64"
      "/usr/sbin"
      "/usr/lib"
      "/usr/lib/debug"
      "/usr/lib/debug/usr"
      "/usr/lib/debug/usr/local"
      "/usr/lib/debug/usr/local/bin")

  set(CPACK_RPM_MAIN_PACKAGE_NAME "${CPACK_PACKAGE_NAME}")
  set(CPACK_RPM_MAIN_FILE_NAME "${CPACK_FILE_NAME}.rpm")

  set(CPACK_RPM_DEBUGINFO_PACKAGE_NAME "${CPACK_PACKAGE_NAME}-debuginfo")
  set(CPACK_RPM_DEBUGINFO_FILE_NAME "${CPACK_DEBUGINFO_FILE_NAME}.rpm")
  set(CPACK_RPM_DEBUGINFO_PACKAGE_REQUIRES 
    "${CPACK_PACKAGE_NAME} = ${CPACK_PACKAGE_VERSION}-${CPACK_RPM_PACKAGE_RELEASE}")
  set(CPACK_COMPONENT_DEBUGINFO_DESCRIPTION "${CPACK_PACKAGE_DEBUGINFO_DESCRIPTION}")
elseif(CPACK_GENERATOR MATCHES "DEB")
  set(CPACK_DEB_COMPONENT_INSTALL ON)

  set(CPACK_DEBIAN_PACKAGE_DEPENDS "libc6 (>= 2.17)")
  set(CPACK_DEBIAN_PACKAGE_SUMMARY "${CPACK_PACKAGE_DESCRIPTION_SUMMARY}")
  string(CONCAT CPACK_DEBIAN_PACKAGE_DESCRIPTION 
    "${CPACK_PACKAGE_DESCRIPTION_SUMMARY}" "\n" "${CPACK_PACKAGE_DESCRIPTION}")
  set(CPACK_DEBIAN_PACKAGE_SECTION "utils")
  set(CPACK_DEBIAN_PACKAGE_MAINTAINER "${CPACK_PACKAGE_GROUP}")

  set(CPACK_DEBIAN_MAIN_PACKAGE_NAME "${CPACK_PACKAGE_NAME}")
  set(CPACK_DEBIAN_MAIN_FILE_NAME "${CPACK_FILE_NAME}.deb")

  set(CPACK_DEBIAN_DEBUGINFO_PACKAGE_NAME "${CPACK_PACKAGE_NAME}-dbg")
  set(CPACK_DEBIAN_DEBUGINFO_FILE_NAME "${CPACK_DEBUGINFO_FILE_NAME}.deb")
  set(CPACK_DEBIAN_DEBUGINFO_PACKAGE_DEPENDS 
    "${CPACK_PACKAGE_NAME} (= ${CPACK_PACKAGE_VERSION})")
  string(CONCAT CPACK_COMPONENT_DEBUGINFO_DESCRIPTION 
    "${CPACK_PACKAGE_DESCRIPTION_SUMMARY}" "\n" "${CPACK_PACKAGE_DEBUGINFO_DESCRIPTION}")
endif()

include(CPack)
