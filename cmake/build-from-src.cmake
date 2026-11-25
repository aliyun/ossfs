set(actually_built)

function(build_from_src [dep])

  if (dep STREQUAL "gflags")
    ExternalProject_Add(
            gflags
            URL ${GFLAGS_SOURCE}
            URL_MD5 1a865b93bacfa963201af3f75b7bd64c
            CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DCMAKE_POSITION_INDEPENDENT_CODE=ON
            INSTALL_COMMAND ""
    )
    if (CMAKE_BUILD_TYPE STREQUAL "Debug")
      set(POSTFIX "_debug")
    endif ()
    ExternalProject_Get_Property(gflags BINARY_DIR)
    set(GFLAGS_INCLUDE_DIRS ${BINARY_DIR}/include PARENT_SCOPE)
    set(GFLAGS_LIBRARIES ${BINARY_DIR}/lib/libgflags${POSTFIX}.a PARENT_SCOPE)

  elseif (dep STREQUAL "googletest")
    ExternalProject_Add(
            googletest
            URL ${GOOGLETEST_SOURCE}
            URL_MD5 e82199374acdfda3f425331028eb4e2a
            CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DINSTALL_GTEST=OFF -DCMAKE_POSITION_INDEPENDENT_CODE=ON
            INSTALL_COMMAND ""
    )
    ExternalProject_Get_Property(googletest SOURCE_DIR)
    ExternalProject_Get_Property(googletest BINARY_DIR)
    set(GOOGLETEST_INCLUDE_DIRS ${SOURCE_DIR}/googletest/include ${SOURCE_DIR}/googlemock/include PARENT_SCOPE)
    set(GOOGLETEST_LIBRARIES ${BINARY_DIR}/lib/libgmock.a ${BINARY_DIR}/lib/libgtest.a PARENT_SCOPE)

  elseif (dep STREQUAL "openssl")
    set(BINARY_DIR ${PROJECT_BINARY_DIR}/openssl-build)
    # Only libssl.a/libcrypto.a and the public headers are consumed by the
    # project (statically linked, no openssl app/engines). Trim the build so we
    # skip compiling/linking the test, apps and fuzz programs, and skip the
    # manpage/HTML install that dominates `make install`. Fully reproducible.
    set(install_cmd
            "${CMAKE_COMMAND} -E copy_directory include/openssl ${BINARY_DIR}/include/openssl &&
             ${CMAKE_COMMAND} -E make_directory ${BINARY_DIR}/lib &&
             ${CMAKE_COMMAND} -E copy libcrypto.a ${BINARY_DIR}/lib/libcrypto.a &&
             ${CMAKE_COMMAND} -E copy libssl.a ${BINARY_DIR}/lib/libssl.a")
    ExternalProject_Add(
            openssl
            URL ${OPENSSL_SOURCE}
            URL_MD5 3f76825f195e52d4b10c70040681a275
            UPDATE_DISCONNECTED ON
            BUILD_IN_SOURCE ON
            CONFIGURE_COMMAND ./config -fPIC --prefix=${BINARY_DIR} --openssldir=${BINARY_DIR} no-shared no-tests
            BUILD_COMMAND $(MAKE) build_libs
            INSTALL_COMMAND sh -c "${install_cmd}"
            LOG_CONFIGURE ON
            LOG_BUILD ON
            LOG_INSTALL ON
    )
    set(OPENSSL_ROOT_DIR ${BINARY_DIR} PARENT_SCOPE)
    set(OPENSSL_INCLUDE_DIRS ${BINARY_DIR}/include PARENT_SCOPE)
    set(OPENSSL_LIBRARIES ${BINARY_DIR}/lib/libssl.a ${BINARY_DIR}/lib/libcrypto.a PARENT_SCOPE)
  
  elseif (dep STREQUAL "aio")
    set(BINARY_DIR ${PROJECT_BINARY_DIR}/aio-build)
    ExternalProject_Add(
            aio
            URL ${AIO_SOURCE}
            URL_MD5 605237f35de238dfacc83bcae406d95d
            UPDATE_DISCONNECTED ON
            BUILD_IN_SOURCE ON
            CONFIGURE_COMMAND ""
            BUILD_COMMAND $(MAKE) prefix=${BINARY_DIR} install
            INSTALL_COMMAND ""
    )
    set(AIO_INCLUDE_DIRS ${BINARY_DIR}/include PARENT_SCOPE)
    set(AIO_LIBRARIES ${BINARY_DIR}/lib/libaio.a PARENT_SCOPE)

  elseif (dep STREQUAL "cityhash")
    set(BINARY_DIR ${PROJECT_BINARY_DIR}/cityhash-build)
    ExternalProject_Add(
            cityhash
            URL ${CITYHASH_SOURCE}
            URL_MD5 ec209861c0f4d7520174ef2a40b64deb
            UPDATE_DISCONNECTED ON
            BUILD_IN_SOURCE ON
            PATCH_COMMAND touch aclocal.m4 configure Makefile.in src/Makefile.in config.h.in
            CONFIGURE_COMMAND ./configure --build=${ARCH}-unknown-linux-gnu --prefix=${BINARY_DIR} --disable-shared
            BUILD_COMMAND $(MAKE) all CXXFLAGS=-g\ -O3\ -fPIC
            INSTALL_COMMAND $(MAKE) install
            LOG_CONFIGURE ON
            LOG_BUILD ON
            LOG_INSTALL ON
    )
    set(CITYHASH_INCLUDE_DIRS ${BINARY_DIR}/include PARENT_SCOPE)
    set(CITYHASH_LIBRARIES ${BINARY_DIR}/lib/libcityhash.a PARENT_SCOPE)
  endif ()

  list(APPEND actually_built ${dep})
  set(actually_built ${actually_built} PARENT_SCOPE)
endfunction()
