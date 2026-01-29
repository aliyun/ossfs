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
    ExternalProject_Add(
            openssl
            URL ${OPENSSL_SOURCE}
            URL_MD5 bad68bb6bd9908da75e2c8dedc536b29
            UPDATE_DISCONNECTED ON
            BUILD_IN_SOURCE ON
            CONFIGURE_COMMAND ./config -fPIC --prefix=${BINARY_DIR} --openssldir=${BINARY_DIR} shared
            BUILD_COMMAND make -j 1  # https://github.com/openssl/openssl/issues/5762#issuecomment-376622684
            INSTALL_COMMAND make -j 1 install
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
  endif ()

  list(APPEND actually_built ${dep})
  set(actually_built ${actually_built} PARENT_SCOPE)
endfunction()
