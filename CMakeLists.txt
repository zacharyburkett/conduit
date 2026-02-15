cmake_minimum_required(VERSION 3.20)
project(conduit VERSION 0.1.0 LANGUAGES C)

include(GNUInstallDirs)
find_package(Threads REQUIRED)

option(CONDUIT_BUILD_TESTS "Build conduit tests" ON)
option(CONDUIT_BUILD_SAMPLES "Build conduit sample apps" ON)

add_library(conduit
    src/core.c
    src/context.c
    src/sync.c
    src/bus.c
    src/transport_ipc.c
    src/transport_ipc_socket.c
    src/transport_inproc.c
)
add_library(conduit::conduit ALIAS conduit)

target_compile_features(conduit PUBLIC c_std_11)
target_link_libraries(conduit PUBLIC Threads::Threads)
target_include_directories(conduit
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

if(MSVC)
    target_compile_options(conduit PRIVATE /W4 /WX)
else()
    target_compile_options(conduit PRIVATE -Wall -Wextra -Wpedantic -Werror)
endif()

install(TARGETS conduit
    EXPORT conduitTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(
    EXPORT conduitTargets
    FILE conduitTargets.cmake
    NAMESPACE conduit::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/conduit
)

if(CONDUIT_BUILD_TESTS)
    enable_testing()
    add_executable(conduit_tests tests/test_main.c)
    target_link_libraries(conduit_tests PRIVATE conduit)
    add_test(NAME conduit_tests COMMAND conduit_tests)
endif()

if(CONDUIT_BUILD_SAMPLES)
    add_executable(conduit_embedded_sample apps/embedded_sample/main.c)
    target_link_libraries(conduit_embedded_sample PRIVATE conduit)
    add_executable(conduit_broker apps/broker/main.c)
    target_link_libraries(conduit_broker PRIVATE conduit)
    add_executable(conduit_loadgen apps/loadgen/main.c)
    target_link_libraries(conduit_loadgen PRIVATE conduit)

    if(CONDUIT_BUILD_TESTS)
        target_compile_definitions(
            conduit_tests
            PRIVATE
                CONDUIT_BROKER_BIN=\"$<TARGET_FILE:conduit_broker>\"
                CONDUIT_LOADGEN_BIN=\"$<TARGET_FILE:conduit_loadgen>\"
        )
        add_test(NAME conduit_embedded_sample_smoke COMMAND conduit_embedded_sample)
    endif()
endif()
