cmake_minimum_required(VERSION 3.20)
project(gorun VERSION 0.4.0 LANGUAGES CXX ASM)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

# Determine if gorun is built as a standalone top-level project or consumed via FetchContent/add_subdirectory
if(CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
    set(GORUN_IS_TOP_LEVEL ON)
else()
    set(GORUN_IS_TOP_LEVEL OFF)
endif()

add_library(gorun INTERFACE)
add_library(gorun::gorun ALIAS gorun)

target_compile_features(gorun INTERFACE cxx_std_20)
target_include_directories(gorun INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

if(MSVC)
    target_compile_options(gorun INTERFACE /W4 /permissive- /EHsc)
else()
    target_compile_options(gorun INTERFACE -Wall -Wextra -Wpedantic -pthread)
    target_link_libraries(gorun INTERFACE pthread)
endif()

if(WIN32)
    target_link_libraries(gorun INTERFACE ws2_32 mswsock)
endif()

option(GORUN_BUILD_TESTS "Build gorun unit and stress tests" ${GORUN_IS_TOP_LEVEL})
option(GORUN_BUILD_EXAMPLES "Build gorun examples" ${GORUN_IS_TOP_LEVEL})
option(GORUN_BUILD_BENCHMARKS "Build gorun performance benchmarks" ${GORUN_IS_TOP_LEVEL})
option(GORUN_INSTALL "Generate install target and CMake package config for gorun" ON)

if(GORUN_BUILD_TESTS)
    enable_testing()
    add_executable(gorun_tests tests/test_main.cpp)
    target_link_libraries(gorun_tests PRIVATE gorun::gorun)
    add_test(NAME gorun_tests COMMAND gorun_tests)

    add_executable(gorun_v04_tests tests/test_v04_features.cpp)
    target_link_libraries(gorun_v04_tests PRIVATE gorun::gorun)
    add_test(NAME gorun_v04_tests COMMAND gorun_v04_tests)
endif()

if(GORUN_BUILD_EXAMPLES)
    add_executable(gorun_demo examples/demo.cpp)
    target_link_libraries(gorun_demo PRIVATE gorun::gorun)

    add_executable(example_01_channels examples/01_channels_and_select.cpp)
    target_link_libraries(example_01_channels PRIVATE gorun::gorun)

    add_executable(example_02_structured examples/02_structured_concurrency_and_defer.cpp)
    target_link_libraries(example_02_structured PRIVATE gorun::gorun)

    add_executable(example_03_web_context examples/03_web_microservice_context.cpp)
    target_link_libraries(example_03_web_context PRIVATE gorun::gorun)

    add_executable(example_04_worker_pool examples/04_worker_pool_and_sync.cpp)
    target_link_libraries(example_04_worker_pool PRIVATE gorun::gorun)

    add_executable(example_05_memory_arenas examples/05_memory_arenas_and_pointers.cpp)
    target_link_libraries(example_05_memory_arenas PRIVATE gorun::gorun)

    add_executable(example_06_http_server examples/06_native_http_server.cpp)
    target_link_libraries(example_06_http_server PRIVATE gorun::gorun)
endif()

if(GORUN_BUILD_BENCHMARKS)
    add_executable(gorun_bench benchmarks/bench_gorun.cpp)
    target_link_libraries(gorun_bench PRIVATE gorun::gorun)

    add_executable(std_bench benchmarks/bench_std_alternatives.cpp)
    target_link_libraries(std_bench PRIVATE gorun::gorun)
endif()

# ============================================================================
# CMake Package Installation & Export (supports find_package(gorun CONFIG))
# ============================================================================
if(GORUN_INSTALL)
    install(TARGETS gorun
        EXPORT gorunTargets
        INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    )

    install(DIRECTORY include/gorun
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    )

    install(FILES LICENSE README.md
        DESTINATION ${CMAKE_INSTALL_DOCDIR}
    )

    install(EXPORT gorunTargets
        FILE gorunTargets.cmake
        NAMESPACE gorun::
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/gorun
    )

    configure_package_config_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/cmake/gorunConfig.cmake.in
        ${CMAKE_CURRENT_BINARY_DIR}/gorunConfig.cmake
        INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/gorun
    )

    write_basic_package_version_file(
        ${CMAKE_CURRENT_BINARY_DIR}/gorunConfigVersion.cmake
        VERSION ${PROJECT_VERSION}
        COMPATIBILITY SameMajorVersion
        ARCH_INDEPENDENT
    )

    install(FILES
        ${CMAKE_CURRENT_BINARY_DIR}/gorunConfig.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/gorunConfigVersion.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/gorun
    )
endif()
