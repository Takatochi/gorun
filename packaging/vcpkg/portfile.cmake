vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO Takatochi/gorun
    REF "v${VERSION}"
    SHA512 e8a40d765885b301fdde995cca0981c1f0c9c7a629ea75d9b466d83868260e28c0baf2bba0b839efed7551acb58e03607a4142640f56d7c34ddaa911c43d4ca0
    HEAD_REF main
)

vcpkg_cmake_configure(
    SOURCE_PATH "${SOURCE_PATH}"
    OPTIONS
        -DGORUN_BUILD_TESTS=OFF
        -DGORUN_BUILD_EXAMPLES=OFF
        -DGORUN_BUILD_BENCHMARKS=OFF
        -DGORUN_INSTALL=ON
)

vcpkg_cmake_install()
vcpkg_cmake_config_fixup(CONFIG_PATH lib/cmake/gorun)

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug" "${CURRENT_PACKAGES_DIR}/lib")

vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/LICENSE")
