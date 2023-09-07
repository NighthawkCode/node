#TODO: Do nothing if python dev is not installed
find_package(Python3 COMPONENTS Interpreter Development)

if(Python3_FOUND)
    add_library(pynode SHARED pynode.cpp)
    target_include_directories(pynode PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
    target_link_libraries(pynode PUBLIC Python3::Module nodelib pycbuf)
    target_compile_options(pynode PRIVATE -Wno-writable-strings -Wno-c99-designator -Wno-unused-function)
    set_target_properties(pynode PROPERTIES PREFIX "")

    set(CPACK_PACKAGING_INSTALL_PREFIX "/usr/local")
    install(TARGETS pynode DESTINATION lib)
    install(TARGETS pycbuf DESTINATION "/usr/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}")
    install(TARGETS pycbuf DESTINATION "/usr/local/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}")

    set(PYNODE_VERSION_MAJOR "1")
    set(PYNODE_VERSION_MINOR "0")
    set(PYNODE_VERSION_PATCH "0")

    set(CPACK_PACKAGE_NAME "pynode-${Python3_VERSION_MAJOR}-${Python3_VERSION_MINOR}")
    set(CPACK_GENERATOR "DEB")
    set(CPACK_PACKAGE_CONTACT "lawrence@verdantrobotics.com")
    set(CPACK_DEBIAN_PACKAGE_MAINTAINER "Lawrence Ibarria <${CPACK_PACKAGE_CONTACT}>")
    set(CPACK_PACKAGE_VENDOR "Verdant Robotics")
    set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Python interface to the node ecosystem"
        CACHE STRING "Pynode provides a set of python interface to interact with node, listing topics, subscribing." )
    set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS YES)
    set(CPACK_PACKAGE_VERSION_MAJOR ${PYNODE_VERSION_MAJOR})
    set(CPACK_PACKAGE_VERSION_MINOR ${PYNODE_VERSION_MINOR})
    set(CPACK_PACKAGE_VERSION_PATCH ${PYNODE_VERSION_PATCH})
    set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/../LICENSE.txt")
    set(CPACK_DEBIAN_FILE_NAME DEB-DEFAULT)
    include(CPack)
endif()
