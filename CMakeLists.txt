cmake_minimum_required( VERSION 3.2.0 )

project( node )

list( APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" )

include(FetchDependency)

fetch_dependency( vlog "https://git.martian.ag/open-source/vlog.git" "master" )
fetch_dependency( cbuf "https://git.martian.ag/open-source/cbuf.git" "master" )

set( NODE_SRCS 
    src/nodelib.cpp src/node_registry.cpp src/process.cpp src/core.cpp
)

include(BuildCbuf)
build_cbuf( cbuf/image.cbuf )
build_cbuf( cbuf/registry.cbuf )

#add_custom_command(
#    OUTPUT image.h
#    COMMAND ${cbuf_BINARY_DIR}/cbuf ${CMAKE_CURRENT_SOURCE_DIR}/cbuf/image.cbuf > image.h
#    DEPENDS cbuf ${CMAKE_CURRENT_SOURCE_DIR}/cbuf/image.cbuf
#    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
#)

#add_custom_command(
#    OUTPUT registry.h
#    COMMAND ${cbuf_BINARY_DIR}/cbuf ${CMAKE_CURRENT_SOURCE_DIR}/cbuf/registry.cbuf > registry.h
#    DEPENDS cbuf ${CMAKE_CURRENT_SOURCE_DIR}/cbuf/registry.cbuf
#    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
#)

add_library( nodelib STATIC ${NODE_SRCS} cbuf/image.h cbuf/registry.h )
target_include_directories( nodelib 
    PUBLIC 
        ${CMAKE_CURRENT_SOURCE_DIR}/src 
        ${CMAKE_CURRENT_SOURCE_DIR}/include 
    )
target_include_directories( nodelib PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/cbuf )
target_link_libraries( nodelib PRIVATE uloglib cbuf_lib)
set_target_properties( nodelib PROPERTIES CXX_STANDARD 17 )
add_subdirectory(apps)
add_subdirectory(test)
