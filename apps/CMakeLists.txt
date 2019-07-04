cmake_minimum_required( VERSION 3.2.0 )

add_custom_command(
    OUTPUT registry.h
    COMMAND ${cbuf_BINARY_DIR}/cbuf ${CMAKE_CURRENT_SOURCE_DIR}/../cbuf/registry.cbuf > registry.h
    DEPENDS cbuf ${CMAKE_CURRENT_SOURCE_DIR}/../cbuf/registry.cbuf
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
add_library( nodecore SHARED registry.h)
target_include_directories( nodecore PRIVATE ${CMAKE_CURRENT_BINARY_DIR} )
target_link_libraries( nodecore PRIVATE nodelib cbuf_lib pthread rt)

add_executable( nodesvr nodesrv.cpp registry.h)
target_include_directories( nodesvr PRIVATE ${CMAKE_CURRENT_BINARY_DIR} )
target_link_libraries( nodesvr PRIVATE nodecore nodelib cbuf_lib pthread rt)

add_executable( node nodecli.cpp registry.h)
target_include_directories( node PRIVATE ${CMAKE_CURRENT_BINARY_DIR} )
target_link_libraries( node PRIVATE nodelib cbuf_lib pthread rt)
