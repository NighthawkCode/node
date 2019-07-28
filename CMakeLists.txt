cmake_minimum_required( VERSION 3.2.0 )

project( node )

list( APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" )

include(FetchDependency)

fetch_dependency( vlog "https://git.martian.ag/open-source/vlog.git" "master" )
fetch_dependency( cbuf "https://git.martian.ag/open-source/cbuf.git" "master" )

set( NODE_SRCS
    src/nodecore.cpp
    src/nodelib.cpp 
    src/node_registry.cpp 
    src/process.cpp 
    src/core.cpp
    src/hailer.cpp
    src/responder.cpp )

set( NODE_APP_SRCS
    src/node_app_main.cpp )

include(BuildCbuf)
build_cbuf( CBUF_FILE cbuf/image.cbuf )
build_cbuf( CBUF_FILE cbuf/registry.cbuf )

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

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
add_library( nodelib STATIC ${NODE_SRCS} cbuf/image.h cbuf/registry.h )
target_include_directories( nodelib 
    PUBLIC 
        ${CMAKE_CURRENT_SOURCE_DIR}/src 
        ${CMAKE_CURRENT_SOURCE_DIR}/include 
    )
target_include_directories( nodelib PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/cbuf )
target_link_libraries( nodelib PRIVATE uloglib cbuf_lib)
target_link_libraries( nodelib PRIVATE pthread rt )
set_target_properties( nodelib PROPERTIES CXX_STANDARD 17 )

# Node app library.  This contains main and is a .so so that it can be
# dlopen'd 
add_library( nodeapp ${NODE_APP_SRCS} include/node/node_app.h )
target_link_libraries( nodeapp PUBLIC nodelib uloglib cbuf_lib rt)
target_include_directories( nodeapp 
    PUBLIC 
        ${CMAKE_CURRENT_SOURCE_DIR}/src 
        ${CMAKE_CURRENT_SOURCE_DIR}/include 
    )

add_subdirectory(apps)
add_subdirectory(test)
