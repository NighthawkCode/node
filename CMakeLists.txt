cmake_minimum_required( VERSION 3.2.0 )

project( node )

include(FetchContent)
FetchContent_Declare(
    cbuf
    GIT_REPOSITORY "https://git.martian.ag/verdant/cbuf.git"
    )
FetchContent_GetProperties(cbuf)
if(NOT cbuf_POPULATED)
    execute_process(COMMAND ${CMAKE_COMMAND} -E echo_append "-- Downloading cbuf... ")
    FetchContent_Populate(cbuf) # does the actual download (I think)
    message(" into ${cbuf_SOURCE_DIR}... done. ")
    add_subdirectory( ${cbuf_SOURCE_DIR} ${cbuf_BINARY_DIR} )
endif()

set( NODE_SRCS 
    src/nodelib.cpp src/node_registry.cpp src/process.cpp src/core.cpp
)

add_custom_command(
    OUTPUT image.h
    COMMAND ${cbuf_BINARY_DIR}/cbuf ${CMAKE_CURRENT_SOURCE_DIR}/cbuf/image.cbuf > image.h
    DEPENDS cbuf ${CMAKE_CURRENT_SOURCE_DIR}/cbuf/image.cbuf
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

add_custom_command(
    OUTPUT registry.h
    COMMAND ${cbuf_BINARY_DIR}/cbuf ${CMAKE_CURRENT_SOURCE_DIR}/cbuf/registry.cbuf > registry.h
    DEPENDS cbuf ${CMAKE_CURRENT_SOURCE_DIR}/cbuf/registry.cbuf
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

add_library( nodelib STATIC ${NODE_SRCS} image.h registry.h )
target_include_directories( nodelib 
    PUBLIC 
        ${CMAKE_CURRENT_SOURCE_DIR}/src 
        ${CMAKE_CURRENT_SOURCE_DIR}/include 
    )
target_include_directories( nodelib PRIVATE ${CMAKE_CURRENT_BINARY_DIR} )
target_link_libraries( nodelib PRIVATE cbuf_lib)

add_subdirectory(apps)
add_subdirectory(test)
