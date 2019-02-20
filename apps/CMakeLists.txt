cmake_minimum_required( VERSION 3.2.0 )

include(FetchContent)
FetchContent_Declare(
    cbuf
    GIT_REPOSITORY "https://git.martian.ag/verdant/vrm.git"
    )
FetchContent_GetProperties(cbuf)
if(NOT cbuf_POPULATED)
    execute_process(COMMAND ${CMAKE_COMMAND} -E echo_append "-- Downloading cbuf... ")
    FetchContent_Populate(cbuf) # does the actual download (I think)
    message(" into ${cbuf_SOURCE_DIR}... done. ")
    add_subdirectory( ${cbuf_SOURCE_DIR} ${cbuf_BINARY_DIR} )
endif()

add_custom_command(
    OUTPUT registry.h
    COMMAND ${cbuf_BINARY_DIR}/cbuf ${CMAKE_SOURCE_DIR}/cbuf/registry.cbuf > registry.h
    DEPENDS cbuf ${CMAKE_SOURCE_DIR}/cbuf/registry.cbuf
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)


add_executable( nodecore nodesrv.cpp registry.h)
target_include_directories( nodecore PRIVATE ${CMAKE_CURRENT_BINARY_DIR} )
target_link_libraries( nodecore PRIVATE nodelib cbuf_lib pthread rt)

add_executable( node nodecli.cpp registry.h)
target_include_directories( node PRIVATE ${CMAKE_CURRENT_BINARY_DIR} )
target_link_libraries( node PRIVATE nodelib cbuf_lib pthread rt)
