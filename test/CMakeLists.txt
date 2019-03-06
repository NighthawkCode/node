cmake_minimum_required( VERSION 3.2.0 )

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

add_custom_command(
    OUTPUT image.h
    COMMAND ${cbuf_BINARY_DIR}/cbuf ${CMAKE_CURRENT_SOURCE_DIR}/../cbuf/image.cbuf > image.h
    DEPENDS cbuf ${CMAKE_CURRENT_SOURCE_DIR}/../cbuf/image.cbuf
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

add_executable( prod producer.cpp image.h)
target_include_directories( prod PRIVATE ${CMAKE_CURRENT_BINARY_DIR} )
target_link_libraries( prod PRIVATE nodelib cbuf_lib pthread rt)

add_executable( cons consumer.cpp image.h)
target_include_directories( cons PRIVATE ${CMAKE_CURRENT_BINARY_DIR} )
target_link_libraries( cons PRIVATE nodelib cbuf_lib pthread rt)
