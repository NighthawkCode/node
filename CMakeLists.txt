cmake_minimum_required( VERSION 3.2.0 )

project( node )

macro( fetch_dependency target url tag  )
  include(FetchContent)
  if (NOT TARGET ${target})
    find_file(${target}_project ${target}.project PATHS ../${target} ../../${target} ../../../${target} )
    if (NOT ${target}_project)
        FetchContent_Declare( ${target} GIT_REPOSITORY ${url} )
        FetchContent_GetProperties( ${target} )
        if( NOT ${target}_POPULATED )
        execute_process(COMMAND ${CMAKE_COMMAND} -E echo_append "-- (node) Downloading ${target} ")
        FetchContent_Populate( ${target} )
        message( "into ${${target}_SOURCE_DIR}... done." )
        add_subdirectory( ${${target}_SOURCE_DIR}  ${${target}_BINARY_DIR} )
        endif()
    else()
        get_filename_component( dir ${${target}_project} DIRECTORY )  
        set(${target}_SOURCE_DIR ${dir})
        message( "-- Using ${target} on the local disk at: ${${target}_SOURCE_DIR}" )
        add_subdirectory( ${${target}_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR}/${target} )
    endif()
  endif()
endmacro()

fetch_dependency( cbuf "https://git-shared.martian.ag/shared/cbuf.git" "master" )

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
set_target_properties( nodelib PROPERTIES CXX_STANDARD 17 )
add_subdirectory(apps)
add_subdirectory(test)
