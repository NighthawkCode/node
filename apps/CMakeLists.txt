cmake_minimum_required( VERSION 3.2.0 )

macro( fetch_dependency target url tag  )
  include(FetchContent)
  if( NOT TARGET ${target})
    FetchContent_Declare( ${target} GIT_REPOSITORY ${url} )
    FetchContent_GetProperties( ${target} )
    if( NOT ${target}_POPULATED )
      execute_process(COMMAND ${CMAKE_COMMAND} -E echo_append "-- Downloading ${target} ")
      FetchContent_Populate( ${target} )
      message( "into ${${target}_SOURCE_DIR}... done." )
      add_subdirectory( ${${target}_SOURCE_DIR}  ${${target}_BINARY_DIR} )
    endif()
  endif()
endmacro()

fetch_dependency( cbuf "https://git-shared.martian.ag/shared/cbuf.git" "master" )

add_custom_command(
    OUTPUT registry.h
    COMMAND ${cbuf_BINARY_DIR}/cbuf ${CMAKE_CURRENT_SOURCE_DIR}/../cbuf/registry.cbuf > registry.h
    DEPENDS cbuf ${CMAKE_CURRENT_SOURCE_DIR}/../cbuf/registry.cbuf
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)


add_executable( nodecore nodesrv.cpp registry.h)
target_include_directories( nodecore PRIVATE ${CMAKE_CURRENT_BINARY_DIR} )
target_link_libraries( nodecore PRIVATE nodelib cbuf_lib pthread rt)

add_executable( node nodecli.cpp registry.h)
target_include_directories( node PRIVATE ${CMAKE_CURRENT_BINARY_DIR} )
target_link_libraries( node PRIVATE nodelib cbuf_lib pthread rt)
