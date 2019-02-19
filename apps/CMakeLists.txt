cmake_minimum_required( VERSION 3.2.0 )

include(FetchContent)
FetchContent_Declare(
    vrm
    GIT_REPOSITORY "https://git.martian.ag/verdant/vrm.git"
    )
FetchContent_GetProperties(vrm)
if(NOT vrm_POPULATED)
    execute_process(COMMAND ${CMAKE_COMMAND} -E echo_append "-- Downloading vrm... ")
    FetchContent_Populate(vrm) # does the actual download (I think)
    message(" into ${vrm_SOURCE_DIR}... done. ")
    add_subdirectory( ${vrm_SOURCE_DIR} ${vrm_BINARY_DIR} )
endif()

add_custom_command(
    OUTPUT registry.h
    COMMAND ${vrm_BINARY_DIR}/vrm ${CMAKE_SOURCE_DIR}/vrm/registry.vrm > registry.h
    DEPENDS vrm ${CMAKE_SOURCE_DIR}/vrm/registry.vrm
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)


add_executable( nodecore nodesrv.cpp registry.h)
target_include_directories( nodecore PRIVATE ${CMAKE_CURRENT_BINARY_DIR} )
target_link_libraries( nodecore PRIVATE nodelib vrm_lib pthread rt)

add_executable( node nodecli.cpp registry.h)
target_include_directories( node PRIVATE ${CMAKE_CURRENT_BINARY_DIR} )
target_link_libraries( node PRIVATE nodelib vrm_lib pthread rt)
