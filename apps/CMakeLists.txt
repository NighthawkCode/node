cmake_minimum_required(VERSION 3.13.0)

fetch_dependency(toolbox "https://git.martian.ag/open-source/toolbox.git" "master")

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
add_library(nodecore INTERFACE)
target_link_libraries(nodecore INTERFACE node_messages_internal nodelib cbuf_lib pthread rt)

add_executable(nodemaster nodemaster.cpp)
target_link_libraries(nodemaster PRIVATE node_messages_internal nodecore nodelib cbuf_lib pthread rt)

add_executable(node nodecli.cpp)
target_link_libraries(node PRIVATE node_messages_internal nodelib cbuf_lib pthread rt toolbox vlog cbuf_parse )

install(TARGETS node nodemaster DESTINATION bin)
