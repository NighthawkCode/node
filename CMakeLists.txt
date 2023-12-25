cmake_minimum_required(VERSION 3.12.0)

project(node)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(FetchDependency)
include(AddTest)

include("BuildSettings.cmake")

fetch_dependency(vlog "https://github.com/NighhawkCode/vlog.git" "master")
fetch_dependency(cbuf "https://github.com/NighhawkCode/cbuf.git" "master")

set(NODE_SRCS src/nodemaster_main.cpp src/nodelib.cpp src/node_registry.cpp
              src/process.cpp src/core.cpp src/node_utils.cpp src/crc.cpp src/nodemaster_utils.cpp)

set(NODE_APP_SRCS src/node_app_main.cpp src/node_app.cpp)

include(BuildCbuf)
build_cbuf(NAME node_cbuf MSG_FILES cbuf/image.cbuf cbuf/registry.cbuf cbuf/rpcmsg.cbuf)

add_library(node_messages_internal INTERFACE)
target_link_libraries(node_messages_internal INTERFACE node_cbuf)
target_include_directories(node_messages_internal INTERFACE ${CMAKE_CURRENT_BINARY_DIR}/cbuf)

add_library(nodelib STATIC ${NODE_SRCS} cbuf/image.h cbuf/registry.h)
target_include_directories(nodelib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_include_directories(nodelib PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/cbuf)
target_include_directories(nodelib PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(nodelib PRIVATE cbuf_parse explain pthread rt)
target_link_libraries(nodelib PUBLIC vlog cbuf_lib)
set_target_properties(nodelib PROPERTIES CXX_STANDARD 20)

# Node app library.  This contains main and is a .so so that it can be dlopen'd
add_library(nodeapp STATIC ${NODE_APP_SRCS} include/node/node_app.h)
target_link_libraries(nodeapp PUBLIC nodelib vlog rt)
target_include_directories(nodeapp PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src ${CMAKE_CURRENT_SOURCE_DIR}/include)
set_property(TARGET nodeapp PROPERTY POSITION_INDEPENDENT_CODE ON)

find_package(CUDAToolkit)
if(CMAKE_CUDA_COMPILER)
  target_compile_definitions(nodeapp PUBLIC HAVE_CUDA)
  target_include_directories(nodeapp SYSTEM PUBLIC ${CMAKE_CUDA_TOOLKIT_INCLUDE_DIRECTORIES})
  target_link_libraries(nodeapp PRIVATE CUDA::nvToolsExt)
endif()

add_subdirectory(apps)
add_subdirectory(test)

if(${CMAKE_SYSTEM_PROCESSOR} MATCHES "x86_64")
  add_subdirectory(pynode)
endif()
