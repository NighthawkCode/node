cmake_minimum_required( VERSION 3.2.0 )

add_custom_command(
    OUTPUT image.h
    COMMAND ${cbuf_BINARY_DIR}/cbuf ${CMAKE_CURRENT_SOURCE_DIR}/../cbuf/image.cbuf > image.h
    DEPENDS cbuf ${CMAKE_CURRENT_SOURCE_DIR}/../cbuf/image.cbuf
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

add_executable( prod producer.cpp image.h)
target_include_directories( prod PRIVATE ${CMAKE_CURRENT_BINARY_DIR} )
target_link_libraries( prod PRIVATE nodelib cbuf_lib pthread rt)

# NodeApp version of the same producer test node
add_executable( prod_app producer_app.cpp image.h)
target_include_directories( prod_app PRIVATE ${CMAKE_CURRENT_BINARY_DIR} )
target_link_libraries( prod_app PRIVATE nodeapp )

add_executable( cons consumer.cpp image.h)
target_include_directories( cons PRIVATE ${CMAKE_CURRENT_BINARY_DIR} )
target_link_libraries( cons PRIVATE nodelib cbuf_lib pthread rt)

# NodeApp version of the same consumer test node
add_executable( cons_app consumer_app.cpp image.h)
target_include_directories( cons_app PRIVATE ${CMAKE_CURRENT_BINARY_DIR} )
target_link_libraries( cons_app PRIVATE nodeapp )



