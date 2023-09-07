cmake_minimum_required(VERSION 3.13.0)

add_executable(prod producer.cpp)
target_link_libraries(prod PRIVATE node_messages_internal nodelib cbuf_lib pthread rt)

# NodeApp version of the same producer test node
add_executable(prod_app producer_app.cpp)
target_link_libraries(prod_app PRIVATE nodeapp node_messages_internal)

add_executable(cons consumer.cpp)
target_link_libraries(cons PRIVATE node_messages_internal nodelib cbuf_lib pthread rt)

# Consumer using the notify api
add_executable(consn consumer_notify.cpp)
target_link_libraries(consn PRIVATE node_messages_internal nodelib cbuf_lib pthread rt)

# NodeApp version of the same consumer test node
add_executable(cons_app consumer_app.cpp)
target_link_libraries(cons_app PRIVATE nodeapp node_messages_internal)

add_executable(provider provider.cpp)
target_link_libraries(provider PRIVATE node_messages_internal nodelib cbuf_lib pthread rt)

add_executable(requester requester.cpp)
target_link_libraries(requester PRIVATE node_messages_internal nodelib cbuf_lib pthread rt)

add_executable(prod_large producer_large.cpp)
target_link_libraries(prod_large PRIVATE node_messages_internal nodelib cbuf_lib pthread rt)

add_executable(cons_large consumer_large.cpp)
target_link_libraries(cons_large PRIVATE nodeapp node_messages_internal nodelib cbuf_lib pthread rt)

# Test for the key-value store in registry
add_executable(key_value_test key_value_test.cpp)
target_link_libraries(key_value_test PRIVATE gtest vlog nodeapp node_messages_internal nodelib cbuf_lib rt)
