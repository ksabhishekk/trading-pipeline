cmake_minimum_required(VERSION 3.15)
project(low_latency_trading_pipeline CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(MSVC)
    add_compile_options(/O2 /W4)
else()
    add_compile_options(-O2 -Wall -Wextra)
endif()

include_directories(include)

add_executable(market_data_generator src/market_data_generator.cpp)
add_executable(execution_engine src/execution_engine.cpp)
add_executable(feed_publisher src/feed_publisher.cpp)
add_executable(feed_handler src/feed_handler.cpp)

if(WIN32)
    target_link_libraries(market_data_generator ws2_32)
    target_link_libraries(execution_engine ws2_32)
    target_link_libraries(feed_publisher ws2_32)
    target_link_libraries(feed_handler ws2_32)
endif()

if(EXISTS "${CMAKE_SOURCE_DIR}/tests/test_spsc_queue.cpp")
    add_executable(test_spsc_queue tests/test_spsc_queue.cpp)
endif()

if(EXISTS "${CMAKE_SOURCE_DIR}/tests/test_feed_handler.cpp")
    add_executable(test_feed_handler tests/test_feed_handler.cpp)
endif()
