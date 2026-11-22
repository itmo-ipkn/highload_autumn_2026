cmake_minimum_required(VERSION 3.20)
project(kafka_events LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(RdKafka CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(stduuid CONFIG REQUIRED)

add_executable(producer
    src/main.cpp
    src/event.cpp
    src/config.cpp
    src/producer.cpp
)

target_link_libraries(producer PRIVATE
    RdKafka::rdkafka++
    nlohmann_json::nlohmann_json
    stduuid
)
