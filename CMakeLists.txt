cmake_minimum_required(VERSION 3.20)
project(kafka_events LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(RdKafka CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(stduuid CONFIG REQUIRED)
find_package(libpqxx CONFIG REQUIRED)

add_executable(producer
    src/producer_main.cpp
    src/event.cpp
    src/config.cpp
    src/producer.cpp
)

target_link_libraries(producer PRIVATE
    RdKafka::rdkafka++
    nlohmann_json::nlohmann_json
    stduuid
)

add_executable(consumer
    src/consumer_main.cpp
    src/consumer.cpp
    src/event.cpp
    src/config.cpp
    src/sink.cpp
    src/dlt.cpp
)

target_link_libraries(consumer PRIVATE
    RdKafka::rdkafka++
    nlohmann_json::nlohmann_json
    libpqxx::pqxx
    stduuid
)

add_executable(verify
    src/verify_main.cpp
    src/event.cpp
    src/config.cpp
)

target_link_libraries(verify PRIVATE
    RdKafka::rdkafka++
    nlohmann_json::nlohmann_json
    libpqxx::pqxx
    stduuid
)
