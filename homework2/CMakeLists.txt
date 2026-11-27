cmake_minimum_required(VERSION 3.20)
project(kafka_homework LANGUAGES CXX)

find_package(userver REQUIRED COMPONENTS kafka postgresql)
userver_setup_environment()

add_executable(homework-producer src/producer.cpp)
target_link_libraries(homework-producer PRIVATE userver::kafka)

add_executable(homework-consumer src/consumer.cpp)
target_link_libraries(homework-consumer PRIVATE userver::kafka userver::postgresql)
