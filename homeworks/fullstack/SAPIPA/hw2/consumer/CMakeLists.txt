cmake_minimum_required(VERSION 3.20)
project(consumer LANGUAGES CXX)

option(USERVER_FEATURE_TESTSUITE "Enable userver testsuite" OFF)
find_package(userver 3.2 REQUIRED COMPONENTS kafka)
userver_setup_environment()
find_package(SQLite3 REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(RdKafka REQUIRED IMPORTED_TARGET rdkafka>=2.3)

add_executable(consumer main.cpp kafka_consumer.cpp sink.cpp)
target_link_libraries(consumer PRIVATE userver::kafka PkgConfig::RdKafka SQLite::SQLite3)
