cmake_minimum_required(VERSION 3.20)
project(consumer LANGUAGES CXX)

option(USERVER_FEATURE_TESTSUITE "Enable userver testsuite" OFF)
find_package(userver 3.2 REQUIRED COMPONENTS core)
userver_setup_environment()

add_executable(consumer main.cpp)
target_link_libraries(consumer PRIVATE userver::core)
