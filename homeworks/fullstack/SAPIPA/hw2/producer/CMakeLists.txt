cmake_minimum_required(VERSION 3.20)
project(producer LANGUAGES CXX)

option(USERVER_FEATURE_TESTSUITE "Enable userver testsuite" OFF)
find_package(userver 3.2 REQUIRED COMPONENTS kafka)
userver_setup_environment()

add_executable(producer main.cpp)
target_link_libraries(producer PRIVATE userver::kafka)
