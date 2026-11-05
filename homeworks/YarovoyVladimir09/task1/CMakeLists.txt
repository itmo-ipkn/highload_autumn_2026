cmake_minimum_required(VERSION 3.24)

project(my_service LANGUAGES CXX)

find_package(userver REQUIRED COMPONENTS core)

include(FetchContent)

set(ENABLE_PULL OFF CACHE BOOL "" FORCE)
set(ENABLE_PUSH OFF CACHE BOOL "" FORCE)
set(ENABLE_COMPRESSION OFF CACHE BOOL "" FORCE)
set(ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(OVERRIDE_CXX_STANDARD_FLAGS OFF CACHE BOOL "" FORCE)
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
    prometheus_cpp
    URL https://codeload.github.com/jupp0r/prometheus-cpp/tar.gz/refs/tags/v1.3.0
    URL_HASH SHA256=ac6e958405a29fbbea9db70b00fa3c420e16ad32e1baf941ab233ba031dd72ee
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)

FetchContent_MakeAvailable(prometheus_cpp)

add_executable(my_service
    src/main.cpp
    src/components/business_handler.cpp
    src/components/item_storage.cpp
    src/components/metric.cpp
    src/components/monitored_handler.cpp
    src/views/create_item.cpp
    src/views/handlers.cpp
    src/views/health.cpp
    src/views/item.cpp
    src/views/metrics.cpp
)

target_compile_features(my_service PRIVATE cxx_std_20)

target_link_libraries(
    my_service
    PRIVATE
        userver::core
        prometheus-cpp::core
)
