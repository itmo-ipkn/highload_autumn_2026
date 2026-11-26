cmake_minimum_required(VERSION 3.20)
project(items_service LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(httplib CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(prometheus-cpp CONFIG REQUIRED)

add_executable(app
    src/main.cpp
    src/store.cpp
    src/metrics.cpp
    src/exposition.cpp
    src/failure.cpp
)

target_link_libraries(app PRIVATE 
    httplib::httplib 
    nlohmann_json::nlohmann_json
    prometheus-cpp::core
    prometheus-cpp::pull
)
