cmake_minimum_required(VERSION 3.20)
project(app LANGUAGES CXX)

find_package(userver REQUIRED COMPONENTS core)
userver_setup_environment()

add_executable(app
    src/handlers/main.cpp
    src/handlers/health.cpp
    src/handlers/create_item.cpp
    src/handlers/get_item.cpp
    src/handlers/update_item.cpp
    src/handlers/handlers.cpp
    src/storage/item_storage.cpp
    src/monitoring/latency_metrics.cpp
)
target_include_directories(app PRIVATE src)
target_link_libraries(app PRIVATE userver::core)

if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/conftest.py")
    userver_testsuite_add_simple()
endif()
