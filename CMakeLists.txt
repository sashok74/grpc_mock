cmake_minimum_required(VERSION 3.20)
project(grpc_server VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(Crow CONFIG REQUIRED)

set(PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)
set(PROTO_FILE ${PROTO_DIR}/table.proto)
set(GENERATED_PROTO_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${GENERATED_PROTO_DIR})

add_custom_command(
    OUTPUT ${GENERATED_PROTO_DIR}/table.pb.cc ${GENERATED_PROTO_DIR}/table.pb.h
    COMMAND protobuf::protoc
    ARGS --proto_path=${PROTO_DIR}
         --cpp_out=${GENERATED_PROTO_DIR}
         ${PROTO_FILE}
    DEPENDS ${PROTO_FILE} protobuf::protoc
    COMMENT "Generating protobuf sources"
    VERBATIM
)

add_custom_command(
    OUTPUT ${GENERATED_PROTO_DIR}/table.grpc.pb.cc ${GENERATED_PROTO_DIR}/table.grpc.pb.h
    COMMAND protobuf::protoc
    ARGS --proto_path=${PROTO_DIR}
         --grpc_out=${GENERATED_PROTO_DIR}
         --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
         ${PROTO_FILE}
    DEPENDS ${PROTO_FILE} protobuf::protoc gRPC::grpc_cpp_plugin
    COMMENT "Generating gRPC sources"
    VERBATIM
)

add_library(table_proto STATIC
    ${GENERATED_PROTO_DIR}/table.pb.cc
    ${GENERATED_PROTO_DIR}/table.grpc.pb.cc
)
target_include_directories(table_proto PUBLIC ${GENERATED_PROTO_DIR})
target_link_libraries(table_proto PUBLIC protobuf::libprotobuf gRPC::grpc++)

add_executable(grpc_server src/main.cpp)
target_include_directories(grpc_server PRIVATE ${GENERATED_PROTO_DIR})
target_link_libraries(grpc_server PRIVATE Crow::Crow table_proto gRPC::grpc++)
target_compile_features(grpc_server PRIVATE cxx_std_20)
