# Main CMakeFiles.txt 
cmake_minimum_required(VERSION 3.15)
cmake_policy(SET CMP0002 NEW) 
project     (odb4py  LANGUAGES C CXX)


# Force position-independent code for shared libs
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Scikit-build provides Python variables
find_package(Python3 REQUIRED COMPONENTS Interpreter  Development.Module )

# Include ecmwf macros 
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
set ( MACROS_DIR     ${CMAKE_SOURCE_DIR}/cmake   )                                  
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")                  
list(APPEND ${CMAKE_MODULE_PATH}    ${MACROS_DIR}      ) 

# Some ecbuild macros 
include(ecbuild_system)
include(odb_add_schema)
include(odb_link_schemas)
include(odb_set_environment)


# Compilation 
add_subdirectory(odb )              # genarate odb binaries + libodb*.so  
add_subdirectory(bindings)          # generate odb4py.so  

# __init__
install( FILES  ${CMAKE_SOURCE_DIR}/__init__.py     DESTINATION   . )

# ODB  (Relatively to scikit.build directory)  
install(DIRECTORY bin     DESTINATION bin     OPTIONAL)
install(DIRECTORY include DESTINATION include OPTIONAL)
install(DIRECTORY lib     DESTINATION lib     OPTIONAL)
install(DIRECTORY share   DESTINATION share   OPTIONAL)
install(DIRECTORY module  DESTINATION module  OPTIONAL)
