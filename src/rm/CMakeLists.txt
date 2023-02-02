add_library(rm rm.cc RM_ScanIterator.cpp)
add_dependencies(rm rbfm ix googlelog)
target_link_libraries(rm rbfm ix glog)