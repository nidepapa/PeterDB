add_library(pfm pfm.cc pfm_test.cpp ../rbfm/PageHandle.cpp ../rbfm/RecordHandle.cpp)
add_dependencies(pfm googlelog)
target_link_libraries(pfm glog)