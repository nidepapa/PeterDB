add_library(pfm pfm.cc pfm_test.cpp ../rbfm/PageHelper.cpp ../rbfm/RecordHelper.cpp)
add_dependencies(pfm googlelog)
target_link_libraries(pfm glog)