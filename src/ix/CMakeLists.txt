add_library(ix ix.cc IXFileHandle.cpp IXScanIterator.cpp IXNode.cpp)
add_dependencies(ix pfm googlelog)
target_link_libraries(ix pfm glog)